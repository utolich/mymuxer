use crate::pes::parser::PesReassemblerMap;
use crate::psi::pat::PatInfo;
use crate::psi::pmt::PmtInfo;
use crate::psi::sdt::SdtInfo;
use crate::{packet, psi};
use bytes::{Buf, Bytes, BytesMut};
use chrono::{DateTime, Utc};
use serde::Serialize;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast;

const MAX_BUFFER_SIZE: usize = 1024 * 1024;
const MUX_BUFFER_SIZE: usize = 3 * packet::TS_PACKET_SIZE;

const PROBE_BUFFER_SIZE: usize = 15 * 1024 * 1024;

const TIMEOUT_SEC: u64 = 5;

#[derive(Debug, Clone, Serialize)]
struct ProbeProgram {
    pat: PatInfo,
    pmt: Option<PmtInfo>,
    sdt: Option<SdtInfo>,
}

impl ProbeProgram {
    fn new(pat: PatInfo) -> ProbeProgram {
        ProbeProgram {
            pat,
            pmt: None,
            sdt: None,
        }
    }

    pub fn set_pmt(&mut self, pmt: PmtInfo) {
        self.pmt = Some(pmt);
    }

    pub fn set_sdt(&mut self, sdt: SdtInfo) {
        self.sdt = Some(sdt);
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProbeInfo {
    probe_programs: Vec<ProbeProgram>,
}

impl ProbeInfo {
    fn new() -> ProbeInfo {
        ProbeInfo {
            probe_programs: Vec::new(),
        }
    }
}

struct Probe {
    in_buff: BytesMut,
    is_sync: bool,
    is_pat: bool,
    is_sdt: bool,
    is_pmt: bool,
    is_pes: bool,
    pid_to_stream: HashMap<u16, u8>,
    psi_reasm: psi::SectionReassemblerMap,
    probe_info: Option<ProbeInfo>,
}

impl Probe {
    fn new() -> Probe {
        Probe {
            in_buff: BytesMut::with_capacity(MAX_BUFFER_SIZE),
            is_sync: false,
            is_pat: false,
            is_sdt: false,
            is_pmt: false,
            is_pes: false,
            pid_to_stream: HashMap::new(),
            psi_reasm: psi::SectionReassemblerMap::new(),
            probe_info: None,
        }
    }

    fn stream_allowed(&self, stream_type: u8) -> bool {
        matches!(&stream_type, 2 | 3 | 4 | 15 | 27)
    }

    fn write(&mut self, chunk: Bytes) -> bool {
        self.in_buff.extend_from_slice(&chunk);
        if !self.is_sync {
            if self.in_buff.len() < MUX_BUFFER_SIZE {
                return false;
            }

            while self.in_buff.len() >= packet::TS_PACKET_SIZE * 3 {
                if self.in_buff[0] == packet::TS_SYNC_BYTE
                    && self.in_buff[packet::TS_PACKET_SIZE] == packet::TS_SYNC_BYTE
                    && self.in_buff[packet::TS_PACKET_SIZE * 2] == packet::TS_SYNC_BYTE
                {
                    self.is_sync = true;
                    break;
                } else {
                    if let Some(pos) = self.in_buff[1..]
                        .iter()
                        .position(|&b| b == packet::TS_SYNC_BYTE)
                    {
                        self.in_buff.advance(pos + 1);
                    } else {
                        self.in_buff.clear();
                        return false;
                    };
                }
            }
        }

        if self.in_buff.len() < PROBE_BUFFER_SIZE {
            return false;
        }

        true
    }

    fn process(&mut self) -> Option<ProbeInfo> {
        if !self.is_sync {
            return None;
        }

        let mut pointer = 0;
        let available_size = (self.in_buff.len() / packet::TS_PACKET_SIZE) * packet::TS_PACKET_SIZE;

        let mut reasm = PesReassemblerMap::new();
        while pointer < available_size {
            if self.in_buff[pointer] == packet::TS_SYNC_BYTE {
                let pkt_range = pointer..pointer + packet::TS_PACKET_SIZE;
                pointer += packet::TS_PACKET_SIZE;
                let pkt_data = &self.in_buff[pkt_range];
                let pid = packet::pkt_pid(&pkt_data);
                match pid {
                    0x1FFF => continue,
                    0x0000 if !self.is_pat => {
                        if let Some(section) = self.psi_reasm.push(&pkt_data) {
                            let pats = psi::pat::analyze_pat(&section);
                            let mut probe_info = ProbeInfo::new();
                            for pat in pats {
                                let probe_program = ProbeProgram::new(pat);
                                probe_info.probe_programs.push(probe_program);
                            }
                            self.probe_info = Some(probe_info);
                            self.is_pat = true;
                        }
                    }
                    0x0011 if self.is_pat && !self.is_sdt => {
                        if let Some(section) = self.psi_reasm.push(&pkt_data) {
                            let sdts = psi::sdt::analyze_sdt(&section);
                            if !sdts.is_empty() {
                                for sdt in sdts {
                                    let probe_program = self
                                        .probe_info
                                        .as_mut()
                                        .unwrap()
                                        .probe_programs
                                        .iter_mut()
                                        .find(|probe| probe.pat.prog_num == sdt.service_id);
                                    if let Some(probe_prog) = probe_program {
                                        probe_prog.set_sdt(sdt);
                                    }
                                }
                            }
                            self.is_sdt = true;
                        }
                    }
                    id if self.is_pat && !self.is_pmt && {
                        let probe_program = self
                            .probe_info
                            .as_mut()
                            .unwrap()
                            .probe_programs
                            .iter()
                            .find(|probe| probe.pmt.is_none());
                        if let Some(probe_prog) = probe_program {
                            id == probe_prog.pat.pmt_pid
                        } else {
                            self.is_pmt = true;
                            false
                        }
                    } =>
                    {
                        if let Some(section) = self.psi_reasm.push(&pkt_data) {
                            let pmt = psi::pmt::analyze_pmt(&section);
                            for stream in pmt.streams.iter() {
                                if self.stream_allowed(stream.stream_type) {
                                    self.pid_to_stream.insert(stream.pid, stream.stream_type);
                                }
                            }
                            let probe_program = self
                                .probe_info
                                .as_mut()
                                .unwrap()
                                .probe_programs
                                .iter_mut()
                                .find(|probe| probe.pat.pmt_pid == id);
                            if let Some(probe_prog) = probe_program {
                                probe_prog.set_pmt(pmt);
                            }
                        }
                    }
                    id if self.is_pmt && !self.is_pes => {
                        if let Some(pes) = reasm.push(&pkt_data, &self.pid_to_stream) {
                            if let Some(es) = pes.es {
                                let stream = self.probe_info.as_mut().and_then(|info| {
                                    info.probe_programs
                                        .iter_mut()
                                        .filter_map(|program| program.pmt.as_mut())
                                        .flat_map(|pmt| pmt.streams.iter_mut())
                                        .find(|stream| stream.pid == id)
                                });
                                if let Some(stream) = stream {
                                    stream.description = es.description();
                                    self.pid_to_stream.remove(&id);
                                }
                            }
                        }

                        if self.pid_to_stream.is_empty() {
                            self.is_pes = true;
                        }
                    }
                    _ => {}
                }
            } else {
                self.is_sync = false;
                self.in_buff.advance(pointer);
                return None;
            }
        }

        self.probe_info.clone()
    }
}

#[derive(Debug, Serialize)]
pub struct ProbeResult {
    id: u32,
    timestamp: DateTime<Utc>,
    status: bool,
    error_msg: String,
    programs: Vec<ProbeProgram>,
}

pub async fn probe(stream_id: u32, mut rx: broadcast::Receiver<Bytes>) -> ProbeResult {
    let timeout = tokio::time::sleep(Duration::from_secs(TIMEOUT_SEC));
    tokio::pin!(timeout);

    let mut probe_result = ProbeResult {
        id: stream_id,
        timestamp: Utc::now(),
        status: true,
        error_msg: String::new(),
        programs: Vec::new(),
    };

    let mut probe = Probe::new();

    loop {
        tokio::select! {
            res = rx.recv() => {
                match res {
                    Ok(chunk) => {
                        if probe.write(chunk) {
                            break;
                        }
                    },
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        continue;
                    },
                    Err(e) => {
                        probe_result.status = false;
                        probe_result.error_msg = format!("Received failed:{:?}", e);
                        break;
                    }
                }
            },
            _ = &mut timeout => {
                break;
            }
        }
    }

    let probe_info = probe.process();
    match probe_info {
        Some(probe_info) => probe_result.programs = probe_info.probe_programs,
        None => {
            probe_result.status = false;
            probe_result.error_msg = "Probe process failed".to_string();
        }
    };

    probe_result
}
