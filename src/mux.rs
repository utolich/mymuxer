use std::cmp::max;
use crate::config::{Output, Program};
use crate::status::OutputStats;
use crate::workers::Helper;
use crate::{biss::Biss1Descrambler, config, packet, psi};
use anyhow::{Result, anyhow};
use bytes::{Buf, Bytes, BytesMut};
use chrono::Utc;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use tokio::sync::Notify;
use crate::psi::pat::PatInfo;
use crate::psi::pmt::PmtInfo;

const IN_BUFFER_SIZE: usize = 64 * 1024;
const FRAME_BUFFER_SIZE: usize = 8192;
const MUX_BUFFER_SIZE: usize = 3 * packet::TS_PACKET_SIZE;
const MAX_BUFFER_SIZE: usize = 50 * 1024 * 1024;
const MAX_BUFFER_COUNT_V: usize = 5_000;
const MAX_BUFFER_COUNT_A: usize = 500;

const BUFFER_DURATION_MS: usize = 5_000;
const MAX_BUFFER_DURATION_COEF: usize = 2;

const PERIOD_PAT_MS: u64 = 100;
const PERIOD_PMT_MS: u64 = 100;
const PERIOD_SDT_MS: u64 = 1_000;
const DEFAULT_SERVICE_TYPE: u8 = 0x01;

const DISCONTINUITY_THRESHOLD_MS: u64 = 1_000;

const DTS_PCR_DIFF_MS: u64 = 300;
const MIN_JITTER_MS: u64 = 30;
const MAX_JITTER_MS: u64 = 3_000;

const DTS_MASK: u64 = (1u64 << 33) - 1;
const DTS_HALF: u64 = 1u64 << 32;
// PCR is a 33-bit base plus an extension in the range 0..300.
const PCR_CYCLE: u64 = (1u64 << 33) * 300;
const PCR_HALF: u64 = PCR_CYCLE / 2;

const PCR_HZ: u128 = 27_000_000;

fn dts_after(a: u64, b: u64) -> bool {
    let diff = (a.wrapping_sub(b)) & DTS_MASK;
    diff != 0 && diff < DTS_HALF
}

fn dts_before_or_equal(a: u64, b: u64) -> bool {
    !dts_after(a, b)
}

fn dts_signed_delta(a: u64, b: u64) -> i64 {
    let delta = (a.wrapping_sub(b)) & DTS_MASK;
    if delta >= DTS_HALF {
        delta as i64 - ((DTS_MASK + 1) as i64)
    } else {
        delta as i64
    }
}

fn dts_forward_delta_ms(new_dts: u64, last_dts: u64) -> Option<u64> {
    let diff = (new_dts.wrapping_sub(last_dts)) & DTS_MASK;
    if diff != 0 && diff < DTS_HALF {
        Some(diff / 90)
    } else {
        None
    }
}

fn pcr_after(a: u64, b: u64) -> bool {
    let a = pcr_normalize(a);
    let b = pcr_normalize(b);
    let diff = if a >= b { a - b } else { PCR_CYCLE - (b - a) };
    diff != 0 && diff < PCR_HALF
}

fn pcr_add(a: u64, b: u64) -> u64 {
    pcr_normalize(pcr_normalize(a) + pcr_normalize(b))
}

fn pcr_sub(a: u64, b: u64) -> u64 {
    let a = pcr_normalize(a);
    let b = pcr_normalize(b);
    if a >= b { a - b } else { PCR_CYCLE - (b - a) }
}

fn pcr_signed_delta(a: u64, b: u64) -> i64 {
    let delta = pcr_sub(a, b);
    if delta >= PCR_HALF {
        delta as i64 - PCR_CYCLE as i64
    } else {
        delta as i64
    }
}

fn pcr_normalize(pcr: u64) -> u64 {
    pcr % PCR_CYCLE
}

struct UpdateTimers {
    insert_pat: Option<u64>,
    insert_pmt: Option<u64>,
    insert_sdt: Option<u64>,
}

struct TsPacket {
    data: [u8; packet::TS_PACKET_SIZE],
    pid: u16,
    pts: u64,
    dts: u64,
    pusi: bool,
    discontinuity: bool,
    pcr: Option<u64>,
}
impl TsPacket {
    fn new(
        data: [u8; packet::TS_PACKET_SIZE],
        pid: u16,
        pts: u64,
        dts: u64,
        pusi: bool,
        discontinuity: bool,
    ) -> Self {
        Self {
            data,
            pid,
            pts,
            dts,
            pusi,
            discontinuity,
            pcr: None,
        }
    }

    fn extract_pcr(&mut self) {
        let packet = self.data.as_ref();
        self.pcr = packet::pkt_extract_pcr(packet);
    }

}

pub struct OutBuffer {
    inner: VecDeque<TsPacket>,
    max_size: usize,
    unique_frames_count: usize,
    pcr_count: usize,
}

impl OutBuffer {
    pub fn new(max_size: usize) -> Self {
        Self {
            inner: VecDeque::new(),
            max_size,
            unique_frames_count: 0,
            pcr_count: 0,
        }
    }

    fn clear(&mut self) {
        self.inner = VecDeque::new();
        self.unique_frames_count = 0;
        self.pcr_count = 0;
    }

    fn push_back(&mut self, pkt: TsPacket) {
        if pkt.pusi {
            self.unique_frames_count += 1;
        }

        if pkt.pcr.is_some() {
            self.pcr_count += 1;
        }

        self.inner.push_back(pkt);
    }

    fn pop_front(&mut self) -> Option<TsPacket> {
        let current = self.inner.pop_front()?;
        if current.pusi {
            self.unique_frames_count = self.unique_frames_count.saturating_sub(1);
        }

        if current.pcr.is_some() {
            self.pcr_count = self.pcr_count.saturating_sub(1);
        }

        Some(current)
    }

    fn front(&self) -> Option<&TsPacket> {
        self.inner.front()
    }

    fn back(&self) -> Option<&TsPacket> {
        self.inner.back()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn size(&self) -> usize {
        self.inner.len() * packet::TS_PACKET_SIZE
    }

    pub fn duration(&self) -> u64 {
        match (self.inner.front(), self.inner.back()) {
            (Some(first), Some(last)) => dts_forward_delta_ms(last.dts, first.dts).unwrap_or(0),
            _ => 0,
        }
    }
}

// multistream not supported yet
pub struct PacketHandler {
    pub(crate) output: Output,
    pub(crate) helper: Helper,
    is_ready: bool,
    is_sync: bool,
    is_buffer_process_async: bool,
    process_notify: Arc<Notify>,

    in_buff: BytesMut,
    out_buff_v: OutBuffer,
    out_buff_a: OutBuffer,
    frame_buff: BytesMut,

    check_buffer_size: bool,
    min_buff_size: usize,
    max_buff_size: usize,

    last_pts_dts: HashMap<u16, (u64, u64)>,
    last_cc: HashMap<u16, u8>,
    cc_errors: HashMap<u16, usize>,

    buffer_duration_ms: u64,

    jitter_ms: u64,
    adjust_buf: u64,

    pat_info: Vec<PatInfo>,
    pmt_info: Option<PmtInfo>,

    pat: [u8; packet::TS_PACKET_SIZE],
    pmt: [u8; packet::TS_PACKET_SIZE],
    sdt: [u8; packet::TS_PACKET_SIZE],

    pat_ready: bool,
    pmt_ready: bool,
    sdt_ready: bool,
    psi_reasm: psi::SectionReassemblerMap,

    timers: UpdateTimers,

    count_psi_packets: usize,
    count_null_packets: usize,
    count_payload_packets: usize,
    count_adjust_packets: usize,
    fps: f64,

    pcr_restamper: Option<PcrRestamper>,
    biss: Option<Biss1Descrambler>,

    pub(crate) stats: Arc<OutputStats>,
    dirty: bool,
}

impl PacketHandler {
    pub fn new(output: Output, helper: Helper, stats: Arc<OutputStats>) -> Option<Self> {
        let pcr_restamper = if !output.proxy {
            Some(PcrRestamper::new(output.cbr as u64))
        } else {
            None
        };

        let biss = Self::init_biss(&output.biss_key)
            .inspect_err(|e| {
                helper.clone().log(&format!("BISS init failed: {}", e));
            })
            .ok();

        let mut cc_errors = HashMap::new();
        let mut jitter_ms = 0;
        let mut adjust_buf = 0;
        // ToDo
        output.programs.iter().for_each(|program| {
            program.audio_pids.iter().for_each(|pid| {
                cc_errors.insert(*pid, 0);
            });
            program.video_pids.iter().for_each(|pid| {
                cc_errors.insert(*pid, 0);
            });
            jitter_ms = program.jitter as u64;
            adjust_buf = program.adjust_buf as u64;
        });
        stats.update_cc(&cc_errors);

        jitter_ms = jitter_ms.clamp(MIN_JITTER_MS, MAX_JITTER_MS);

        Some(Self {
            output,
            helper,
            is_ready: false,
            is_sync: false,
            is_buffer_process_async: false,
            process_notify: Arc::new(Notify::new()),

            in_buff: BytesMut::with_capacity(IN_BUFFER_SIZE),
            out_buff_v: OutBuffer::new(MAX_BUFFER_COUNT_V),
            out_buff_a: OutBuffer::new(MAX_BUFFER_COUNT_A),
            frame_buff: BytesMut::with_capacity(FRAME_BUFFER_SIZE),

            check_buffer_size: false,
            min_buff_size: 0,
            max_buff_size: 0,

            last_pts_dts: HashMap::new(),
            last_cc: HashMap::new(),
            cc_errors,

            buffer_duration_ms: BUFFER_DURATION_MS as u64,

            pat_info: Vec::new(),
            pmt_info: None,

            pat: [0u8; packet::TS_PACKET_SIZE],
            pmt: [0u8; packet::TS_PACKET_SIZE],
            sdt: [0u8; packet::TS_PACKET_SIZE],

            pat_ready: false,
            pmt_ready: false,
            sdt_ready: false,
            psi_reasm: psi::SectionReassemblerMap::new(),

            timers: UpdateTimers {
                insert_pat: None,
                insert_pmt: None,
                insert_sdt: None,
            },

            jitter_ms,
            adjust_buf,

            count_psi_packets: 0,
            count_null_packets: 0,
            count_payload_packets: 0,
            count_adjust_packets: 0,
            fps: 0.0,

            pcr_restamper,
            biss,

            stats,
            dirty: false,
        })
    }

    pub fn init_biss(biss_key: &str) -> Result<Biss1Descrambler> {
        if biss_key.len() > 0 {
            match Biss1Descrambler::new_from_hex(&biss_key) {
                Ok(biss) => Ok(biss),
                Err(e) => Err(e),
            }
        } else {
            Err(anyhow!("BISS key is empty"))
        }
    }

    pub fn sync_buffer(buff: &mut BytesMut) -> bool {
        if buff.len() < MUX_BUFFER_SIZE {
            return false;
        }

        let mut is_sync = false;

        while buff.len() >= packet::TS_PACKET_SIZE * 3 {
            if buff[0] == packet::TS_SYNC_BYTE
                && buff[packet::TS_PACKET_SIZE] == packet::TS_SYNC_BYTE
                && buff[packet::TS_PACKET_SIZE * 2] == packet::TS_SYNC_BYTE
            {
                is_sync = true;
                break;
            } else {
                if let Some(pos) = buff[1..].iter().position(|&b| b == packet::TS_SYNC_BYTE) {
                    buff.advance(pos + 1);
                } else {
                    buff.clear();
                    break;
                };
            }
        }

        is_sync
    }

    pub fn get_pkt_size(&self) -> u16 {
        if self.output.pkt_size == 0 {
            packet::TS_PACKET_SIZE as u16
        } else {
            self.output.pkt_size
        }
    }

    pub fn get_bitrate(&self) -> Option<u64> {
        self.pcr_restamper.as_ref().and_then(|r| {
            Some(r.target_bitrate_bps)
        })
    }

    pub fn calculate_fps(&self) -> Option<f64> {
        if self.out_buff_v.unique_frames_count < 2 {
            return None;
        }

        let first = self.out_buff_v.inner.iter().find(|p| p.pusi);
        let last = self.out_buff_v.inner.iter().rev().find(|p| p.pusi);

        match (first, last) {
            (Some(first), Some(last)) => {
                let diff_ticks = (last.dts.wrapping_sub(first.dts)) & DTS_MASK;
                if diff_ticks == 0 {
                    return None;
                }
                let actual_intervals = (self.out_buff_v.unique_frames_count - 1) as f64;
                let average_delta = diff_ticks as f64 / actual_intervals;

                Some(90000.0 / average_delta)
            }
            _ => None,
        }
    }

    pub fn set_check_buffer_size(&mut self) {
        self.check_buffer_size = true;
    }

    pub fn set_buffer_process_async(&mut self) {
        self.is_buffer_process_async = true;
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
        self.buffers_clear();
        self.helper.cmd_mark_dirty();
    }

    fn unmark_dirty(&mut self) {
        self.dirty = false;
        self.helper.cmd_unmark_dirty();
    }

    pub fn write(&mut self, chunk: Bytes) {
        if self.dirty {
            return;
        }

        if self.in_buff.len() + chunk.len() < MAX_BUFFER_SIZE {
            self.in_buff.extend_from_slice(&chunk);
        } else {
            self.helper.log_warn(&format!(
                "Input buffer overflow: {}/{}",
                self.in_buff.len(),
                MAX_BUFFER_SIZE
            ));
            self.in_buff = BytesMut::with_capacity(IN_BUFFER_SIZE);
            self.is_sync = false;
        }
        if self.is_buffer_process_async {
            self.process_notify.notify_one();
        } else {
            self.process();
        }
    }

    fn has_work(&self) -> bool {
        if self.is_ready && !self.is_buffer_drain() {
            return false;
        }
        if !self.is_sync {
            return self.in_buff.len() >= MUX_BUFFER_SIZE;
        }
        self.in_buff.len() >= packet::TS_PACKET_SIZE
    }

    pub async fn buffer_process(&mut self) -> Result<()> {
        if !self.has_work() {
            self.process_notify.notified().await;
            return Ok(());
        }

        self.process();
        tokio::task::yield_now().await;
        Ok(())
    }

    fn pat_transport_stream_id(&self) -> u16 {
        u16::from_be_bytes([self.pat[8], self.pat[9]])
    }

    fn process(&mut self) {
        if !self.is_sync {
            if Self::sync_buffer(&mut self.in_buff) {
                self.is_sync = true;
            } else {
                return;
            }
        }

        if self.is_sync {
            let mut pointer = 0;
            let available_size =
                (self.in_buff.len() / packet::TS_PACKET_SIZE) * packet::TS_PACKET_SIZE;
            while pointer < available_size {
                if self.is_buffer_process_async && self.is_ready && !self.is_buffer_drain() {
                    break;
                }
                if self.in_buff[pointer] == packet::TS_SYNC_BYTE {
                    let pkt_range = pointer..pointer + packet::TS_PACKET_SIZE;
                    pointer += packet::TS_PACKET_SIZE;

                    let pid = packet::pkt_pid(&self.in_buff[pkt_range.clone()]);
                    let has_biss = self.biss.is_some();
                    let (is_video, is_audio) = {
                        let program = self.get_program();
                        (
                            program.video_pids.contains(&pid),
                            program.audio_pids.contains(&pid),
                        )
                    };
                    let do_descramble = has_biss && (is_video || is_audio);
                    if do_descramble {
                        if let Some(biss) = &self.biss {
                            let pkt_data = &mut self.in_buff[pkt_range.clone()];
                            if let Err(e) = biss.descramble_ts_packet(pkt_data.try_into().unwrap())
                            {
                                self.helper
                                    .log(&format!("BISS descramble error pid {}: {}", pid, e));
                            }
                        }
                    }
                    let mut pkt_data = [0u8; packet::TS_PACKET_SIZE];
                    pkt_data.copy_from_slice(&self.in_buff[pkt_range]);

                    match pid {
                        0x1FFF => continue,
                        0x0000 => {
                            if !self.pat_ready || !self.is_ready {
                                if let Some(mut section) = self.psi_reasm.push(&pkt_data) {
                                    let pat_info = psi::pat::analyze_pat(&section);
                                    if !self.pat_ready {
                                        let program = self.get_program();
                                        // ToDo
                                        // Check pat.program_number == program.program_id, pat.pmt_pid == program.pmt_pid

                                        // keep program_id and pmt_pid
                                        section = psi::pat::patch_pat_section(
                                            &section,
                                            &[(program.program_id, program.pmt_pid)],
                                        );

                                        self.helper.log(&format!("PAT found: {:?}", pat_info));
                                        self.helper.log(&format!(
                                            "Patched PAT: {:?}",
                                            psi::pat::analyze_pat(&section)
                                        ));

                                        self.pat_info = pat_info;

                                        let cc = psi::cached_cc(&self.pat);
                                        self.pat = psi::pack(&section, pid, cc);
                                        self.pat_ready = true;
                                    } else {
                                        if psi::pat::is_pat_changed(&self.pat_info, &pat_info) {
                                            self.helper.log_warn("PAT changed");
                                            self.mark_dirty();
                                        } else if self.dirty {
                                            self.helper.log_warn("PAT restored");
                                            self.unmark_dirty();
                                        }
                                    }
                                }
                            }
                        }
                        0x0011 => {
                            if self.pat_ready && !self.sdt_ready {
                                if let Some(section) = self.psi_reasm.push(&pkt_data) {
                                    let sdts = psi::sdt::analyze_sdt(&section);
                                    if !sdts.is_empty() {
                                        self.helper.log(&format!("SDT found: {:?}", sdts));
                                        let program = self.get_program();
                                        if let Some(sdt) = sdts
                                            .iter()
                                            .find(|sdt| sdt.service_id == program.program_id)
                                        {
                                            let service_type = if program.service_type == 0 {
                                                match sdt.service_type {
                                                    0 => DEFAULT_SERVICE_TYPE,
                                                    s => s as u8,
                                                }
                                            } else {
                                                program.service_type as u8
                                            };

                                            let provider_name =
                                                match program.service_provider_name.trim() {
                                                    "" => sdt.provider_name.trim(),
                                                    _ => program.service_provider_name.trim(),
                                                };

                                            let service_name = match program.service_name.trim() {
                                                "" => sdt.service_name.trim(),
                                                _ => program.service_name.trim(),
                                            };

                                            self.sdt = psi::sdt::prepare_sdt(
                                                self.pat_transport_stream_id(),
                                                program.program_id,
                                                service_type,
                                                provider_name,
                                                service_name,
                                            );
                                            self.sdt_ready = true;

                                            self.helper.log(&format!(
                                                "SDT patched: {:?}",
                                                psi::sdt::analyze_sdt(&section)
                                            ));
                                        } else {
                                            self.helper.log(&format!(
                                                "SDT found, but service_id {} is not present",
                                                program.program_id
                                            ));
                                        }
                                    }
                                }
                            }
                        }
                        id if id == self.get_program().pmt_pid
                            && (!self.pmt_ready || !self.is_ready) =>
                        {
                            if let Some(mut section) = self.psi_reasm.push(&pkt_data) {
                                let pmt_info = psi::pmt::analyze_pmt(&section);
                                if !self.pmt_ready {
                                    let mut es_mapping = Vec::new();
                                    let video_pids = self.get_program().video_pids.clone();
                                    for old_pid in video_pids {
                                        es_mapping.push((old_pid, old_pid));
                                    }
                                    let audio_pids = self.get_program().audio_pids.clone();
                                    for old_pid in audio_pids {
                                        es_mapping.push((old_pid, old_pid));
                                    }
                                    let ca_flag = self.output.ca_flag == config::CA_FLAG_UNSCRAMBLED;

                                    section = psi::pmt::patch_pmt_section(
                                        &section,
                                        pmt_info.pcr_pid,
                                        &es_mapping,
                                        ca_flag,
                                    );

                                    self.helper.log(&format!("PMT found: {:?}", pmt_info));
                                    self.helper.log(&format!(
                                        "Patched PMT: {:?}",
                                        psi::pmt::analyze_pmt(&section)
                                    ));

                                    self.pmt_info = Some(pmt_info);

                                    let cc = psi::cached_cc(&self.pmt);
                                    self.pmt = psi::pack(&section, pid, cc);
                                    self.pmt_ready = true;
                                } else if let Some(cur_pmt_info) = &self.pmt_info {
                                    if psi::pmt::is_pmt_changed(&cur_pmt_info, &pmt_info) {
                                        self.helper.log_warn("PMT changed");
                                        self.mark_dirty();
                                    } else if self.dirty {
                                        self.helper.log_warn("PMT restored");
                                        self.unmark_dirty();
                                    }
                                }
                            }
                        }
                        id if self.is_audio_pid(&id) || self.is_video_pid(&id) => {
                            let mut discontinuity = packet::pkt_get_discontinuity(&pkt_data);
                            if !discontinuity {
                                self.check_cc(&pkt_data, pid);
                            }

                            let mut pusi = false;
                            if let Some((pts, dts)) = packet::pkt_pts_dts(&pkt_data, pid) {
                                pusi = true;
                                if let Some((_last_pts, last_dts)) = self.last_pts_dts.get(&pid) {
                                    if let Some(delta_ms) = dts_forward_delta_ms(dts, *last_dts) {
                                        if delta_ms > DISCONTINUITY_THRESHOLD_MS {
                                            discontinuity = true;
                                            self.helper.log_warn(&format!(
                                                "Discontinuity pid: {:#x}, dts jump: {} ms, v:{}, a:{}", pid, delta_ms, self.out_buff_v.size(), self.out_buff_a.size()
                                            ));
                                        }
                                    }
                                }
                                self.last_pts_dts.insert(pid, (pts, dts));
                            }

                            if let Some((pts, dts)) = self.last_pts_dts.get(&pid) {
                                if self.is_video_pid(&pid) {
                                    let data: [u8; packet::TS_PACKET_SIZE] =
                                        pkt_data.try_into().unwrap();
                                    let mut ts_packet =
                                        TsPacket::new(data, pid, *pts, *dts, pusi, discontinuity);
                                    ts_packet.extract_pcr();
                                    self.out_buff_v.push_back(ts_packet);
                                }
                                if self.is_audio_pid(&pid) {
                                    let data: [u8; packet::TS_PACKET_SIZE] =
                                        pkt_data.try_into().unwrap();
                                    let ts_packet =
                                        TsPacket::new(data, pid, *pts, *dts, pusi, false);
                                    self.out_buff_a.push_back(ts_packet);
                                }
                            }

                            if self.check_buffer_size {
                                if self.is_ready && self.buffer_is_empty() {
                                    self.buffers_clear();
                                    self.last_pts_dts = HashMap::new();
                                }

                                if self.is_ready && self.buffer_is_full() {
                                    self.buffers_clear();
                                    self.last_pts_dts = HashMap::new();
                                }
                            }
                        }
                        _ => {}
                    }
                } else {
                    self.is_sync = false;
                    self.in_buff.advance(pointer);
                    return;
                }
            }
            self.in_buff.advance(pointer);
        }

        if !self.is_ready && self.pat_ready && self.pmt_ready {
            if !self.sdt_ready {
                let program = self.get_program();
                let service_type = match program.service_type {
                    0 => DEFAULT_SERVICE_TYPE,
                    s => s as u8,
                };
                let provider_name = program.service_provider_name.trim();
                let service_name = program.service_name.trim();

                self.sdt = psi::sdt::prepare_sdt(
                    self.pat_transport_stream_id(),
                    program.program_id,
                    service_type,
                    provider_name,
                    service_name,
                );
                self.sdt_ready = true;

                self.helper.log("Generated SDT from config");
            }

            match (self.out_buff_v.front(), self.out_buff_v.back()) {
                (Some(first), Some(last)) => {
                    if dts_after(first.dts, last.dts) {
                        self.helper.log(&format!(
                            "Wrong DTS, flushing: {} - {}",
                            first.dts, last.dts
                        ));
                        self.buffers_clear();
                    } else {
                        let buffer_duration = self.out_buff_v.duration();

                        if buffer_duration >= self.buffer_duration_ms {
                            let buff_size = self.out_buff_v.size() + self.out_buff_a.size();

                            self.min_buff_size = buff_size * 1_000 / buffer_duration as usize / 2;
                            self.max_buff_size =
                                buff_size * BUFFER_DURATION_MS * MAX_BUFFER_DURATION_COEF / buffer_duration as usize;

                            let bitrate = buff_size as u64 * 8 / buffer_duration;
                            let msg = format!(
                                "Buffer ready: duration {} ms, bitrate {} kbit/sec, size {} bytes, min {} bytes",
                                buffer_duration, bitrate, buff_size, self.min_buff_size
                            );

                            if let Some(r) = self.pcr_restamper.as_mut() {
                                if r.target_bitrate_bps == 0 {
                                    r.target_bitrate_bps = (bitrate as f64 * 1.2 * 1024.00) as u64;
                                }
                            }

                            self.helper.log_info(&msg);
                            self.resync(first.dts);
                            self.fps = self.calculate_fps().unwrap_or(0.0);
                            self.is_ready = true;
                            self.cc_errors = HashMap::new();
                            self.stats.update_ready_time(Utc::now());
                        };
                    }
                }
                _ => {}
            }
        };
    }

    pub fn update_stats(&mut self) {
        self.stats.update_in_buffer(&self.in_buff);
        self.stats.update_v_buffer(&self.out_buff_v);
        self.stats.update_a_buffer(&self.out_buff_a);
        self.stats.update_cc(&self.cc_errors);
        self.stats.update_bitrate_psi(&mut self.count_psi_packets);
        self.stats
            .update_bitrate_payload(&mut self.count_payload_packets);
        self.stats.update_bitrate_null(&mut self.count_null_packets);
        self.stats
            .update_bitrate_adjust(&mut self.count_adjust_packets);
        let fps = self.calculate_fps().unwrap_or(self.fps);
        self.stats.update_fps(&fps);
        self.stats.update_jitter(self.jitter_ms);
        self.stats.update_adjust_buf(self.adjust_buf);
        self.stats.update_dirty(self.dirty);
    }

    fn check_cc(&mut self, packet: &[u8], pid: u16) {
        if packet::pkt_has_payload(packet) {
            let current_cc = packet[3] & 0x0F;
            if let Some(last_cc) = self.last_cc.get(&pid) {
                let expected_cc = (last_cc + 1) & 0x0F;
                if current_cc != expected_cc {
                    self.helper.log_warn(&format!(
                        "Wrong cc: pid [{:#x}] received {} but expected {}",
                        pid, current_cc, expected_cc
                    ));
                    self.update_cc(pid);
                }
            }
            self.last_cc.insert(pid, current_cc);
        }
    }

    fn update_cc(&mut self, pid: u16) {
        self.cc_errors
            .entry(pid)
            .and_modify(|cc| *cc += 1)
            .or_insert(1);
    }

    fn buffers_clear(&mut self) {
        let msg = format!(
            "Buffers clear: v:{},  a:{}",
            self.out_buff_v.size(),
            self.out_buff_a.size()
        );
        self.is_ready = false;
        self.helper.log_warn(&msg);
        self.out_buff_v.clear();
        self.out_buff_a.clear();
        self.min_buff_size = 0;
        self.max_buff_size = 0;
    }

    fn buffer_is_full(&mut self) -> bool {
        let res = self.out_buff_v.size() + self.out_buff_a.size() > self.max_buff_size
            || self.out_buff_v.duration() > 2 * BUFFER_DURATION_MS as u64;

        if res {
            self.helper.log(&format!(
                "Buffer full: v:{}, a:{}, max:{}, duration:{}",
                self.out_buff_v.size(),
                self.out_buff_a.size(),
                self.max_buff_size,
                self.out_buff_v.duration()
            ));
        };

        res
    }

    fn buffer_is_empty(&mut self) -> bool {
        let res = self.out_buff_v.size() + self.out_buff_a.size() < self.min_buff_size;

        if res {
            self.helper.log(&format!(
                "Buffer empty: v:{}, a:{}, min:{}",
                self.out_buff_v.size(),
                self.out_buff_a.size(),
                self.min_buff_size
            ));
        };

        res
    }

    fn is_buffer_drain(&self) -> bool {
        self.out_buff_v.duration() < BUFFER_DURATION_MS as u64
    }

    fn is_video_pid(&self, pid: &u16) -> bool {
        self.get_program().video_pids.contains(&pid)
    }

    fn is_audio_pid(&self, pid: &u16) -> bool {
        self.get_program().audio_pids.contains(&pid)
    }

    fn flush_psi_packets(&mut self, out: &mut BytesMut) -> usize {
        let pkt_count = 1;

        let mut flush_count = 0;
        let pcr = self
                .pcr_restamper
                .as_ref()
                .and_then(|r| r.get_pcr()
        );
        if self.pat_ready {
            match (pcr, self.timers.insert_pat) {
                (Some(pcr), Some(ins_pat)) => {
                    let limit = pcr_add(ins_pat, PERIOD_PAT_MS * 27_000);
                    if pcr_after(pcr, limit) {
                        self.timers.insert_pat = Some(pcr);
                        packet::pkt_next(self.pat.as_mut_slice());
                        out.extend_from_slice(self.pat.as_slice());
                        flush_count += 1;
                    }
                }
                (Some(pcr), None) => {
                    self.timers.insert_pat = Some(pcr);
                    packet::pkt_next(self.pat.as_mut_slice());
                    out.extend_from_slice(self.pat.as_slice());
                    flush_count += 1;
                }
                _ => {}
            }
        }
        if self.pmt_ready {
            if pkt_count > flush_count {
                match (pcr, self.timers.insert_pmt) {
                    (Some(pcr), Some(ins_pmt)) => {
                        let limit = pcr_add(ins_pmt, PERIOD_PMT_MS * 27_000);
                        if pcr_after(pcr, limit) {
                            self.timers.insert_pmt = Some(pcr);
                            packet::pkt_next(self.pmt.as_mut_slice());
                            out.extend_from_slice(self.pmt.as_slice());
                            flush_count += 1;
                        }
                    }
                    (Some(pcr), None) => {
                        self.timers.insert_pmt = Some(pcr);
                        packet::pkt_next(self.pmt.as_mut_slice());
                        out.extend_from_slice(self.pmt.as_slice());
                        flush_count += 1;
                    }
                    _ => {}
                }
            }
        }
        if self.sdt_ready {
            if pkt_count > flush_count {
                match (pcr, self.timers.insert_sdt) {
                    (Some(pcr), Some(ins_sdt)) => {
                        let limit = pcr_add(ins_sdt, PERIOD_SDT_MS * 27_000);
                        if pcr_after(pcr, limit) {
                            self.timers.insert_sdt = Some(pcr);
                            packet::pkt_next(self.sdt.as_mut_slice());
                            out.extend_from_slice(self.sdt.as_slice());
                            flush_count += 1;
                        }
                    }
                    (Some(pcr), None) => {
                        self.timers.insert_sdt = Some(pcr);
                        packet::pkt_next(self.sdt.as_mut_slice());
                        out.extend_from_slice(self.sdt.as_slice());
                        flush_count += 1;
                    }
                    _ => {}
                }
            }
        }

        flush_count
    }

    pub fn flush_adjust_buf_packets(&self, out: &mut BytesMut) -> usize {
        let mut flush_count: usize = 0;
        if self.adjust_buf > 0 {
            let d = self.out_buff_v.duration() as f64 / BUFFER_DURATION_MS as f64;
            let adjust_buf = 1.0 - self.adjust_buf as f64 / 100.0;
            if d < adjust_buf {
                out.extend_from_slice(&packet::TS_NULL_PACKET);
                flush_count = 1;
            }
        }

        flush_count
    }

    pub async fn proxy_packet<F, Fut>(&mut self, chunk: Bytes, handler: F) -> usize
    where
        F: Fn(Bytes) -> Fut,
        Fut: Future<Output = Result<usize>>,
    {
        let mut send_bytes: usize = 0;

        if self.output.pkt_size == 0 {
            send_bytes = chunk.len();
            let _ = handler(chunk).await;
            return send_bytes;
        }

        self.in_buff.extend_from_slice(&chunk);

        if !self.is_sync {
            if Self::sync_buffer(&mut self.in_buff) {
                self.is_sync = true;
            } else {
                return send_bytes;
            }
        }
        if self.is_sync {
            while self.in_buff.len() >= self.output.pkt_size as usize {
                let pkt = self
                    .in_buff
                    .split_to(self.output.pkt_size as usize)
                    .freeze();

                match handler(pkt).await {
                    Ok(i) => {
                        send_bytes += i;
                    }
                    Err(e) => {
                        self.helper.log(&format!("proxy send error: {:?}", e));
                    }
                }
            }
        }

        send_bytes
    }

    pub fn flush_frame(&mut self) -> Option<Bytes> {
        if self.dirty || !self.is_ready {
            return None;
        };

        while self.frame_buff.len() < FRAME_BUFFER_SIZE {
            match self.flush_packet_for_vbr() {
                Some(pkt) => self.frame_buff.extend_from_slice(&pkt),
                None => break,
            }
        }

        if self.frame_buff.len() >= FRAME_BUFFER_SIZE {
            let out = self.frame_buff.split_to(FRAME_BUFFER_SIZE).freeze();
            return Some(out);
        }

        None
    }


    fn flush_packet_for_vbr(&mut self) -> Option<Bytes> {
        if self.dirty || !self.is_ready {
            return None;
        };

        let pkt_size = self.get_pkt_size() as usize;

        let mut pkt_count = pkt_size / packet::TS_PACKET_SIZE;

        let mut out = BytesMut::with_capacity(pkt_size);

        let flush_count = self.flush_psi_packets(&mut out);
        if flush_count > 0 {
            self.count_psi_packets += flush_count;
            pkt_count = pkt_count.saturating_sub(flush_count);
        }

        let mut first_pcr: Option<u64> = None;

        for _ in 0..pkt_count {
            match self.get_a_or_v() {
                Some(pkt) => {
                    self.count_payload_packets += 1;
                    out.extend_from_slice(&pkt.data);
                    if let Some(pcr) = pkt.pcr {
                        if first_pcr.is_none() {
                            first_pcr = Some(pcr);
                        }
                    }
                },
                None => break
            }
        }
        if let Some(pcr) = first_pcr {
            if let Some(r) = self.pcr_restamper.as_mut() {
                r.first_pcr_value = Some(pcr);
            }
        }



        if !out.is_empty() {
            Some(out.freeze())
        } else {
            None
        }
    }

    pub fn flush_packet_for_cbr(&mut self) -> Option<Bytes> {
        if self.dirty || !self.is_ready {
            return None;
        };

        let mut out = BytesMut::with_capacity(self.output.pkt_size as usize);

        let pkt_size = self.get_pkt_size();

        let mut pkt_count = pkt_size as usize / packet::TS_PACKET_SIZE;

        let flush_count = self.flush_psi_packets(&mut out);
        if flush_count > 0 {
            if let Some(r) = self.pcr_restamper.as_mut() {
                r.total_packets_sent += flush_count as u64;
            }
            self.count_psi_packets += flush_count;
            pkt_count = pkt_count.saturating_sub(flush_count);
        }


        let mut is_adjusted = false;
        if pkt_count > 0 {
            let flush_count = self.flush_adjust_buf_packets(&mut out);
            if flush_count > 0 {
                is_adjusted = true;
                if let Some(r) = self.pcr_restamper.as_mut() {
                    r.total_packets_sent += flush_count as u64;
                }
                self.count_adjust_packets += flush_count;
                pkt_count = pkt_count.saturating_sub(flush_count);
            }
        }

        if pkt_count > 0 {
            for _ in 0..pkt_count {
                if let Some(r) = self.pcr_restamper.as_mut() {
                    r.total_packets_sent += 1;
                }

                let packet = self.get_a_or_v_ready();

                let data = match packet {
                    Some(pkt) => {
                        self.count_payload_packets += 1;
                        self.process_packet(pkt)
                    }
                    None => {
                        let pkt = if !is_adjusted && self.adjust_buf > 0 &&
                            self.in_buff.len() > (self.out_buff_v.size() as f64 * self.adjust_buf as f64 / 100.0) as usize
                        {
                            self.get_v()
                        } else {
                            None
                        };

                        if let Some(pkt) = pkt {
                            self.count_payload_packets += 1;
                            self.process_packet(pkt)
                        } else {
                            self.count_null_packets += 1;
                            packet::TS_NULL_PACKET
                        }
                    }
                };

                out.extend_from_slice(&data);
            }
        }

        Some(out.freeze())
    }

     fn process_packet(&mut self, pkt: TsPacket) -> [u8; packet::TS_PACKET_SIZE] {
        let mut data = pkt.data.clone();

        if pkt.discontinuity {
            packet::pkt_set_discontinuity(&mut data);
            self.resync(pkt.dts);
        }

        if self.is_video_pid(&pkt.pid) {
            let dts_drift = self.dts_drift(pkt.dts);
            if dts_drift < 0 {
                self.resync(pkt.dts);
            }
        }

        if pkt.pid == self.get_program().pcr_pid {
            let mut restamp_result = None;

            if let Some(r) = self.pcr_restamper.as_mut() {
                let orig_pcr = r.extract_pcr_safe(&data);
                if let Some(new_pcr) = r.process_packet(&mut data) {
                    restamp_result = Some((orig_pcr, new_pcr));
                }
            }

            if let Some((orig_pcr, new_pcr)) = restamp_result {
                if let Some(orig_pcr) = orig_pcr {
                    self.pcr_drift(orig_pcr, new_pcr);
                }
            }
        }

        data
    }

    fn resync(&mut self, dts: u64) {
        if let Some(r) = self.pcr_restamper.as_mut() {
            let dts = dts.wrapping_sub(DTS_PCR_DIFF_MS * 90) & DTS_MASK;
            r.resync(dts);
        }
    }

    fn get_a_or_v_ready(&mut self) -> Option<TsPacket> {
        let packet = match (self.out_buff_a.front(), self.out_buff_v.front()) {
            (Some(a), Some(v)) if dts_after(a.dts, v.dts) && self.ready_send(v.dts) => {
                self.out_buff_v.pop_front()
            }
            (Some(a), Some(v)) if dts_before_or_equal(a.dts, v.dts) && self.ready_send(a.dts) => {
                self.out_buff_a.pop_front()
            }
            (Some(a), None) if self.ready_send(a.dts) => self.out_buff_a.pop_front(),
            (None, Some(v)) if self.ready_send(v.dts) => self.out_buff_v.pop_front(),
            _ => None,
        };
        if self.is_buffer_process_async && packet.is_some() {
            self.process_notify.notify_one();
        };

        packet
    }

    fn get_a_or_v(&mut self) -> Option<TsPacket> {
        let packet = match (self.out_buff_a.front(), self.out_buff_v.front()) {
            (Some(a), Some(v)) => {
                if dts_after(a.dts, v.dts) {
                    self.out_buff_v.pop_front()
                } else {
                    self.out_buff_a.pop_front()
                }
            }
            (Some(_), None) => self.out_buff_a.pop_front(),
            (None, Some(_)) => self.out_buff_v.pop_front(),
            (None, None) => None,
        };
        if self.is_buffer_process_async && packet.is_some() {
            self.process_notify.notify_one();
        };

        packet
    }

    fn get_v(&mut self) -> Option<TsPacket> {
        let packet = match self.out_buff_v.front() {
            Some(_) => self.out_buff_v.pop_front(),
            None => None,
        };
        if self.is_buffer_process_async && packet.is_some() {
            self.process_notify.notify_one();
        };

        packet
    }

    pub fn ready_send(&self, pts_or_dts: u64) -> bool {
        if let Some(current_pcr) = self.pcr_restamper.as_ref().and_then(|r| r.get_pcr()) {
            let ticks = pcr_normalize(pts_or_dts * 300);
            let limit = pcr_add(current_pcr, self.jitter_ms * 27_000);
            !pcr_after(ticks, limit)
        } else {
            false
        }
    }

    pub fn get_program(&self) -> &Program {
        self.output.programs.first().unwrap()
    }

    fn dts_drift(&mut self, dts: u64) -> i64 {
        let mut lead_time_ms: i64 = 0;
        if let Some(r) = &self.pcr_restamper {
            if let Some(current_pcr) = r.get_pcr() {
                let pcr_90k = current_pcr / 300;
                lead_time_ms = dts_signed_delta(dts, pcr_90k) / 90;
            }
        }
        self.stats.update_drift(lead_time_ms);
        lead_time_ms
    }

    fn pcr_drift(&mut self, orig_pcr: u64, current_pcr: u64) -> i64 {
        let lead_pcr_ms = pcr_signed_delta(orig_pcr, current_pcr) / 27_000;
        self.stats.update_pcr_drift(lead_pcr_ms);
        lead_pcr_ms
    }
}

#[derive(Clone)]
pub struct PcrRestamper {
    target_bitrate_bps: u64,
    total_packets_sent: u64,
    first_pcr_value: Option<u64>, // 27 MHz
}

impl PcrRestamper {
    pub fn new(bitrate_bps: u64) -> Self {
        Self {
            target_bitrate_bps: bitrate_bps,
            total_packets_sent: 0,
            first_pcr_value: None,
        }
    }
    pub fn resync(&mut self, dts_90k: u64) {
        self.first_pcr_value = Some(pcr_normalize(dts_90k * 300));
        self.total_packets_sent = 0;
    }

    fn process_packet(&mut self, packet: &mut [u8; packet::TS_PACKET_SIZE]) -> Option<u64> {
        if packet::pkt_is_pcr_present(packet) {
            let new_pcr = self.get_pcr()?;
            self.write_pcr_safe(packet, new_pcr);
            self.total_packets_sent = 0;
            self.first_pcr_value = Some(new_pcr);

            return Some(new_pcr);
        };

        None
    }

    fn extract_pcr_safe(&self, packet: &[u8; packet::TS_PACKET_SIZE]) -> Option<u64> {
        packet::pkt_extract_pcr(packet)
    }

    fn write_pcr_safe(&self, packet: &mut [u8; packet::TS_PACKET_SIZE], pcr: u64) {
        packet::pkt_write_pcr(packet, pcr);
    }

    fn get_raw_pcr(&self) -> Option<u64> {
        let last_pcr = self.first_pcr_value?;
        if self.target_bitrate_bps != 0 {
            let bits_sent: u128 =
                self.total_packets_sent as u128 * packet::TS_PACKET_SIZE as u128 * 8u128;
            let ticks = ((bits_sent * PCR_HZ) / self.target_bitrate_bps as u128) as u64 + last_pcr;

            Some(ticks)
        } else {
            self.first_pcr_value
        }
    }

    fn get_pcr(&self) -> Option<u64> {
        self.get_raw_pcr().map(pcr_normalize)
    }
}
