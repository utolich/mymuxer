use super::es_parsers::{
    EsParser, EsParser2, EsParser3, EsParser4, EsParser15, EsParser27, EsParserUnknown,
};
use super::pes::Pes;
use crate::packet::{TS_PACKET_SIZE, TS_SYNC_BYTE, pkt_pid};
use bytes::BytesMut;
use std::collections::HashMap;

pub struct PesReassembler {
    pid: u16,
    stream_type: u8,
    buffer: BytesMut,
}

impl PesReassembler {
    pub fn new(pid: u16, stream_type: u8) -> Self {
        Self {
            pid,
            stream_type,
            buffer: BytesMut::with_capacity(1024),
        }
    }

    pub fn push(&mut self, packet: &[u8]) -> Option<Pes> {
        if packet.len() < 188 || packet[0] != 0x47 {
            return None;
        }

        let pid = ((packet[1] as u16 & 0x1F) << 8) | packet[2] as u16;
        if pid != self.pid {
            return None;
        }

        // PUSI
        let pusi = (packet[1] & 0x40) != 0;

        // Визначаємо початок Payload (PES-заголовка)
        let af_control = (packet[3] >> 4) & 0x03;
        let mut offset = 4;
        match af_control {
            0 => return None, // Reserved
            1 => { /* Тільки Payload, offset = 4, нічого не робимо */ }
            2 => return None, // Тільки Adaptation Field (немає PES)
            3 => {
                // AF + Payload
                let af_length = packet[4] as usize;
                // offset = 4 (header) + 1 (af_length byte) + сама довжина AF
                offset += 1 + af_length;
            }
            _ => unreachable!(),
        }
        if pusi {
            if !self.buffer.is_empty() {
                if let Some(result) = Parser::parse(&self.buffer, pid, Some(self.stream_type)) {
                    if result.es.is_some() {
                        return Some(result);
                    }
                }
            }
            self.buffer.clear();
            self.buffer.extend_from_slice(&packet[offset..]);
        } else if !self.buffer.is_empty() {
            self.buffer.extend_from_slice(&packet[offset..]);
        }

        None
    }
}

pub struct PesReassemblerMap {
    per_pid: HashMap<u16, PesReassembler>,
}

impl PesReassemblerMap {
    pub fn new() -> Self {
        Self {
            per_pid: HashMap::new(),
        }
    }

    pub fn push(&mut self, packet: &[u8], pid_to_stream: &HashMap<u16, u8>) -> Option<Pes> {
        if packet.len() < TS_PACKET_SIZE || packet[0] != TS_SYNC_BYTE {
            return None;
        }

        let pid = pkt_pid(packet);
        let Some(&stream_type) = pid_to_stream.get(&pid) else {
            return None;
        };

        let reasm = self
            .per_pid
            .entry(pid)
            .or_insert_with(|| PesReassembler::new(pid, stream_type));

        reasm.push(packet)
    }
}

pub struct Parser;

impl Parser {
    pub fn parse(data: &[u8], pid: u16, stream_type: Option<u8>) -> Option<Pes> {
        if data.len() < 4 {
            return None;
        }

        let mut pes = Pes::new(pid);
        let mut pointer = 0usize;
        if data.len() < pointer + 4 {
            return None;
        }
        let start_code = u32::from_be_bytes([
            data[pointer],
            data[pointer + 1],
            data[pointer + 2],
            data[pointer + 3],
        ]);
        pointer += 4;

        if (start_code >> 8) != 0x000001 {
            return None;
        }

        if data.len() < pointer + 3 {
            return None;
        }

        let tmp_marker = data[pointer + 2];
        if (tmp_marker >> 6) != 0x02 {
            return None;
        }

        // Matches the PHP behavior: uses the marker byte as stream_id.
        pes.stream_id = tmp_marker & 0xFF;

        if data.len() < pointer + 2 {
            return None;
        }
        let _pes_packet_len = u16::from_be_bytes([data[pointer], data[pointer + 1]]);
        pointer += 2;

        if data.len() < pointer + 2 {
            return None;
        }
        let flags = u16::from_be_bytes([data[pointer], data[pointer + 1]]);
        pointer += 2;

        pes.scrabling = ((flags >> 12) & 0x03) as u8;
        pes.priority = ((flags >> 11) & 0x01) as u8;
        pes.alignment_indicator = ((flags >> 10) & 0x01) as u8;
        pes.copyright = ((flags >> 9) & 0x01) as u8;
        pes.original_or_copy = ((flags >> 8) & 0x01) as u8;
        pes.flags_pts_dts = ((flags >> 6) & 0x03) as u8;
        pes.flag_escr = ((flags >> 5) & 0x01) as u8;
        pes.flag_es_rate = ((flags >> 4) & 0x01) as u8;
        pes.flag_dsm_trick_mode = ((flags >> 3) & 0x01) as u8;
        pes.flag_additional_copy_info = ((flags >> 2) & 0x01) as u8;
        pes.flag_crc = ((flags >> 1) & 0x01) as u8;
        pes.flag_extension = (flags & 0x01) as u8;

        if data.len() <= pointer {
            return None;
        }

        let pes_header_len = data[pointer] as usize;
        pointer += 1 + pes_header_len;

        if data.len() < pointer {
            return None;
        }

        if let Some(stream_type) = stream_type {
            let es_data = &data[pointer..];
            let parser: Box<dyn EsParser> = match stream_type {
                2 => Box::new(EsParser2::new()),
                3 => Box::new(EsParser3::new()),
                4 => Box::new(EsParser4::new()),
                15 => Box::new(EsParser15::new()),
                27 => Box::new(EsParser27::new()),
                _ => Box::new(EsParserUnknown::new()),
            };
            pes.es = parser.parse(es_data);
        }

        Some(pes)
    }
}
