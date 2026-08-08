use crate::packet;
use crate::packet::MPEG2_CRC;
use crate::packet::{TS_PACKET_SIZE, TS_SYNC_BYTE, pkt_pid};
use chrono::{DateTime, Datelike, Local, Offset, Timelike, Utc};
use chrono_tz::Tz;
use std::collections::HashMap;

pub(crate) mod pat;
pub(crate) mod pmt;
pub(crate) mod sdt;
pub mod eit;
pub mod tdt;
pub mod tot;

pub struct Section {
    pub pid: u16,
    pub bytes: Vec<u8>,
}

pub struct SectionReassembler {
    pid: u16,
    buffer: Vec<u8>,
    expected_len: Option<usize>,
}

impl SectionReassembler {
    pub fn new(pid: u16) -> Self {
        Self {
            pid,
            buffer: Vec::with_capacity(1024),
            expected_len: None,
        }
    }

    pub fn push(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        if packet.len() < TS_PACKET_SIZE || packet[0] != TS_SYNC_BYTE {
            self.clear();
            return None;
        }

        if pkt_pid(packet) != self.pid {
            return None;
        }

        let pusi = (packet[1] & 0x40) != 0;
        let mut offset = payload_offset(packet)?;

        if pusi {
            if offset >= packet.len() {
                self.clear();
                return None;
            }

            let pointer = packet[offset] as usize;
            offset += 1;

            let pointer_end = offset.saturating_add(pointer);
            if pointer_end > packet.len() {
                self.clear();
                return None;
            }

            if !self.buffer.is_empty() && pointer > 0 {
                self.buffer.extend_from_slice(&packet[offset..pointer_end]);
                if let Some(section) = self.take_complete() {
                    self.clear();
                    return Some(section);
                }
            }

            self.clear();
            offset = pointer_end;
        } else if self.buffer.is_empty() {
            return None;
        }

        if offset >= packet.len() {
            return None;
        }

        self.buffer.extend_from_slice(&packet[offset..]);
        self.take_complete()
    }

    fn take_complete(&mut self) -> Option<Vec<u8>> {
        if self.expected_len.is_none() && self.buffer.len() >= 3 {
            let section_len =
                (((self.buffer[1] as u16 & 0x0F) << 8) | self.buffer[2] as u16) as usize;
            let total_len = section_len + 3;
            if total_len > 3 + 4095 {
                self.clear();
                return None;
            }
            self.expected_len = Some(total_len);
        }

        let expected_len = self.expected_len?;
        if self.buffer.len() < expected_len {
            return None;
        }

        let section = self.buffer[..expected_len].to_vec();
        self.clear();
        Some(section)
    }

    fn clear(&mut self) {
        self.buffer.clear();
        self.expected_len = None;
    }
}

pub struct SectionReassemblerMap {
    per_pid: HashMap<u16, SectionReassembler>,
}

impl SectionReassemblerMap {
    pub fn new() -> Self {
        Self {
            per_pid: HashMap::new(),
        }
    }

    pub fn push(&mut self, packet: &[u8]) -> Option<Vec<u8>> {
        if packet.len() < TS_PACKET_SIZE || packet[0] != TS_SYNC_BYTE {
            return None;
        }

        let pid = pkt_pid(packet);
        let reasm = self
            .per_pid
            .entry(pid)
            .or_insert_with(|| SectionReassembler::new(pid));

        reasm.push(packet)
    }
}

pub fn pack(section: &[u8], pid: u16, cc: u8) -> [u8; TS_PACKET_SIZE] {
    let mut next_cc = cc;
    pack_multi(section, pid, &mut next_cc)
        .into_iter()
        .next()
        .unwrap_or([0xFF; TS_PACKET_SIZE])
}

pub fn pack_multi(section: &[u8], pid: u16, cc: &mut u8) -> Vec<[u8; TS_PACKET_SIZE]> {
    let mut packets = Vec::new();
    let mut offset = 0;
    let mut first = true;

    while first || offset < section.len() {
        let mut packet = [0xFF; TS_PACKET_SIZE];

        packet[0] = TS_SYNC_BYTE;
        packet[1] = (((pid >> 8) as u8) & 0x1F) | if first { 0x40 } else { 0x00 };
        packet[2] = (pid & 0xFF) as u8;
        packet[3] = 0x10 | (*cc & 0x0F);
        *cc = (*cc + 1) & 0x0F;

        let mut payload = 4;
        if first {
            packet[payload] = 0x00;
            payload += 1;
            first = false;
        }

        let capacity = TS_PACKET_SIZE - payload;
        let copy_len = capacity.min(section.len().saturating_sub(offset));
        packet[payload..payload + copy_len].copy_from_slice(&section[offset..offset + copy_len]);
        offset += copy_len;

        packets.push(packet);
    }

    packets
}

fn payload_offset(packet: &[u8]) -> Option<usize> {
    if packet.len() < TS_PACKET_SIZE {
        return None;
    }

    let af_control = (packet[3] >> 4) & 0x03;
    let mut offset = 4;

    match af_control {
        0 => return None,
        1 => {}
        2 => return None,
        3 => {
            if packet.len() <= offset {
                return None;
            }
            offset += packet[offset] as usize + 1;
        }
        _ => unreachable!(),
    }

    if offset > packet.len() {
        return None;
    }

    Some(offset)
}

fn calculate_mpeg2_crc32(data: &[u8]) -> u32 {
    MPEG2_CRC.checksum(data)
}

pub(crate) fn section_with_crc(pid: u16, mut bytes: Vec<u8>) -> Section {
    let crc = calculate_mpeg2_crc32(&bytes);
    bytes.extend_from_slice(&crc.to_be_bytes());
    Section { pid, bytes }
}

pub(crate) fn set_section_length(section: &mut [u8]) {
    let len = (section.len() - 3 + 4) as u16;
    section[1] = (section[1] & 0xF0) | high(len);
    section[2] = low(len);
}

pub(crate) fn dvb_text(text: &str, max_len: usize) -> Vec<u8> {
    let budget = max_len.saturating_sub(1);
    let mut end = text.len().min(budget);
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }

    let mut out = vec![0x15];
    out.extend_from_slice(text[..end].as_bytes());
    out
}

pub(crate) fn encode_dvb_time(dt: chrono::DateTime<Utc>) -> [u8; 5] {
    let mjd = mjd(dt.year(), dt.month() as i32, dt.day() as i32);
    [
        high(mjd),
        low(mjd),
        bcd(dt.hour() as u8),
        bcd(dt.minute() as u8),
        bcd(dt.second() as u8),
    ]
}

pub(crate) fn encode_duration(seconds: u32) -> [u8; 3] {
    let h = (seconds / 3600).min(99) as u8;
    let m = ((seconds % 3600) / 60) as u8;
    let s = (seconds % 60) as u8;
    [bcd(h), bcd(m), bcd(s)]
}

fn mjd(mut year: i32, mut month: i32, day: i32) -> u16 {
    if month <= 2 {
        year -= 1;
        month += 12;
    }
    let a = year / 100;
    let b = 2 - a + a / 4;
    let jd = (365.25 * (year + 4716) as f64).floor() as i32
        + (30.6001 * (month + 1) as f64).floor() as i32
        + day
        + b
        - 1524;
    (jd - 2_400_001) as u16
}

fn get_local_offset_bcd(timezone: &str) -> [u8; 2] {
    let tz: Tz = timezone.parse().unwrap();

    let now = Utc::now();
    let local = now.with_timezone(&tz);

    let offset_seconds = local.offset().fix().local_minus_utc();
    let offset_minutes = offset_seconds.abs() / 60;
    let hours = (offset_minutes / 60) as u8;
    let minutes = (offset_minutes % 60) as u8;
    [bcd(hours), bcd(minutes)]
}

fn bcd(value: u8) -> u8 {
    ((value / 10) << 4) | (value % 10)
}

pub(crate) fn high(value: u16) -> u8 {
    (value >> 8) as u8
}

pub(crate) fn low(value: u16) -> u8 {
    value as u8
}

pub(crate) fn cached_cc(cached_pkt: &[u8; packet::TS_PACKET_SIZE]) -> u8 {
    if cached_pkt[0] == packet::TS_SYNC_BYTE {
        cached_pkt[3] & 0x0F
    } else {
        0
    }
}

fn psi_header(section: &[u8]) -> Option<(u8, u16, u8, bool)> {
    if section.len() < 8 {
        return None;
    }

    let table_id = section[0];
    let table_id_extension = u16::from_be_bytes([section[3], section[4]]);
    let version_number = (section[5] >> 1) & 0x1F;
    let current_next = (section[5] & 0x01) != 0;

    Some((table_id, table_id_extension, version_number, current_next))
}

fn cached_psi_header(cached_pkt: &[u8; packet::TS_PACKET_SIZE]) -> Option<(u8, u16, u8, bool)> {
    if cached_pkt[0] != packet::TS_SYNC_BYTE || cached_pkt[1] & 0x40 == 0 {
        return None;
    }

    let af_control = (cached_pkt[3] >> 4) & 0x03;
    let mut offset = 4;

    match af_control {
        0 | 2 => return None,
        1 => {}
        3 => {
            let af_len = cached_pkt[offset] as usize;
            offset = offset.checked_add(1)?.checked_add(af_len)?;
            if offset >= cached_pkt.len() {
                return None;
            }
        }
        _ => unreachable!(),
    }

    let pointer = cached_pkt[offset] as usize;
    offset = offset.checked_add(1)?.checked_add(pointer)?;
    let header_end = offset.checked_add(8)?;
    if header_end > packet::TS_PACKET_SIZE {
        return None;
    }

    psi_header(&cached_pkt[offset..header_end])
}

pub(crate) fn psi_section_changed(
    cached_pkt: &[u8; packet::TS_PACKET_SIZE],
    section: &[u8],
) -> bool {
    let Some((table_id, table_id_extension, version_number, current_next)) = psi_header(section)
    else {
        return false;
    };

    if !current_next {
        return false;
    }

    let Some((
        cached_table_id,
        cached_table_id_extension,
        cached_version_number,
        cached_current_next,
    )) = cached_psi_header(cached_pkt)
    else {
        return false;
    };

    table_id != cached_table_id
        || table_id_extension != cached_table_id_extension
        || version_number != cached_version_number
        || !cached_current_next
}
