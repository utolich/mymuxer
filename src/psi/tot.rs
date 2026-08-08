use crate::psi::{encode_dvb_time, high, low, section_with_crc, set_section_length, Section, get_local_offset_bcd};
use chrono::Utc;

pub const PID: u16 = 0x0014;

pub fn section(now: chrono::DateTime<Utc>, country_code: &str, timezone: &str) -> Section {
    let timezone_bcd = get_local_offset_bcd(timezone);

    let mut local_time_offset_descriptor = vec![0x58, 13];
    local_time_offset_descriptor.extend_from_slice(country_code[0..3].as_bytes());
    local_time_offset_descriptor.push(0x00);
    local_time_offset_descriptor.extend_from_slice(&timezone_bcd);
    local_time_offset_descriptor.extend_from_slice(&[
        0xFF,
        0xFF,
        0xFF,
        0xFF,
        0xFF,
    ]);
    local_time_offset_descriptor.extend_from_slice(&timezone_bcd);

    let desc_loop_len = local_time_offset_descriptor.len() as u16;
    let mut bytes = vec![0x73, 0x70, 0x00];
    bytes.extend_from_slice(&encode_dvb_time(now));
    bytes.push(0xF0 | high(desc_loop_len));
    bytes.push(low(desc_loop_len));
    bytes.extend_from_slice(&local_time_offset_descriptor);
    set_section_length(&mut bytes);

    section_with_crc(PID, bytes)
}
