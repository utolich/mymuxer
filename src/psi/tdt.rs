use crate::psi::{Section, encode_dvb_time};
use chrono::Utc;

pub const PID: u16 = 0x0014;

pub fn section(now: chrono::DateTime<Utc>) -> Section {
    let mut bytes = Vec::with_capacity(8);
    bytes.extend_from_slice(&[0x70, 0x70, 0x05]);
    bytes.extend_from_slice(&encode_dvb_time(now));

    Section { pid: PID, bytes }
}
