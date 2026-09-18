use bytes::Bytes;
use crc::{CRC_32_MPEG_2, Crc};
pub const MPEG2_CRC: Crc<u32> = Crc::<u32>::new(&CRC_32_MPEG_2);

pub const TS_PACKET_SIZE: usize = 188;
pub const TS_SYNC_BYTE: u8 = 0x47;

pub const TS_NULL_PACKET: [u8; TS_PACKET_SIZE] = {
    let mut packet = [0xFF; TS_PACKET_SIZE];
    packet[0] = 0x47;
    packet[1] = 0x1F;
    packet[2] = 0xFF;
    packet[3] = 0x10;
    packet
};

pub fn null_pkt() -> Bytes {
    Bytes::from_static(&TS_NULL_PACKET)
}

pub fn pkt_pid(pkt: &[u8]) -> u16 {
    ((pkt[1] as u16 & 0x1F) << 8) | pkt[2] as u16
}

pub fn pkt_pts_dts(packet: &[u8], check_pid: u16) -> Option<(u64, u64)> {
    if packet.len() < TS_PACKET_SIZE || packet[0] != TS_SYNC_BYTE {
        return None;
    }

    let pid = ((packet[1] as u16 & 0x1F) << 8) | packet[2] as u16;
    if pid != check_pid {
        return None;
    }

    // PUSI
    if (packet[1] & 0x40) == 0 {
        return None;
    }

    // Start of Payload (PES-header)
    let af_control = (packet[3] >> 4) & 0x03;
    let mut offset = 4;

    match af_control {
        0 => return None, // Reserved
        1 => { /* only Payload, offset = 4, do nothing */ }
        2 => return None, // only Adaptation Field (PES no exist, offset = 4)
        3 => {
            // AF + Payload
            let af_length = packet[4] as usize;
            // offset = 4 (header) + 1 (af_length byte) + AF length
            offset += 1 + af_length;
        }
        _ => unreachable!(),
    }

    if offset + 14 > TS_PACKET_SIZE {
        return None;
    }

    // Search PES prefix: [0x00, 0x00, 0x01]
    if packet[offset..offset + 3] != [0x00, 0x00, 0x01] {
        return None;
    }

    // Allowed audio (0xC0-0xDF), video (0xE0-0xEF) and Private Stream (0xBD)
    let stream_id = packet[offset + 3];
    if !((0xC0..=0xEF).contains(&stream_id) || stream_id == 0xBD) {
        return None;
    }

    // Flags: check PTS (bit 7 in byte offset + 7)
    // PES: [Start Code 3b][ID 1b][Len 2b][Flags 2b][HdrLen 1b]...
    let pts_dts_flags = (packet[offset + 7] & 0xC0) >> 6;
    if pts_dts_flags < 2 {
        // 2 = only PTS, 3 = PTS & DTS
        return None;
    }

    let off = offset + 9; // start PTS data
    if off + 4 >= 188 {
        return None;
    }

    // PTS
    let pts: u64 = ((packet[off] as u64 & 0x0E) << 29)
        | ((packet[off + 1] as u64) << 22)
        | ((packet[off + 2] as u64 & 0xFE) << 14)
        | ((packet[off + 3] as u64) << 7)
        | ((packet[off + 4] as u64 & 0xFE) >> 1);

    // DTS (flags == 3)
    let dts = if pts_dts_flags == 3 {
        let off_dts = offset + 14;
        if off_dts + 4 >= TS_PACKET_SIZE {
            pts
        } else {
            ((packet[off_dts] as u64 & 0x0E) << 29)
                | ((packet[off_dts + 1] as u64) << 22)
                | ((packet[off_dts + 2] as u64 & 0xFE) << 14)
                | ((packet[off_dts + 3] as u64) << 7)
                | ((packet[off_dts + 4] as u64 & 0xFE) >> 1)
        }
    } else {
        pts // DTS = PTS
    };

    Some((pts, dts))
}

pub fn pkt_extract_pcr(packet: &[u8]) -> Option<u64>{
    let af_control = (packet[3] >> 4) & 0x03;
    if af_control < 2 {
        return None;
    }

    let af_len = packet[4] as usize;
    if af_len < 7 {
        return None;
    }

    let pcr_flag = (packet[5] & 0x10) != 0;
    if !pcr_flag {
        return None;
    }
    let mut base: u64 = 0;
    base |= (packet[6] as u64) << 25;
    base |= (packet[7] as u64) << 17;
    base |= (packet[8] as u64) << 9;
    base |= (packet[9] as u64) << 1;
    base |= (packet[10] as u64) >> 7;

    let ext = (((packet[10] as u64) & 0x01) << 8) | (packet[11] as u64);

    Some(base * 300 + ext)
}

pub fn pkt_write_pcr(packet: &mut [u8], pcr: u64){
    let base = pcr / 300;
    let ext = pcr % 300;

    packet[6] = (base >> 25) as u8;
    packet[7] = (base >> 17) as u8;
    packet[8] = (base >> 9) as u8;
    packet[9] = (base >> 1) as u8;
    packet[10] = ((base << 7) as u8 & 0x80) | 0x7E | ((ext >> 8) as u8 & 0x01);
    packet[11] = (ext & 0xFF) as u8;
}

pub fn pkt_is_pcr_present(packet: &[u8]) -> bool {
    let af_control = (packet[3] >> 4) & 0x03;

    if af_control < 2 { return false; }
    
    let af_len = packet[4];
    let pcr_flag = (packet[5] & 0x10) != 0;

    pcr_flag && af_len >= 7
}

pub fn pkt_has_payload(packet: &[u8]) -> bool {
    let afc = (packet[3] >> 4) & 0x03;

    match afc {
        0 => {
            // Reserved/Invalid
            false
        }
        1 | 3 => true, // Payload
        2 => false,    // only Adaptation Field
        _ => unreachable!(),
    }
}

pub fn pkt_get_discontinuity(packet: &[u8]) -> bool {
    if packet.len() < 6 {
        return false;
    }

    let af_control = (packet[3] >> 4) & 0x03;
    if af_control < 2 {
        return false;
    }

    let af_len = packet[4] as usize;
    if af_len < 1 {
        return false;
    }

    (packet[5] & 0x80) != 0
}

pub fn pkt_set_discontinuity(packet: &mut [u8]) -> bool {
    if packet.len() < 6 {
        return false;
    }

    let af_control = (packet[3] >> 4) & 0x03;
    if af_control < 2 {
        return false;
    }

    let af_len = packet[4] as usize;
    if af_len < 1 {
        return false;
    }

    packet[5] |= 0x80;
    true
}

// pub fn pkt_counter(packet: &[u8]) -> u8 {
//     packet[3] & 0x0F
// }

pub fn pkt_next(packet: &mut [u8]) {
    let current_byte = packet[3];
    let current_counter = current_byte & 0x0F;
    let next_counter = (current_counter + 1) & 0x0F;
    packet[3] = (current_byte & 0xF0) | next_counter;
}
