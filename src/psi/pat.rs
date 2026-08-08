use crate::psi;
use serde::Serialize;

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct PatInfo {
    pub prog_num: u16,
    pub pmt_pid: u16,
}

pub fn analyze_pat(section: &[u8]) -> Vec<PatInfo> {
    let mut programs = Vec::new();
    let mut pos = 8;
    let section_end = section.len() - 4; // CRC 4 bytes

    while pos + 4 <= section_end {
        let prog_num = ((section[pos] as u16) << 8) | section[pos + 1] as u16;
        let pmt_pid = ((section[pos + 2] as u16 & 0x1F) << 8) | section[pos + 3] as u16;

        if prog_num != 0 {
            programs.push(PatInfo { prog_num, pmt_pid });
        }
        pos += 4;
    }
    programs
}

// (new_program_number, new_pmt_pid)
pub fn patch_pat_section(original_section: &[u8], mappings: &[(u16, u16)]) -> Vec<u8> {
    let mut new_section = Vec::with_capacity(original_section.len());

    // Copy first 8 bytes of header (table_id, syntax, length, transport_stream_id, etc.)
    new_section.extend_from_slice(&original_section[0..8]);

    for &(prog_num, pmt_pid) in mappings {
        // Program Number (16 bits)
        new_section.push((prog_num >> 8) as u8);
        new_section.push((prog_num & 0xFF) as u8);

        // Reserved (3 bits '111') + PMT PID (13 bits)
        new_section.push(((pmt_pid >> 8) as u8 & 0x1F) | 0xE0);
        new_section.push((pmt_pid & 0xFF) as u8);
    }

    let section_length = (new_section.len() - 3 + 4) as u16;
    new_section[1] = (new_section[1] & 0xF0) | ((section_length >> 8) as u8 & 0x0F);
    new_section[2] = (section_length & 0xFF) as u8;

    let crc = psi::calculate_mpeg2_crc32(&new_section);
    new_section.extend_from_slice(&crc.to_be_bytes());

    new_section
}

pub fn is_pat_changed(a: &[PatInfo], b: &[PatInfo]) -> bool {
    if a.len() != b.len() {
        return true;
    }

    if a == b {
        return false;
    }

    let mut cur_sorted = a.to_vec();
    let mut new_sorted = b.to_vec();

    cur_sorted.sort_unstable_by_key(|p| p.prog_num);
    new_sorted.sort_unstable_by_key(|p| p.prog_num);

    cur_sorted != new_sorted
}
