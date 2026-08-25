use crate::psi;
use serde::Serialize;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct StreamInfo {
    pub stream_type: u8,
    pub pid: u16,
    pub descriptors: HashMap<String, String>,
    pub description: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct PmtInfo {
    pub pcr_pid: u16,
    pub streams: Vec<StreamInfo>,
}

pub fn analyze_pmt(section: &[u8]) -> PmtInfo {
    // if section.len() < 16 { return None; }

    let pcr_pid = ((section[8] as u16 & 0x1F) << 8) | section[9] as u16;
    let program_info_len = (((section[10] as u16 & 0x0F) << 8) | section[11] as u16) as usize;

    let mut streams = Vec::new();
    let mut pos = 12 + program_info_len;
    let section_end = section.len().saturating_sub(4); // Залишаємо місце під CRC

    while pos + 5 <= section_end {
        let stype = section[pos];
        let pid = ((section[pos + 1] as u16 & 0x1F) << 8) | section[pos + 2] as u16;
        let es_info_len =
            (((section[pos + 3] as u16 & 0x0F) << 8) | section[pos + 4] as u16) as usize;

        let descriptors_start = pos + 5;
        let descriptors_end = descriptors_start + es_info_len;

        let descriptors = pasre_descriptors(&section[descriptors_start..descriptors_end]);
        streams.push(StreamInfo {
            stream_type: stype,
            pid,
            descriptors,
            description: String::new(),
        });

        pos = descriptors_end;
    }

    PmtInfo { pcr_pid, streams }
}

fn pasre_descriptors(data: &[u8]) -> HashMap<String, String> {
    let mut descriptors = HashMap::new();

    let mut i = 0;
    while i + 1 < data.len() {
        let tag = data[i];
        let len = data[i + 1] as usize;
        let start = i + 2;
        let end = start + len;

        if end > data.len() {
            break;
        }

        match &tag {
            0x0A if len >= 3 => {
                descriptors.insert(
                    "lang".to_string(),
                    String::from_utf8_lossy(&data[start..start + 3]).to_string(),
                );
            }
            // 0x6A - на AC-3, 0x59 — subtitles
            _ => {}
        };
        i = end;
    }

    descriptors
}

pub fn patch_pmt_section(
    original_section: &[u8],
    new_pcr_pid: u16,
    es_mappings: &[(u16, u16)],
    strip_ca_descriptors: bool,
) -> Vec<u8> {
    let mut new_section = Vec::with_capacity(original_section.len());

    // Copy header (first 8 bytes: table_id...program_number...version)
    new_section.extend_from_slice(&original_section[0..8]);

    // New PCR PID (2 bytes)
    new_section.push(((new_pcr_pid >> 8) as u8 & 0x1F) | 0xE0);
    new_section.push((new_pcr_pid & 0xFF) as u8);

    // Program Info Length (Descriptors for all PMTs are the same, so we can use the length from the first PMT)
    let prog_info_len =
        (((original_section[10] as u16 & 0x0F) << 8) | original_section[11] as u16) as usize;
    let program_descriptors = &original_section[12..12 + prog_info_len];
    let program_descriptors = patch_descriptors(program_descriptors, strip_ca_descriptors);
    let new_prog_info_len = program_descriptors.len();
    new_section.push(((new_prog_info_len >> 8) as u8 & 0x0F) | 0xF0);
    new_section.push((new_prog_info_len & 0xFF) as u8);

    // Copy Program Info Descriptor
    new_section.extend_from_slice(&program_descriptors);

    // ES (Elementary Streams)
    let mut pos = 12 + prog_info_len;
    let section_end = original_section.len() - 4; // без CRC

    while pos + 5 <= section_end {
        let stream_type = original_section[pos];
        let old_es_pid =
            ((original_section[pos + 1] as u16 & 0x1F) << 8) | original_section[pos + 2] as u16;
        let es_info_len = (((original_section[pos + 3] as u16 & 0x0F) << 8)
            | original_section[pos + 4] as u16) as usize;

        // Is PID allowed
        if let Some(&(_, new_es_pid)) = es_mappings.iter().find(|(old, _)| *old == old_es_pid) {
            let descriptors_start = pos + 5;
            let descriptors_end = descriptors_start + es_info_len;
            let es_descriptors = patch_descriptors(
                &original_section[descriptors_start..descriptors_end],
                strip_ca_descriptors,
            );
            let new_es_info_len = es_descriptors.len();

            new_section.push(stream_type);
            new_section.push(((new_es_pid >> 8) as u8 & 0x1F) | 0xE0);
            new_section.push((new_es_pid & 0xFF) as u8);
            new_section.push(((new_es_info_len >> 8) as u8 & 0x0F) | 0xF0);
            new_section.push((new_es_info_len & 0xFF) as u8);
            new_section.extend_from_slice(&es_descriptors);
        }
        pos += 5 + es_info_len;
    }

    let section_length = (new_section.len() - 3 + 4) as u16;
    new_section[1] = (new_section[1] & 0xF0) | ((section_length >> 8) as u8 & 0x0F);
    new_section[2] = (section_length & 0xFF) as u8;

    let crc = psi::calculate_mpeg2_crc32(&new_section);
    new_section.extend_from_slice(&crc.to_be_bytes());

    new_section
}

fn patch_descriptors(descriptors: &[u8], strip_ca_descriptors: bool) -> Vec<u8> {
    if !strip_ca_descriptors {
        return descriptors.to_vec();
    }

    let mut patched = Vec::with_capacity(descriptors.len());
    let mut pos = 0;
    while pos + 2 <= descriptors.len() {
        let tag = descriptors[pos];
        let len = descriptors[pos + 1] as usize;
        let end = pos + 2 + len;
        if end > descriptors.len() {
            break;
        }

        if tag != 0x09 {
            patched.extend_from_slice(&descriptors[pos..end]);
        }
        pos = end;
    }

    patched
}

pub fn is_pmt_changed(a: &PmtInfo, b: &PmtInfo) -> bool {
    #[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
    struct StreamInfoDiff {
        stream_type: u8,
        pid: u16,
    }

    if a.pcr_pid != b.pcr_pid || a.streams.len() != b.streams.len() {
        return true;
    }

    let mut a_streams:Vec<StreamInfoDiff> = a.streams.iter().map(|s| StreamInfoDiff { stream_type: s.stream_type, pid: s.pid }).collect();
    let mut b_streams:Vec<StreamInfoDiff> = b.streams.iter().map(|s| StreamInfoDiff { stream_type: s.stream_type, pid: s.pid }).collect();

    if a_streams == b_streams {
        return false;
    }

    a_streams.sort_unstable();
    b_streams.sort_unstable();

    a_streams != b_streams
}
