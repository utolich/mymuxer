use crate::psi;
use encoding_rs::{ISO_8859_5, UTF_16BE};
use serde::Serialize;

#[derive(Debug, Default, Clone, Serialize)]
pub struct SdtInfo {
    pub service_id: u16,
    pub service_type: u8,
    pub provider_name: String,
    pub service_name: String,
    pub running_status: u8,
}

pub fn analyze_sdt(section: &[u8]) -> Vec<SdtInfo> {
    if section.len() < 11 || section[0] != 0x42 {
        return Vec::new();
    }

    let mut services = Vec::new();
    let mut pos = 11;
    let section_limit = section.len() - 4;

    while pos + 5 <= section_limit {
        let sid = u16::from_be_bytes([section[pos], section[pos + 1]]);
        let running_status = (section[pos + 2] >> 5) & 0x07;
        let descriptors_loop_len =
            ((section[pos + 3] as usize & 0x0F) << 8) | section[pos + 4] as usize;
        pos += 5;

        let inner_limit = pos + descriptors_loop_len;

        while pos + 2 <= inner_limit {
            let tag = section[pos];
            let len = section[pos + 1] as usize;
            pos += 2;

            if tag == 0x48 && pos + len <= inner_limit {
                let mut info = SdtInfo::default();
                info.service_id = sid;
                info.running_status = running_status;

                info.service_type = section[pos];

                let prov_len = section[pos + 1] as usize;
                if pos + 2 + prov_len <= pos + len {
                    info.provider_name = parse_dvb_string(&section[pos + 2..pos + 2 + prov_len]);

                    let serv_len_pos = pos + 2 + prov_len;
                    if serv_len_pos + 1 <= pos + len {
                        let serv_len = section[serv_len_pos] as usize;
                        if serv_len_pos + 1 + serv_len <= pos + len {
                            info.service_name = parse_dvb_string(
                                &section[serv_len_pos + 1..serv_len_pos + 1 + serv_len],
                            );
                        }
                    }
                }
                services.push(info);
            }
            pos += len;
        }
    }
    services
}
fn parse_dvb_string(bytes: &[u8]) -> String {
    if bytes.is_empty() {
        return String::new();
    }

    match bytes[0] {
        0x01 => ISO_8859_5.decode(&bytes[1..]).0.into_owned(),

        0x15 => String::from_utf8_lossy(&bytes[1..]).into_owned(),

        0x11 => {
            let data = &bytes[1..];
            if data.len() % 2 == 0 {
                UTF_16BE.decode(data).0.into_owned()
            } else {
                String::from_utf8_lossy(data).into_owned()
            }
        }

        b if b >= 0x20 => decode_iso_6937(bytes),

        _ => {
            if bytes.len() > 1 {
                String::from_utf8_lossy(&bytes[1..]).into_owned()
            } else {
                String::new()
            }
        }
    }
}

fn decode_iso_6937(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn encode_dvb_string(text: &str) -> Vec<u8> {
    let mut v = Vec::new();
    v.push(0x15); // For UTF-8
    v.extend_from_slice(text.as_bytes());
    v
}

pub fn patch_sdt_section(
    original_section: &[u8],
    new_service_id: u16,
    service_type: u8,
    provider_name: &str,
    service_name: &str,
) -> Vec<u8> {
    let mut new_section = Vec::with_capacity(original_section.len() + 64);

    new_section.extend_from_slice(&original_section[0..11]);

    let mut service_descriptor = Vec::new();
    service_descriptor.push(0x48); // Tag

    let prov_bytes = encode_dvb_string(provider_name);
    let serv_bytes = encode_dvb_string(service_name);

    // 1 (type) + 1 (prov_len) + prov + 1 (serv_len) + serv
    service_descriptor.push((3 + prov_bytes.len() + serv_bytes.len()) as u8);
    service_descriptor.push(service_type);

    service_descriptor.push(prov_bytes.len() as u8);
    service_descriptor.extend_from_slice(prov_bytes.as_slice());

    service_descriptor.push(serv_bytes.len() as u8);
    service_descriptor.extend_from_slice(serv_bytes.as_slice());

    new_section.push((new_service_id >> 8) as u8);
    new_section.push((new_service_id & 0xFF) as u8);

    let service_flags = (4 << 5) | (0 << 4) | 0x01;
    new_section.push(service_flags);

    let desc_len = service_descriptor.len() as u16;
    new_section.push(((desc_len >> 8) as u8) | 0xF0);
    new_section.push(desc_len as u8);
    new_section.extend(service_descriptor);

    let section_length = (new_section.len() - 3 + 4) as u16;
    new_section[1] = (new_section[1] & 0xF0) | ((section_length >> 8) as u8 & 0x0F);
    new_section[2] = (section_length & 0xFF) as u8;

    let crc = psi::calculate_mpeg2_crc32(&new_section);
    new_section.extend_from_slice(&crc.to_be_bytes());

    new_section
}

pub fn prepare_sdt(
    transport_stream_id: u16,
    program_id: u16,
    service_type: u8,
    provider_name: &str,
    service_name: &str,
) -> [u8; 188] {
    let mut dummy_section = [
        0x42, // table_id: 0x42 (SDT Actual TS)
        0xF0, // section_syntax_indicator (1) + reserved/flags
        0x00, // length low
        0x00, // transport_stream_id high
        0x00, // transport_stream_id low
        0xC1, // reserved (11) + version (00000) + current_next_indicator (1)
        0x00, // section_number = 0
        0x00, // last_section_number = 0
        0x00, // original_network_id high (0x0001)
        0x01, // original_network_id low
        0xFF, // reserved byte
    ];
    dummy_section[3] = (transport_stream_id >> 8) as u8;
    dummy_section[4] = (transport_stream_id & 0xFF) as u8;
    let section = patch_sdt_section(
        &dummy_section,
        program_id,
        service_type,
        provider_name,
        service_name,
    );

    psi::pack(&section, 0x0011, 0)
}
