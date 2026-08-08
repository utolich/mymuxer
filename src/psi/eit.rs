use crate::psi::{
    Section, dvb_text, encode_duration, encode_dvb_time, high, low, section_with_crc,
    set_section_length,
};
use chrono::{Duration, Utc};

pub const PID: u16 = 0x0012;

const MAX_DESCRIPTOR_PAYLOAD: usize = 255;
const EXTENDED_HEADER_SIZE: usize = 6;
const MAX_TEXT_BYTES_PER_DESC: usize = MAX_DESC_PAYLOAD - HEADER_SIZE - 1;
const MAX_TEXT_PER_DESC: usize = MAX_DESC_PAYLOAD - HEADER_SIZE - 1;
const MAX_DESC_PAYLOAD: usize = 255;
const HEADER_SIZE: usize = 6;
const MAX_TEXT_CHUNK: usize = 248;
const DVB_UTF8_PREFIX: u8 = 0x15;

pub fn build_extended_event_descriptors(
    lang: &[u8; 3],
    text: &str,
) -> Vec<Vec<u8>> {
    if text.is_empty() {
        return Vec::new();
    }

    let mut chunks = Vec::new();
    let mut remaining = text;

    while !remaining.is_empty() {
        if remaining.len() <= MAX_TEXT_CHUNK {
            chunks.push(remaining);
            break;
        }

        let safe_len = remaining.floor_char_boundary(MAX_TEXT_CHUNK);
        if safe_len == 0 {
            break;
        }

        chunks.push(&remaining[..safe_len]);
        remaining = &remaining[safe_len..];
    }

    let total_chunks = chunks.len().min(16);
    let last_desc_num = (total_chunks - 1) as u8;
    let mut descriptors = Vec::with_capacity(total_chunks);

    for (i, &chunk_str) in chunks.iter().take(total_chunks).enumerate() {
        let chunk_bytes = chunk_str.as_bytes();
        let text_len = 1 + chunk_bytes.len(); // 1 byte 0x15 + text
        let payload_len = 6 + text_len;

        let mut desc = Vec::with_capacity(2 + payload_len);

        desc.push(0x4E);                                          // Tag
        desc.push(payload_len as u8);                                   // Length
        desc.push(((i as u8) << 4) | (last_desc_num & 0x0F));     // seq (descriptor_number | last_descriptor_number)
        desc.extend_from_slice(lang);                                   // Language
        desc.push(0x00);                                          // length_of_items
        desc.push(text_len as u8);                                      // text_length

        desc.push(DVB_UTF8_PREFIX);                                     // 0x15
        desc.extend_from_slice(chunk_bytes);

        descriptors.push(desc);
    }

    descriptors
}

fn split_text_into_dvb_chunks(mut text: &str, max_bytes: usize) -> Vec<&str> {
    let mut chunks = Vec::new();

    while !text.is_empty() {
        if text.len() <= max_bytes {
            chunks.push(text);
            break;
        }

        let safe_len = text.floor_char_boundary(max_bytes);
        if safe_len == 0 {
            break;
        }

        chunks.push(&text[..safe_len]);
        text = &text[safe_len..];
    }

    chunks
}

fn split_text_to_chunks(mut text: &str, max_bytes: usize) -> Vec<&str> {
    let mut chunks = Vec::new();

    while !text.is_empty() {
        if text.len() <= max_bytes {
            chunks.push(text);
            break;
        }
        let safe_len = text.floor_char_boundary(max_bytes);

        if safe_len == 0 {
            break;
        }

        chunks.push(&text[..safe_len]);
        text = &text[safe_len..];
    }

    chunks
}

fn parse_iso_639_2(lang: &str) -> [u8; 3] {
    let mut code = *b"eng";
    let bytes = lang.as_bytes();
    for (i, &b) in bytes.iter().take(3).enumerate() {
        if b.is_ascii_alphabetic() {
            code[i] = b.to_ascii_lowercase();
        }
    }
    code
}

pub fn present_following_section(
    service_id: u16,
    version: u8,
    transport_stream_id: u16,
    original_network_id: u16,
    section_number: u8,        // 0x00 = Present, 0x01 = Following
    event_id: u16,
    start: chrono::DateTime<Utc>,
    duration: Duration,
    title: &str,
    description: &str,
    language: &str,
) -> Section {
    let lang_code = parse_iso_639_2(language);

    let name_bytes = dvb_text(title, 80);
    let short_text_bytes = if description.len() > 160 {
        Vec::new()
    } else {
        dvb_text(description, 160)
    };

    let mut short_payload = Vec::with_capacity(3 + 1 + name_bytes.len() + 1 + short_text_bytes.len());
    short_payload.extend_from_slice(&lang_code);
    short_payload.push(name_bytes.len() as u8);
    short_payload.extend_from_slice(&name_bytes);
    short_payload.push(short_text_bytes.len() as u8);
    short_payload.extend_from_slice(&short_text_bytes);

    let mut descriptors_loop = Vec::new();

    if short_payload.len() <= 255 {
        descriptors_loop.push(0x4D);
        descriptors_loop.push(short_payload.len() as u8);
        descriptors_loop.extend_from_slice(&short_payload);
    }

    if description.len() > 160 {
        let ext_descriptors = build_extended_event_descriptors(&lang_code, description);
        for ext_desc in ext_descriptors {
            descriptors_loop.extend_from_slice(&ext_desc);
        }
    }

    let desc_loop_len = descriptors_loop.len() as u16;
    assert!(desc_loop_len <= 0x0FFF, "Descriptors loop length overflow!");

    let mut section = vec![
        0x4E,
        0xF0,
        0x00,
        high(service_id),
        low(service_id),
        0xC1 | ((version & 0x1F) << 1),
        section_number,
        0x01,
        high(transport_stream_id),
        low(transport_stream_id),
        high(original_network_id),
        low(original_network_id),
        0x01,
        0x4E,
        high(event_id),
        low(event_id),
    ];

    section.extend_from_slice(&encode_dvb_time(start));
    section.extend_from_slice(&encode_duration(duration.as_seconds_f32() as u32));

    section.push((4 << 5) | high(desc_loop_len));
    section.push(low(desc_loop_len));

    section.extend_from_slice(&descriptors_loop);

    set_section_length(&mut section);
    section_with_crc(PID, section)
}

pub fn empty_present_following_section(
    service_id: u16,
    version: u8,
    transport_stream_id: u16,
    original_network_id: u16,
    section_number: u8,
) -> Section {
    let mut section = vec![
        0x4E,
        0xF0,
        0x00,
        high(service_id),
        low(service_id),
        0xC1 | ((version & 0x1F) << 1),
        section_number,
        0x01,
        high(transport_stream_id),
        low(transport_stream_id),
        high(original_network_id),
        low(original_network_id),
        0x01,
        0x4E,
    ];
    set_section_length(&mut section);

    section_with_crc(PID, section)
}
