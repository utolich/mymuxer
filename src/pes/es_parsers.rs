use super::es::{Es, Es2, Es3, Es4, Es15, Es27, EsUnknown};
use super::golomb::BitReader;

pub trait EsParser {
    fn parse(&self, data: &[u8]) -> Option<Es>;
}

pub struct EsParserUnknown {
    stream_type: u8,
}

impl EsParserUnknown {
    pub fn new() -> Self {
        Self { stream_type: 0 }
    }
}

impl EsParser for EsParserUnknown {
    fn parse(&self, _data: &[u8]) -> Option<Es> {
        Some(Es::Unknown(EsUnknown::new()))
    }
}

pub struct EsParser2 {
    stream_type: u8,
}

impl EsParser2 {
    pub fn new() -> Self {
        Self { stream_type: 2 }
    }

    fn find_data(&self, data: &[u8]) -> Option<Vec<u8>> {
        for i in 0..data.len().saturating_sub(8) {
            if data[i] == 0x00 && data[i + 1] == 0x00 && data[i + 2] == 0x01 && data[i + 3] == 0xB3
            {
                let data = &data[i..];
                if data.len() >= 8 {
                    return Some(data.to_vec());
                }
            }
        }

        None
    }
}

impl EsParser for EsParser2 {
    fn parse(&self, data: &[u8]) -> Option<Es> {
        let Some(data) = self.find_data(data) else {
            return None;
        };
        if let Some(code) = read_u32_be(&*data, 0) {
            if code != 0x000001B3 {
                return None;
            }
        }
        let mut es = Es2::new();
        if let Some(tmp) = read_u32_be(&*data, 4) {
            es.horizontal_size = Some((tmp >> 20) as u16);
            es.vertical_size = Some(((tmp >> 8) & 0x0FFF) as u16);
            es.aspect_ratio = Some(((tmp >> 4) & 0x0F) as u8);
            es.frame_rate = Some((tmp & 0x0F) as u8);
        }
        if let Some(tmp) = read_u32_be(&*data, 8) {
            es.bitrate = Some((tmp >> 14) as u32);
        }
        Some(Es::Es2(es))
    }
}

pub struct EsParser3 {
    stream_type: u8,
}

impl EsParser3 {
    pub fn new() -> Self {
        Self { stream_type: 3 }
    }
}

impl EsParser for EsParser3 {
    fn parse(&self, data: &[u8]) -> Option<Es> {
        let header = find_mpeg_audio_header(data);
        let Some(header) = header else {
            return None;
        };
        let mut es = Es3::new();
        es.id = Some(((header >> 19) & 0x1) as u8);
        es.layer = Some(((header >> 17) & 0x3) as u8);
        es.bit_rate_index = Some(((header >> 12) & 0xF) as u8);
        es.sampling_frequency = Some(((header >> 10) & 0x3) as u8);
        es.mode = Some(((header >> 6) & 0x3) as u8);
        es.mode_extension = Some(((header >> 4) & 0x3) as u8);
        es.copyright = Some(((header >> 3) & 0x1) as u8);
        es.original = Some(((header >> 2) & 0x1) as u8);
        es.emphasis = Some((header & 0x3) as u8);
        Some(Es::Es3(es))
    }
}

pub struct EsParser4 {
    stream_type: u8,
}

impl EsParser4 {
    pub fn new() -> Self {
        Self { stream_type: 4 }
    }
}

impl EsParser for EsParser4 {
    fn parse(&self, data: &[u8]) -> Option<Es> {
        let header = find_mpeg_audio_header(data);
        let Some(header) = header else {
            return None;
        };
        let mut es = Es4::new();
        es.id = Some(((header >> 19) & 0x1) as u8);
        es.layer = Some(((header >> 17) & 0x3) as u8);
        es.bit_rate_index = Some(((header >> 12) & 0xF) as u8);
        es.sampling_frequency = Some(((header >> 10) & 0x3) as u8);
        es.mode = Some(((header >> 6) & 0x3) as u8);
        es.mode_extension = Some(((header >> 4) & 0x3) as u8);
        es.copyright = Some(((header >> 3) & 0x1) as u8);
        es.original = Some(((header >> 2) & 0x1) as u8);
        es.emphasis = Some((header & 0x3) as u8);
        Some(Es::Es4(es))
    }
}

pub struct EsParser15 {
    stream_type: u8,
}

impl EsParser15 {
    pub fn new() -> Self {
        Self { stream_type: 15 }
    }
}

impl EsParser for EsParser15 {
    fn parse(&self, data: &[u8]) -> Option<Es> {
        let header = find_mpeg_audio_header(data);
        let Some(header) = header else {
            return None;
        };
        let mut es = Es15::new();
        es.id = Some(((header >> 19) & 0x1) as u8);
        es.profile = Some(((header >> 16) & 0x3) as u8);
        es.sampling_frequency = Some(((header >> 12) & 0xF) as u8);
        es.channel_configuration = Some(((header >> 9) & 0x7) as u8);
        es.copyright = Some(((header >> 8) & 0x1) as u8);
        es.original = Some(((header >> 7) & 0x1) as u8);
        Some(Es::Es15(es))
    }
}

pub struct EsParser27 {
    stream_type: u8,
}

impl EsParser27 {
    pub fn new() -> Self {
        Self { stream_type: 27 }
    }

    fn find_sps(&self, data: &[u8]) -> Option<Vec<u8>> {
        let mut i = 0;
        let len = data.len();

        while i < len - 2 {
            // 1. Found start code  00 00 01
            if data[i] == 0 && data[i + 1] == 0 && data[i + 2] == 1 {
                let data_start = i + 3;

                // 2. Found end of NAL (next start code)
                let mut next_start = len;
                for j in data_start..(len - 2) {
                    if data[j] == 0 && data[j + 1] == 0 {
                        if data[j + 2] == 1 {
                            next_start = j;
                            break;
                        }
                        // check for 00 00 00 01
                        if j + 3 < len && data[j + 2] == 0 && data[j + 3] == 1 {
                            next_start = j;
                            break;
                        }
                    }
                }

                // 3. NAL type
                if data_start < len {
                    let header_byte = data[data_start];
                    let nal_type = header_byte & 0x1F;

                    let mut data_end = next_start;
                    while data_end > data_start && data[data_end - 1] == 0 {
                        data_end -= 1;
                    }

                    if nal_type == 7 {
                        return Some(data[data_start + 1..data_end].to_vec());
                    };
                }

                i = next_start; // next NAL
                continue;
            }
            i += 1;
        }
        None
    }

    fn ebsp_to_rbsp(&self, data: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(data.len());
        let mut zero_count = 0;

        for &b in data {
            if zero_count == 2 && b == 0x03 {
                zero_count = 0;
                continue;
            }

            out.push(b);

            if b == 0 {
                zero_count += 1;
            } else {
                zero_count = 0;
            }
        }
        out
    }
}

impl EsParser for EsParser27 {
    fn parse(&self, data: &[u8]) -> Option<Es> {
        let Some(ebsp) = self.find_sps(data) else {
            return None;
        };

        let mut es = Es27::new();
        let rbsp = self.ebsp_to_rbsp(&*ebsp);
        if rbsp.len() < 3 {
            return Some(Es::Es27(es));
        }

        let mut br = BitReader::new(&rbsp);

        let profile_idc = br.read_bits(8) as u8;
        br.read_bits(8); // constraint flags + reserved
        let level_idc = br.read_bits(8) as u8;

        let seq_parameter_set_id = br.read_ue();

        es.profile_idc = Some(profile_idc);
        es.level_idc = Some(level_idc);
        es.seq_parameter_set_id = Some(seq_parameter_set_id);

        let mut chroma_format_idc = 1;
        let mut chroma_array_type = chroma_format_idc;
        let mut sub_width_c = 2;
        let mut sub_height_c = 2;

        if matches!(profile_idc, 100 | 110 | 122 | 244 | 44) {
            chroma_format_idc = br.read_ue();
            (sub_width_c, sub_height_c) = match chroma_format_idc {
                0 => (0, 0),
                1 => (2, 2),
                2 => (2, 1),
                3 => (1, 1),
                _ => (0, 0),
            };
            if chroma_format_idc == 3 {
                let separate_colour_plane_flag = br.read_bit();
                if separate_colour_plane_flag == 1 {
                    sub_width_c = 0;
                    sub_height_c = 0;
                } else {
                    chroma_array_type = 0;
                }
            }
            let _bit_depth_luma_minus8 = br.read_ue();
            let _bit_depth_chroma_minus8 = br.read_ue();
            let _qpprime_y_zero_transform_bypass_flag = br.read_bit();
            let seq_scaling_matrix_present_flag = br.read_bit();

            if seq_scaling_matrix_present_flag == 1 {
                let count = if chroma_format_idc != 3 { 8 } else { 12 };
                for i in 0..count {
                    let seq_scaling_list_present_flag = br.read_bit();
                    if seq_scaling_list_present_flag == 1 {
                        let mut c = 64;
                        if i < 6 {
                            c = 16;
                        }
                        let mut last_scale = 8;
                        let mut next_scale = 8;
                        for _ in 0..c {
                            if next_scale != 0 {
                                let delta_scale = br.read_se();
                                next_scale = (last_scale + delta_scale + 256) % 256
                            }
                            last_scale = if next_scale == 0 {
                                last_scale
                            } else {
                                next_scale
                            };
                        }
                    }
                }
            }
        }

        let _log2_max_frame_num_minus4 = br.read_ue();
        let pic_order_cnt_type = br.read_ue();
        if pic_order_cnt_type == 0 {
            let _log2_max_pic_order_cnt_lsb_minus4 = br.read_ue();
        } else if pic_order_cnt_type == 1 {
            let _delta_pic_order_always_zero_flag = br.read_bit();
            let _offset_for_non_ref_pic = br.read_se();
            let _offset_for_top_to_bottom_field = br.read_se();
            let num_ref_frames_in_pic_order_cnt_cycle = br.read_ue();
            for _ in 0..num_ref_frames_in_pic_order_cnt_cycle {
                br.read_se();
            }
        }
        let _max_num_ref_frames = br.read_ue();
        let _gaps_in_frame_num_value_allowed_flag = br.read_bit();
        let pic_width_in_mbs_minus1 = br.read_ue();
        let pic_height_in_map_units_minus1 = br.read_ue();
        let frame_mbs_only_flag = br.read_bit();
        if frame_mbs_only_flag == 0 {
            let _mb_adaptive_frame_field_flag = br.read_bit();
        }
        let _direct_8x8_inference_flag = br.read_bit();
        let frame_cropping_flag = br.read_bit();
        let _frame_crop_left_offset = if frame_cropping_flag == 1 {
            br.read_ue()
        } else {
            0
        };
        let frame_crop_right_offset = if frame_cropping_flag == 1 {
            br.read_ue()
        } else {
            0
        };
        let _frame_crop_top_offset = if frame_cropping_flag == 1 {
            br.read_ue()
        } else {
            0
        };
        let frame_crop_bottom_offset = if frame_cropping_flag == 1 {
            br.read_ue()
        } else {
            0
        };
        let vui_parameters_present_flag = br.read_bit();
        if vui_parameters_present_flag == 1 {
            let aspect_ratio_info_present_flag = br.read_bit();
            let aspect_ratio_idc = if aspect_ratio_info_present_flag == 1 {
                br.read_bits(8)
            } else {
                0
            };
            if aspect_ratio_idc == 255 {
                let _sar_width = br.read_bits(16);
                let _sar_height = br.read_bits(16);
            }
            let overscan_info_present_flag = br.read_bit();
            let _overscan_appropriate_flag = if overscan_info_present_flag == 1 {
                br.read_bit()
            } else {
                0
            };
            let video_signal_type_present_flag = br.read_bit();
            if video_signal_type_present_flag == 1 {
                let _video_format = br.read_bits(3);
                let _video_full_range_flag = br.read_bit();
                let colour_description_present_flag = br.read_bit();
                if colour_description_present_flag == 1 {
                    let _colour_primaries = br.read_bits(8);
                    let _transfer_characteristics = br.read_bits(8);
                    let _matrix_coefficients = br.read_bits(8);
                }
            }
            let chroma_loc_info_present_flag = br.read_bit();
            if chroma_loc_info_present_flag == 1 {
                let _chroma_sample_loc_type_top_field = br.read_ue();
                let _chroma_sample_loc_type_bottom_field = br.read_ue();
            }
            let timing_info_present_flag = br.read_bit();
            if timing_info_present_flag == 1 {
                let num_units_in_tick = br.read_bits(32);
                let time_scale = br.read_bits(32);
                let _fixed_frame_rate_flag = br.read_bit();
                //
                if num_units_in_tick != 0 && time_scale != 0 {
                    let fr = time_scale as f64 / (2.0 * num_units_in_tick as f64);
                    es.frame_rate = Some(fr.round() as u32);
                }
            }
        }

        //
        let (crop_unit_x, crop_unit_y) = if chroma_array_type == 0 {
            (1, 2 - frame_mbs_only_flag)
        } else {
            (sub_width_c, sub_height_c * (2 - frame_mbs_only_flag))
        };
        let width = (pic_width_in_mbs_minus1 + 1) * 16 - crop_unit_x * frame_crop_right_offset;
        let height = (2 - frame_mbs_only_flag as u32) * (pic_height_in_map_units_minus1 + 1) * 16
            - crop_unit_y as u32 * frame_crop_bottom_offset;
        if width > 0 {
            es.horizontal_size = Some(width);
        }
        if height > 0 {
            es.vertical_size = Some(height);
        }

        Some(Es::Es27(es))
    }
}

fn find_mpeg_audio_header(data: &[u8]) -> Option<u32> {
    let mut es_pointer = 0usize;
    while es_pointer < data.len() {
        let Some(pos) = find_byte(data, 0xFF, es_pointer) else {
            break;
        };
        if pos + 4 > data.len() {
            break;
        }
        if let Some(tmp) = read_u32_be(data, pos) {
            if (tmp >> 20) == 0xFFF {
                return Some(tmp);
            }
        }
        es_pointer = pos + 2;
    }
    None
}

fn find_byte(data: &[u8], value: u8, start: usize) -> Option<usize> {
    data.get(start..)?
        .iter()
        .position(|b| *b == value)
        .map(|i| i + start)
}

fn read_u32_be(data: &[u8], offset: usize) -> Option<u32> {
    match data.get(offset..offset + 4) {
        Some(bytes) => Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])),
        None => None,
    }
}
