pub enum Es {
    Unknown(EsUnknown),
    Es2(Es2),
    Es3(Es3),
    Es4(Es4),
    Es15(Es15),
    Es27(Es27),
}

impl Es {
    pub fn description(&self) -> String {
        match self {
            Es::Unknown(e) => e.description(),
            Es::Es2(e) => e.description(),
            Es::Es3(e) => e.description(),
            Es::Es4(e) => e.description(),
            Es::Es15(e) => e.description(),
            Es::Es27(e) => e.description(),
        }
    }
}

pub struct EsUnknown {
    pub stream_type: u8,
}

impl EsUnknown {
    pub fn new() -> Self {
        Self { stream_type: 0 }
    }

    pub fn description(&self) -> String {
        format!("Unknown: {}", self.stream_type)
    }
}

pub struct Es2 {
    pub stream_type: u8,
    pub horizontal_size: Option<u16>,
    pub vertical_size: Option<u16>,
    pub aspect_ratio: Option<u8>,
    pub frame_rate: Option<u8>,
    pub bitrate: Option<u32>,
}

impl Es2 {
    pub fn new() -> Self {
        Self {
            stream_type: 2,
            horizontal_size: None,
            vertical_size: None,
            aspect_ratio: None,
            frame_rate: None,
            bitrate: None,
        }
    }

    fn aspect_ratio_str(&self) -> &str {
        match self.aspect_ratio {
            Some(0b0010) => "4:3",
            Some(0b0011) => "16:9",
            Some(0b0100) => "2.21:1",
            _ => "None",
        }
    }

    fn frame_rate_str(&self) -> &str {
        match self.frame_rate {
            Some(0b0011) => "25",
            Some(0b0110) => "50",
            Some(0b0001) => "24000/1001",
            Some(0b0010) => "24",
            Some(0b0100) => "30000/1001",
            Some(0b0101) => "30",
            Some(0b0111) => "60000/1001",
            Some(0b1000) => "60",
            _ => "None",
        }
    }
    pub fn description(&self) -> String {
        let h = self.horizontal_size.unwrap_or(0);
        let v = self.vertical_size.unwrap_or(0);
        let aspect = self.aspect_ratio_str();
        let fr = self.frame_rate_str();
        let bitrate = match self.bitrate {
            Some(0x3FFFF) => "VBR".to_string(),
            Some(b) => format!("{} kbit/s", (b * 400 + 999) / 1000),
            None => "-".to_string(),
        };
        format!(
            "Video: mpeg2video {}x{}, {}, {} fps, {}",
            h, v, aspect, fr, bitrate
        )
    }
}

pub struct Es3 {
    pub stream_type: u8,
    pub id: Option<u8>,
    pub layer: Option<u8>,
    pub bit_rate_index: Option<u8>,
    pub sampling_frequency: Option<u8>,
    pub mode: Option<u8>,
    pub mode_extension: Option<u8>,
    pub copyright: Option<u8>,
    pub original: Option<u8>,
    pub emphasis: Option<u8>,
}

impl Es3 {
    pub fn new() -> Self {
        Self {
            stream_type: 3,
            id: None,
            layer: None,
            bit_rate_index: None,
            sampling_frequency: None,
            mode: None,
            mode_extension: None,
            copyright: None,
            original: None,
            emphasis: None,
        }
    }

    fn layer_str_es3(&self) -> &str {
        match self.layer {
            Some(0b11) => "Layer I",
            Some(0b10) => "Layer II",
            Some(0b01) => "Layer III",
            _ => "None",
        }
    }

    fn mode_str_es3(&self) -> &str {
        match self.mode {
            Some(0b00) => "stereo",
            Some(0b01) => "joint_stereo (intensity_stereo and/or ms_stereo)",
            Some(0b10) => "dual_channel",
            Some(0b11) => "single_channel",
            _ => "None",
        }
    }

    fn sampling_freq_es3(&self) -> &str {
        match self.sampling_frequency {
            Some(0b00) => "44.1kHz",
            Some(0b01) => "48kHz",
            Some(0b10) => "32kHz",
            _ => "None",
        }
    }

    fn bit_rate_es3(&self, bit_rate_index: u8, layer: u8) -> Option<String> {
        let layer = match layer {
            0b11 => 0b11,
            0b10 => 0b10,
            0b01 => 0b01,
            _ => return None,
        };
        match bit_rate_index {
            0b0001 => Some(
                match layer {
                    0b11 => "32 kbit/s",
                    0b10 => "32 kbit/s",
                    _ => "32 kbit/s",
                }
                .into(),
            ),
            0b0010 => Some(
                match layer {
                    0b11 => "64 kbit/s",
                    0b10 => "48 kbit/s",
                    _ => "40 kbit/s",
                }
                .into(),
            ),
            0b0011 => Some(
                match layer {
                    0b11 => "96 kbit/s",
                    0b10 => "56 kbit/s",
                    _ => "48 kbit/s",
                }
                .into(),
            ),
            0b0100 => Some(
                match layer {
                    0b11 => "128 kbit/s",
                    0b10 => "64 kbit/s",
                    _ => "56 kbit/s",
                }
                .into(),
            ),
            0b0101 => Some(
                match layer {
                    0b11 => "160 kbit/s",
                    0b10 => "80 kbit/s",
                    _ => "64 kbit/s",
                }
                .into(),
            ),
            0b0110 => Some(
                match layer {
                    0b11 => "192 kbit/s",
                    0b10 => "96 kbit/s",
                    _ => "80 kbit/s",
                }
                .into(),
            ),
            0b0111 => Some(
                match layer {
                    0b11 => "224 kbit/s",
                    0b10 => "112 kbit/s",
                    _ => "96 kbit/s",
                }
                .into(),
            ),
            0b1000 => Some(
                match layer {
                    0b11 => "256 kbit/s",
                    0b10 => "128 kbit/s",
                    _ => "112 kbit/s",
                }
                .into(),
            ),
            0b1001 => Some(
                match layer {
                    0b11 => "288 kbit/s",
                    0b10 => "160 kbit/s",
                    _ => "128 kbit/s",
                }
                .into(),
            ),
            0b1010 => Some(
                match layer {
                    0b11 => "320 kbit/s",
                    0b10 => "192 kbit/s",
                    _ => "160 kbit/s",
                }
                .into(),
            ),
            0b1011 => Some(
                match layer {
                    0b11 => "352 kbit/s",
                    0b10 => "224 kbit/s",
                    _ => "192 kbit/s",
                }
                .into(),
            ),
            0b1100 => Some(
                match layer {
                    0b11 => "384 kbit/s",
                    0b10 => "256 kbit/s",
                    _ => "224 kbit/s",
                }
                .into(),
            ),
            0b1101 => Some(
                match layer {
                    0b11 => "416 kbit/s",
                    0b10 => "320 kbit/s",
                    _ => "256 kbit/s",
                }
                .into(),
            ),
            0b1110 => Some(
                match layer {
                    0b11 => "448 kbit/s",
                    0b10 => "384 kbit/s",
                    _ => "320 kbit/s",
                }
                .into(),
            ),
            _ => None,
        }
    }
    pub fn description(&self) -> String {
        let layer = self.layer_str_es3();
        let mode = self.mode_str_es3();
        let sf = self.sampling_freq_es3();
        let br = match (self.bit_rate_index, self.layer) {
            (Some(b), Some(l)) => self.bit_rate_es3(b, l).unwrap_or_else(|| "0".into()),
            _ => "0".into(),
        };
        format!("Audio: mp2 ({}) {}, {}, {}", layer, mode, sf, br)
    }
}

pub struct Es4 {
    pub stream_type: u8,
    pub id: Option<u8>,
    pub layer: Option<u8>,
    pub bit_rate_index: Option<u8>,
    pub sampling_frequency: Option<u8>,
    pub mode: Option<u8>,
    pub mode_extension: Option<u8>,
    pub copyright: Option<u8>,
    pub original: Option<u8>,
    pub emphasis: Option<u8>,
}

impl Es4 {
    pub fn new() -> Self {
        Self {
            stream_type: 4,
            id: None,
            layer: None,
            bit_rate_index: None,
            sampling_frequency: None,
            mode: None,
            mode_extension: None,
            copyright: None,
            original: None,
            emphasis: None,
        }
    }

    fn layer_str_es4(&self) -> &str {
        match self.layer {
            Some(0b11) => "Layer I",
            Some(0b10) => "Layer II",
            Some(0b01) => "Layer III",
            _ => "None",
        }
    }

    fn mode_str_es4(&self) -> &str {
        match self.mode {
            Some(0b00) => "stereo",
            Some(0b01) => "joint_stereo (intensity_stereo and/or ms_stereo)",
            Some(0b10) => "dual_channel",
            Some(0b11) => "mono",
            _ => "None",
        }
    }

    fn sampling_freq_es4(&self) -> &str {
        match self.sampling_frequency {
            Some(0b00) => "22.5kHz",
            Some(0b01) => "24kHz",
            Some(0b10) => "16kHz",
            _ => "None",
        }
    }

    fn bit_rate_es4(&self, bit_rate_index: u8, layer: u8) -> Option<String> {
        let layer = match layer {
            0b11 => 0b11,
            0b10 => 0b10,
            0b01 => 0b01,
            _ => return None,
        };
        match bit_rate_index {
            0b0001 => Some(
                match layer {
                    0b11 => "32 kbit/s",
                    0b10 => "8 kbit/s",
                    _ => "8 kbit/s",
                }
                .into(),
            ),
            0b0010 => Some(
                match layer {
                    0b11 => "48 kbit/s",
                    0b10 => "16 kbit/s",
                    _ => "16 kbit/s",
                }
                .into(),
            ),
            0b0011 => Some(
                match layer {
                    0b11 => "56 kbit/s",
                    0b10 => "24 kbit/s",
                    _ => "24 kbit/s",
                }
                .into(),
            ),
            0b0100 => Some(
                match layer {
                    0b11 => "64 kbit/s",
                    0b10 => "32 kbit/s",
                    _ => "32 kbit/s",
                }
                .into(),
            ),
            0b0101 => Some(
                match layer {
                    0b11 => "80 kbit/s",
                    0b10 => "40 kbit/s",
                    _ => "40 kbit/s",
                }
                .into(),
            ),
            0b0110 => Some(
                match layer {
                    0b11 => "96 kbit/s",
                    0b10 => "48 kbit/s",
                    _ => "48 kbit/s",
                }
                .into(),
            ),
            0b0111 => Some(
                match layer {
                    0b11 => "112 kbit/s",
                    0b10 => "56 kbit/s",
                    _ => "56 kbit/s",
                }
                .into(),
            ),
            0b1000 => Some(
                match layer {
                    0b11 => "128 kbit/s",
                    0b10 => "64 kbit/s",
                    _ => "64 kbit/s",
                }
                .into(),
            ),
            0b1001 => Some(
                match layer {
                    0b11 => "144 kbit/s",
                    0b10 => "80 kbit/s",
                    _ => "80 kbit/s",
                }
                .into(),
            ),
            0b1010 => Some(
                match layer {
                    0b11 => "160 kbit/s",
                    0b10 => "96 kbit/s",
                    _ => "96 kbit/s",
                }
                .into(),
            ),
            0b1011 => Some(
                match layer {
                    0b11 => "176 kbit/s",
                    0b10 => "112 kbit/s",
                    _ => "112 kbit/s",
                }
                .into(),
            ),
            0b1100 => Some(
                match layer {
                    0b11 => "192 kbit/s",
                    0b10 => "128 kbit/s",
                    _ => "128 kbit/s",
                }
                .into(),
            ),
            0b1101 => Some(
                match layer {
                    0b11 => "224 kbit/s",
                    0b10 => "144 kbit/s",
                    _ => "144 kbit/s",
                }
                .into(),
            ),
            0b1110 => Some(
                match layer {
                    0b11 => "256 kbit/s",
                    0b10 => "160 kbit/s",
                    _ => "160 kbit/s",
                }
                .into(),
            ),
            _ => None,
        }
    }

    pub fn description(&self) -> String {
        let layer = self.layer_str_es4();
        let mode = self.mode_str_es4();
        let sf = self.sampling_freq_es4();
        let br = match (self.bit_rate_index, self.layer) {
            (Some(b), Some(l)) => self.bit_rate_es4(b, l).unwrap_or_else(|| "0".into()),
            _ => "0".into(),
        };
        format!("Audio: mp2 ({}) {}, {}, {}", layer, mode, sf, br)
    }
}

pub struct Es15 {
    pub stream_type: u8,
    pub id: Option<u8>,
    pub profile: Option<u8>,
    pub sampling_frequency: Option<u8>,
    pub channel_configuration: Option<u8>,
    pub copyright: Option<u8>,
    pub original: Option<u8>,
}

impl Es15 {
    pub fn new() -> Self {
        Self {
            stream_type: 15,
            id: None,
            profile: None,
            sampling_frequency: None,
            channel_configuration: None,
            copyright: None,
            original: None,
        }
    }

    fn profile_es15(&self) -> &str {
        match self.profile {
            Some(0) => "Main",
            Some(1) => "LC",
            Some(2) => "SSR",
            _ => "None",
        }
    }

    fn sampling_freq_es15(&self) -> &str {
        match self.sampling_frequency {
            Some(0x0) => "96KHz",
            Some(0x1) => "88.2KHz",
            Some(0x2) => "64KHz",
            Some(0x3) => "48KHz",
            Some(0x4) => "44.1KHz",
            Some(0x5) => "32KHz",
            Some(0x6) => "24KHz",
            Some(0x7) => "22.05KHz",
            Some(0x8) => "16KHz",
            Some(0x9) => "12KHz",
            Some(0xA) => "11.025KHz",
            Some(0xB) => "8KHz",
            _ => "None",
        }
    }
    fn channel_configuration_es15(&self) -> &str {
        match self.channel_configuration {
            Some(1) => "Mono",
            Some(2) => "Stereo",
            Some(3) => "3.0",
            Some(4) => "4.0",
            Some(5) => "5.0",
            Some(6) => "5.1 Surround",
            Some(7) => "7.1 Surround",
            _ => "Unknown",
        }
    }
    pub fn description(&self) -> String {
        let profile = self.profile_es15();
        let mode = self.channel_configuration_es15();
        let sf = self.sampling_freq_es15();
        format!("Audio aac ({}), {}, {}", profile, mode, sf)
    }
}

pub struct Es27 {
    pub stream_type: u8,
    pub profile_idc: Option<u8>,
    pub level_idc: Option<u8>,
    pub seq_parameter_set_id: Option<u32>,
    pub horizontal_size: Option<u32>,
    pub vertical_size: Option<u32>,
    pub frame_rate: Option<u32>,
}

impl Es27 {
    pub fn new() -> Self {
        Self {
            stream_type: 27,
            profile_idc: None,
            level_idc: None,
            seq_parameter_set_id: None,
            horizontal_size: None,
            vertical_size: None,
            frame_rate: None,
        }
    }

    fn profile_es27(&self) -> &str {
        match self.profile_idc {
            Some(66) => "Baseline",
            Some(77) => "Main",
            Some(88) => "Extended",
            Some(100) => "High",
            Some(110) => "Hight 10",
            Some(122) => "High 4:2:2",
            Some(244) => "High 4:4:4 Predictive",
            Some(44) => "CAVLC 4:4:4 Intra",
            _ => "None",
        }
    }

    pub fn description(&self) -> String {
        let profile = self.profile_es27();
        let h = self.horizontal_size.unwrap_or(0);
        let v = self.vertical_size.unwrap_or(0);
        let level = self
            .level_idc
            .map(|v| format!("{:.1}", (v as f32) / 10.0))
            .unwrap_or_else(|| "0.0".to_string());
        let fr = self.frame_rate.unwrap_or(0);
        format!(
            "Video: h264 ({}), {}x{}, level {}, {} fps",
            profile, h, v, level, fr
        )
    }
}
