pub struct Pes {
    pub pid: u16,
    pub stream_id: u8,
    pub scrabling: u8,
    pub priority: u8,
    pub alignment_indicator: u8,
    pub copyright: u8,
    pub original_or_copy: u8,
    pub flags_pts_dts: u8,
    pub flag_escr: u8,
    pub flag_es_rate: u8,
    pub flag_dsm_trick_mode: u8,
    pub flag_additional_copy_info: u8,
    pub flag_crc: u8,
    pub flag_extension: u8,
    pub pts: Option<u64>,
    pub dts: Option<u64>,
    pub es: Option<super::es::Es>,
}

impl Pes {
    pub(crate) fn new(pid: u16) -> Self {
        Self {
            pid,
            stream_id: 0,
            scrabling: 0,
            priority: 0,
            alignment_indicator: 0,
            copyright: 0,
            original_or_copy: 0,
            flags_pts_dts: 0,
            flag_escr: 0,
            flag_es_rate: 0,
            flag_dsm_trick_mode: 0,
            flag_additional_copy_info: 0,
            flag_crc: 0,
            flag_extension: 0,
            pts: None,
            dts: None,
            es: None,
        }
    }
}
