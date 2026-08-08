use crate::packet::TS_PACKET_SIZE;
use anyhow::{Result, anyhow, bail};

#[cfg(feature = "dvbcsa")]
mod dvbcsa {
    use std::os::raw::{c_int, c_uchar};

    #[repr(C)]
    pub struct DvbcsaKey {
        _private: [u8; 0],
    }

    #[link(name = "dvbcsa")]
    unsafe extern "C" {
        pub fn dvbcsa_key_alloc() -> *mut DvbcsaKey;
        pub fn dvbcsa_key_free(key: *mut DvbcsaKey);
        pub fn dvbcsa_key_set(cw: *const c_uchar, key: *mut DvbcsaKey);
        pub fn dvbcsa_decrypt(key: *const DvbcsaKey, data: *mut c_uchar, len: c_int);
    }
}

pub struct Biss1Descrambler {
    #[cfg(feature = "dvbcsa")]
    key_even: *mut dvbcsa::DvbcsaKey,
    #[cfg(feature = "dvbcsa")]
    key_odd: *mut dvbcsa::DvbcsaKey,
    #[cfg(not(feature = "dvbcsa"))]
    _placeholder: (),
}

#[cfg(feature = "dvbcsa")]
// SAFETY: The dvbcsa keys are owned by this descrambler and only freed in Drop.
unsafe impl Send for Biss1Descrambler {}

impl Biss1Descrambler {
    /// Create from 12-hex BISS-1 key (or 16 hex with checksum bytes).
    pub fn new_from_hex(key_hex: &str) -> Result<Self> {
        let cw = parse_biss1_key(key_hex)?;

        #[cfg(feature = "dvbcsa")]
        unsafe {
            let key_even = dvbcsa::dvbcsa_key_alloc();
            let key_odd = dvbcsa::dvbcsa_key_alloc();
            if key_even.is_null() || key_odd.is_null() {
                if !key_even.is_null() {
                    dvbcsa::dvbcsa_key_free(key_even);
                }
                if !key_odd.is_null() {
                    dvbcsa::dvbcsa_key_free(key_odd);
                }
                bail!("dvbcsa_key_alloc failed");
            }

            dvbcsa::dvbcsa_key_set(cw.as_ptr(), key_even);
            dvbcsa::dvbcsa_key_set(cw.as_ptr(), key_odd);

            Ok(Self { key_even, key_odd })
        }

        #[cfg(not(feature = "dvbcsa"))]
        {
            let _ = cw;
            bail!("feature \"dvbcsa\" is disabled")
        }
    }

    /// Descramble a single 188-byte TS packet in-place.
    /// Returns true if a scrambled payload was processed.
    pub fn descramble_ts_packet(&self, packet: &mut [u8; TS_PACKET_SIZE]) -> Result<bool> {
        if packet[0] != 0x47 {
            return Ok(false);
        }

        let scrambling = packet[3] & 0xC0;
        if scrambling == 0 {
            return Ok(false);
        }

        let af_control = (packet[3] >> 4) & 0x03;
        if af_control == 0 || af_control == 2 {
            return Ok(false);
        }

        let mut payload_offset = 4usize;
        if af_control == 3 {
            let af_len = packet[4] as usize;
            payload_offset = payload_offset
                .checked_add(1 + af_len)
                .ok_or_else(|| anyhow!("adaptation field length overflow"))?;
            if payload_offset >= TS_PACKET_SIZE {
                return Ok(false);
            }
        }

        let payload_len = TS_PACKET_SIZE - payload_offset;
        if payload_len <= 7 {
            return Ok(false);
        }

        #[cfg(feature = "dvbcsa")]
        {
            unsafe {
                let key = match scrambling {
                    0x80 => self.key_even,
                    0xC0 => self.key_odd,
                    _ => return Ok(false),
                };

                dvbcsa::dvbcsa_decrypt(
                    key,
                    packet[payload_offset..].as_mut_ptr(),
                    payload_len as i32,
                );
            }

            packet[3] &= 0x3F;
            Ok(true)
        }

        #[cfg(not(feature = "dvbcsa"))]
        {
            let _ = payload_len;
            return Err(anyhow!("feature \"dvbcsa\" is disabled"));
        }
    }

    /// Descramble a buffer containing whole TS packets.
    pub fn descramble_ts_packets(&self, data: &mut [u8]) -> Result<usize> {
        let mut processed = 0usize;
        for chunk in data.chunks_exact_mut(TS_PACKET_SIZE) {
            let packet: &mut [u8; TS_PACKET_SIZE] = chunk.try_into().expect("size is exact");
            if self.descramble_ts_packet(packet)? {
                processed += 1;
            }
        }
        Ok(processed)
    }
}

#[cfg(feature = "dvbcsa")]
impl Drop for Biss1Descrambler {
    fn drop(&mut self) {
        unsafe {
            if !self.key_even.is_null() {
                dvbcsa::dvbcsa_key_free(self.key_even);
            }
            if !self.key_odd.is_null() {
                dvbcsa::dvbcsa_key_free(self.key_odd);
            }
        }
    }
}

fn parse_biss1_key(key_hex: &str) -> Result<[u8; 8]> {
    let mut s = key_hex.trim();
    if let Some(rest) = s.strip_prefix("0x") {
        s = rest;
    }

    if s.len() != 12 && s.len() != 16 {
        bail!("BISS-1 key must be 12 or 16 hex digits");
    }

    let bytes = parse_hex_bytes(s)?;
    if bytes.len() == 8 {
        let mut cw = [0u8; 8];
        cw.copy_from_slice(&bytes);
        return Ok(cw);
    }

    if bytes.len() != 6 {
        bail!("BISS-1 12-hex key must be 6 bytes");
    }

    let csum1 = bytes[0].wrapping_add(bytes[1]).wrapping_add(bytes[2]);
    let csum2 = bytes[3].wrapping_add(bytes[4]).wrapping_add(bytes[5]);

    Ok([
        bytes[0], bytes[1], bytes[2], csum1, bytes[3], bytes[4], bytes[5], csum2,
    ])
}

fn parse_hex_bytes(s: &str) -> Result<Vec<u8>> {
    if s.len() % 2 != 0 {
        bail!("hex string must have even length");
    }

    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

fn hex_val(b: u8) -> Result<u8> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => bail!("invalid hex digit"),
    }
}
