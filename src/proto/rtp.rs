use anyhow::Result;
use bytes::BytesMut;
use rand::Rng;
use std::sync::atomic::{AtomicU16, AtomicU32, Ordering};
use std::time::Instant;

pub(crate) struct RtpState {
    start: Instant,
    version: u8,

    padding: bool,
    extension: bool,
    csrc_count: u8,
    marker: bool,
    payload_type: u8,

    sequence: AtomicU16,
    timestamp: AtomicU32,
    ssrc: AtomicU32,
}

impl RtpState {
    pub(crate) fn new() -> Self {
        Self {
            start: Instant::now(),
            version: 2,

            padding: false,
            extension: false,
            csrc_count: 0,
            marker: false,
            payload_type: 33,

            sequence: AtomicU16::new(0),
            timestamp: AtomicU32::new(0),
            ssrc: AtomicU32::new(0),
        }
    }

    pub(crate) fn pack(&self, payload: &[u8]) -> Result<BytesMut> {
        let mut header = [0u8; 12];

        // Version (2), Padding (0), Extension (0), CSRC Count (0) -> 0b10000000 = 0x80
        header[0] = self.version << 6
            | (self.padding as u8) << 5
            | (self.extension as u8) << 4
            | (self.csrc_count);

        // Marker (0), Payload Type для MP2T (MPEG-TS за стандартом RFC 3551 це 33) -> 33
        header[1] = (self.marker as u8) << 7 | self.payload_type;

        // Sequence Number (2 байти, Big-Endian)
        let seq_bytes = self.sequence.fetch_add(1, Ordering::Relaxed).to_be_bytes();
        header[2..4].copy_from_slice(&seq_bytes);

        // SSRC (4 байти, Big-Endian)
        let mut ssrc = self.ssrc.load(Ordering::Relaxed);
        if ssrc == 0 {
            let mut rng = rand::rng();
            ssrc = rng.random();
            self.ssrc.store(ssrc, Ordering::Relaxed);
        }

        // Timestamp (4 байти, Big-Endian)
        let ts = ssrc + ((self.start.elapsed().as_nanos() * 90_000) / 1_000_000_000) as u32;
        self.timestamp.store(ts, Ordering::Relaxed);
        let ts_bytes = ts.to_be_bytes();
        header[4..8].copy_from_slice(&ts_bytes);

        let ssrc_bytes = ssrc.to_be_bytes();
        header[8..12].copy_from_slice(&ssrc_bytes);

        let mut rtp_pkt = BytesMut::with_capacity(12 + payload.len());
        rtp_pkt.extend_from_slice(&header);
        rtp_pkt.extend_from_slice(payload);

        Ok(rtp_pkt)
    }

    // full specification isn't implemented yet
    pub(crate) fn unpack(&self, packet: &[u8]) -> Result<BytesMut> {
        if packet.len() < 12 {
            return Err(anyhow::anyhow!("Invalid RTP packet"));
        }
        let header = &packet[0..12];

        let version = header[0] >> 6;
        if version != self.version {
            return Err(anyhow::anyhow!("Invalid RTP version"));
        }
        let padding = (header[0] & 0x20) != 0;
        if padding {
            return Err(anyhow::anyhow!("Unsupported RTP padding"));
        }
        let extension = (header[0] & 0x10) != 0;
        if extension {
            return Err(anyhow::anyhow!("Unsupported RTP extension"));
        }
        let csrc_count = header[0] & 0x0F;
        if csrc_count != 0 {
            return Err(anyhow::anyhow!("Unsupported RTP CSRC count"));
        }

        let marker = (header[1] & 0x80) != 0;
        if marker {
            return Err(anyhow::anyhow!("Unsupported RTP marker"));
        }
        let payload_type = header[1] & 0x7F;
        if payload_type != self.payload_type {
            return Err(anyhow::anyhow!("Invalid RTP payload type"));
        }

        let _sequence = u16::from_be_bytes(header[2..4].try_into().unwrap());
        let _timestamp = u32::from_be_bytes(header[4..8].try_into().unwrap());

        let ssrc = u32::from_be_bytes(header[8..12].try_into().unwrap());
        let current_ssrc = self.ssrc.load(Ordering::Relaxed);
        if current_ssrc == 0 {
            self.ssrc.store(ssrc, Ordering::Relaxed);
        } else if ssrc != current_ssrc {
            return Err(anyhow::anyhow!("Invalid RTP SSRC"));
        }

        let payload = &packet[12..];
        let pkt = BytesMut::from(payload);

        Ok(pkt)
    }
}
