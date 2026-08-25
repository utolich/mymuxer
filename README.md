**MyMuxer** is a asynchronous, multi-threaded network service written in Rust, designed for real-time reception, demultiplexing, processing, descrambling, and retransmission of media streams.

## 1. Ingest (Input Streams)
Supported Protocols:

* UDP (Multicast)
* HTTP TS
* HLS
* FFmpeg Integration: ability to use `ffmpeg` as an input stream source (enabling pre-transcoding, file streaming, or device capture).

PSI/SI Analysis & Processing:

* On-the-fly parsing of TS packet headers.
* Packet loss detection via Continuity Counter.
* Decoding of PSI/SI system information tables (PAT, PMT, SDT, EIT, etc.).

## 2. Processing & Muxing
* Filtering & Demultiplexing: Selective processing and extraction of required PIDs (video, audio, additional tracks, metadata).
* EPG Generation: Formation and injection of EIT p/f (Present/Following) EPG stream for dynamic delivery of current and upcoming program information.
* Synchronization & PCR: Generation and correction of PCR (Program Clock Reference) timestamps to eliminate jitter and prevent video/audio desynchronization during retransmission.
* Descrambling: BISS-encrypted stream descrambling with hardware/system acceleration via libdvbcsa.

## 3. Egress (Output Streams)
Streaming Protocols:

* UDP Multicast CBR
* RTP
* HTTP TS

## 4. Primary Use Cases
* IPTV / CATV Headends: Ingesting, processing, and converting incoming MPEG-TS streams into fixed-bitrate UDP CBR Multicast for delivery to digital headends (Teleste Luminato) for subsequent broadcast into CATV or IPTV networks.
* Stream Gateway: Converting HTTP/HLS streams into multicast format for internal distribution within an ISP/operator network.
* EIT/EPG Injector: Generating and multiplexing service EPG data into output TS transports.

## 5. Management
* Full control, configuration, and status monitoring are handled via HTTP API.
* A dedicated web interface (developed as a separate PHP project) is available for intuitive management of **MyMuxer**.

Operating System: Tested and optimized for production deployment on FreeBSD (Linux is also supported).

## 📬 Contact & Support

For questions, suggestions, or support, feel free to reach out:
* **Email:** [mymuxer@gmail.com](mailto:mymuxer@gmail.com)
