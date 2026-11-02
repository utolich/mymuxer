# Changelog

## [Unreleased]

### Added

### Changed

### Fixed

### Removed

## [0.2.1]

### Added

- Added PID filtering in proxy mode: only PAT, PMT, PCR and the configured audio/video PIDs are forwarded.
- Added PAT/PMT patching in proxy mode (same as in mux mode).
- Proxy output is paused while PAT/PMT differ from the initial ones and resumes when they are restored.
- Added BISS descrambling for Unix socket input.
- Unix socket input now marks the stream dirty on connect/read errors or input timeout and clears the flag when data resumes.
- Unix socket input now reconnects automatically every 3 seconds after a connect failure, read error, timeout or closed socket. Repeated connect errors are logged once per minute.
- Added buffer stats.
- Downloading a dump for Unix sockets.
- Added BISS descrambling for HTTP input.

### Changed

- Updated dependencies.
- Proxy mode now always sends packets of `pkt_size` bytes (188 bytes if `pkt_size` is 0).
- Proxy passthrough (sending chunks as is) is now used only when no program is configured.
- PAT and PMT changes are tracked separately. The stream is marked dirty once per change, not on every repeated table.
- BISS descrambling for UDP, RTP, HTTP and Unix socket inputs now uses a common stream buffer: the input is synced to TS packet boundaries and only whole packets are descrambled and forwarded. The incomplete tail is kept until the next read.
- UDP and RTP input bitrate stats now count the forwarded TS bytes (the RTP header is no longer included).
- Improved marking/unmarking of the "dirty" flag.

### Fixed

- Fixed BISS descrambling of inputs whose chunks are not aligned to 188-byte TS packets.

### Removed

## [0.2.0]

### Added

- Added CHANGELOG.
- Added a configurable output buffer duration setting.
- Added support for Unix sockets.
- Added an HTTP router. Provides restreaming from a local socket to an HTTP URI.
- Added database support for the HTTP router.
- Added DTS drift stats for any PIDs.

### Changed

- Improved HLS client stability and behavior.
- Improved output buffer checking.
- Check DTS drift for any PIDs.
- Serialized worker API logs and dirty-state commands to preserve their delivery order.

### Fixed

- Adjusted the buffer size to the configured value before streaming.

### Removed
