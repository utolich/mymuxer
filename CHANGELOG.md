# Changelog

## [Unreleased]

### Added

### Changed

### Fixed

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
