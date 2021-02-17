# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [5.1.5] - 2021-02-17
### Added
- Support for multiple LINSTOR controllers

## [5.1.4] - 2020-11-19
### Added
- Support for APIVER7/8

## [5.1.3] - 2020-08-31
### Added
- Support for APIVER6

## [5.1.2] - 2020-06-23
### Fixed
- increase debian/changelog

## [5.1.1] - 2020-06-23
### Fixed
- Remove temporary resource after `vzdump` in case of backup via a snapshot.

## [5.1.0] - 2020-06-22
### Added
- Snapshot rollback to the last snapshot
- Backups via snapshots (i.e., snapshot support in `map_volume()`/`activate_volume()`)
