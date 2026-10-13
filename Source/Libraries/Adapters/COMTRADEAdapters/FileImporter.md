# COMTRADE file importer

`COMTRADEAdapters.FileImporter` is an `InputAdapterBase` implementation that reads CFG/DAT pairs through `GSF.COMTRADE.Parser`. It supports command-driven replay, a configured file, and sequential directory monitoring. Automatic measurement mapping and bad-value filtering are enabled by default.

## Replay

Configure a file with:

```text
FileName=C:\Recordings\event.cfg; ProcessingInterval=0; FramesPerTick=30; SimulateTimestamp=False
```

Or leave `FileName` unset and invoke the commands on the running input adapter:

```text
INVOKE COMTRADE_IMPORT ReplayFile "C:\Recordings\event.cfg"
INVOKE COMTRADE_IMPORT GetStatus
INVOKE COMTRADE_IMPORT GetChannelMap
INVOKE COMTRADE_IMPORT Abort
```

Commands require the Administrator or Editor role. `ReplayFile` replaces an active replay safely. `GetStatus` returns `Completed <filename> ...` after publication and post-processing, or an `Error` prefix on terminal failure. Completion reports include actual and expected frame counts, with a warning when a file is truncated. A truncated file is cleaned up like a complete one. Malformed files and read failures leave the input pair in place.

## Directory monitoring

```text
WatchDirectory=C:\Incoming; WatchFilter=*.cfg; FileSettleTimeMs=2000; ProcessedDirectory=C:\Processed; ProcessingInterval=0; FramesPerTick=30
```

The adapter waits for both files and the settle interval, handles creation/change/rename delivery, deduplicates pending files, and processes them sequentially. `ProcessedDirectory` takes precedence over `DeleteAfterProcessing=True`. With neither setting, files remain in place. Cleanup collisions report an error and leave the source pair in place. A configured `FileName` takes precedence on initialization; `ReplayFile` switches to explicit replay. `AutoRepeat=True` repeats explicit replay, not active watch processing.

## Measurements and filtering

Automatic mapping uses `{DeviceID}_{Channel}` by default; supported template tokens are `{Adapter}`, `{DeviceID}`, `{Channel}`, `{Index}`, and `{Unit}`. Magnitude and angle suffixes are kept. Existing host `ActiveMeasurements` identities are reused when the generated point tag resolves unambiguously. Runtime measurements work without a configuration database, and loading the same channels again reuses their identities. No recording-device code is treated as a database device ID, and no database records are written automatically.

Runtime-created keys can be consumed by an output adapter that accepts runtime/broadcast measurements. A database `Device` filter requires matching registered host metadata. Set `AutoCreateMeasurements=False` and configure the host's standard `OutputMeasurements` setting for positional mapping to existing measurements; a count mismatch maps the available channels and reports a warning.

`SkipBadValues=True` suppresses NaN/infinity and the scaled integer sentinel. For Binary32 files the default treats raw `INT32_MIN` as missing, as the standard specifies, and publishes raw `-32768` as a valid value. Set `Binary32UsesInt16Sentinel=True` for writers that mark missing Binary32 data with the 16-bit sentinel instead. `SkipBadValues=False` publishes parser values unchanged.

`InferTimeFromSampleRates=True` is the default. With multiple sample rates, GSF uses the last qualifying rate; use `False` for recordings with valid explicit timestamps when cumulative rate changes matter. `SimulateTimestamp=True` replaces timestamps with UTC replay time. A negative/default `ProcessingInterval` uses 33 ms, zero uses 1 ms, and a positive value supplies the interval; `FramesPerTick` defaults to one and clamps values below one to one.
