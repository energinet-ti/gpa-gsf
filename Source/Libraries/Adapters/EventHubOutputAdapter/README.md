# EventHubOutputAdapter

Groups measurements by device and timestamp within each processing callback and sends JSON events to Azure Event Hubs over HTTPS using the REST batch API and SAS authentication. Fifty measurements for one device and timestamp in the same callback become one event. This reduces event count; actual byte traffic and cost depend on the payload and service tier.

This is an additional adapter. The existing `AzureEventHubAdapters` implementation remains available.

| Item | Value |
|---|---|
| Assembly | `EventHubOutputAdapter.dll` |
| Type | `EventHubOutputAdapter.EventHubOutputAdapter` |
| Framework | .NET Framework 4.8 |
| Dependencies | GSF.Core and GSF.TimeSeries; no Azure SDK |

## Build

From the GSF checkout, with GSF's build prerequisites and dependencies available:

```sh
dotnet build Source/Libraries/Adapters/EventHubOutputAdapter/EventHubOutputAdapter.csproj -c Release
dotnet test Source/Libraries/Tests/EventHubOutputAdapter.Tests/EventHubOutputAdapter.Tests.csproj -c Release
```

These projects reference GSF source projects. The adapter output is `Build/Output/Release/Libraries/EventHubOutputAdapter.dll`.

## Configuration

Register the assembly and type above as a custom output adapter in a GSF host such as openPDC. A broadcast configuration for sources beginning with `PMU_` is:

```text
EventHubConnectionString={Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=your-send-policy;SharedAccessKey=YOUR_KEY;EntityPath=your-event-hub};EventHubName=your-event-hub;DevicePrefixDelimiter=_;SourceFilter={PMU_*}
```

The braces preserve the connection string's inner semicolons. `SourceFilter` matches `MeasurementKey.Source`, case-insensitively, independently of the device name derived from the tag. With a configured source filter and no resolved input keys, initialization selects broadcast routing. Tags are resolved as measurements arrive, including dynamically created inputs, and routing survives metadata refresh.

For registered measurements, use the normal GSF selection syntax instead:

```text
EventHubConnectionString={Endpoint=sb://your-namespace.servicebus.windows.net/;SharedAccessKeyName=sender;SharedAccessKey=YOUR_KEY;EntityPath=your-event-hub};InputMeasurementKeys={FILTER ActiveMeasurements WHERE Device='SHELBY'}
```

`EventHubName` may be omitted when the connection string contains `EntityPath`. For the underscore-delimited broadcast example, the default `StripDevicePrefix=true` and `StripPointIdSuffix=true` are retained. Public connection-string properties and defaults are documented in the generated `EventHubOutputAdapter.xml` and exposed through GSF's adapter configuration metadata.

## Delivery and buffering

- Grouping is local to each callback. Separate callbacks can produce partial events with the same device and timestamp.
- Parallel senders and retries do not guarantee delivery order or exactly-once delivery. Consumers must tolerate duplicate events.
- Completed batches can spill to a file-based write-ahead log (WAL). Pending data held only in memory can be lost on process failure. Storage failures and delete-oldest retention can cause reported loss.
- The adapter batches by event count, not encoded byte size. Choose `MaxBatchSize` for the actual event size and Azure tier's request limits, including REST envelope overhead.
- Stop cancels requests and retry waits and attempts to persist remaining work. It does not guarantee immediate delivery. Completed retained WAL files can replay after restart.
