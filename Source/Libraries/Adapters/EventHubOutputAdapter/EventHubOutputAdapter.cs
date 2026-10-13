//******************************************************************************************************
//  EventHubOutputAdapter.cs - Gbtc
//
//  Copyright © 2026, Grid Protection Alliance.  All Rights Reserved.
//
//  Licensed to the Grid Protection Alliance (GPA) under one or more contributor license agreements. See
//  the NOTICE file distributed with this work for additional information regarding copyright ownership.
//  The GPA licenses this file to you under the MIT License (MIT), the "License"; you may not use this
//  file except in compliance with the License. You may obtain a copy of the License at:
//
//      http://opensource.org/licenses/MIT
//
//  Unless agreed to in writing, the subject software distributed under the License is distributed on an
//  "AS-IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. Refer to the
//  License for the specific language governing permissions and limitations.
//
//  Code Modification History:
//  ----------------------------------------------------------------------------------------------------
//  02/20/2026 - Frederik Fast
//       Generated original version of source code.
//
//******************************************************************************************************

#region [ Contributor License Agreements ]

//*******************************************************************************************************
//
//   Code contributed by Energinet:
//        https://energinet.dk
//
//   Copyright © 2026, Energinet
//
//*******************************************************************************************************

#endregion

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Net;
using System.Text;
using System.Threading;
using GSF;
using GSF.Diagnostics;
using GSF.TimeSeries;
using GSF.TimeSeries.Adapters;
using ConnectionStringParser = GSF.Configuration.ConnectionStringParser<GSF.TimeSeries.Adapters.ConnectionStringParameterAttribute>;

namespace EventHubOutputAdapter
{
    /// <summary>
    /// Sends device-framed measurements to Azure Event Hub as self-describing JSON events.
    /// Groups measurements by (device, timestamp) within each processing callback.
    /// Uses the Event Hubs REST API directly and has no Azure SDK dependency.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This adapter supports two measurement routing modes:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Standard GSF routing</b>: Set <c>InputMeasurementKeys</c> to a filter expression
    ///     (e.g., <c>FILTER ActiveMeasurements WHERE Device='SHELBY'</c>). Measurements are pre-registered
    ///     at initialization and routed by the GSF framework.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Broadcast mode with SourceFilter</b>: Set <see cref="SourceFilter"/> to a comma-separated
    ///     list of input adapter names (e.g., <c>PMU_1001</c>). The adapter receives all measurements
    ///     and filters by <see cref="MeasurementKey.Source"/> at the application level. Use this mode
    ///     for dynamically-created measurements from input adapters that are not registered in
    ///     the <c>ActiveMeasurements</c> table and cannot be routed via <c>Sourceids</c> or filter expressions.
    ///   </description></item>
    /// </list>
    /// <para>
    /// Architecture: swap-and-send with write-ahead log (WAL) and parallel sender threads.
    /// Background senders persist queued batches before sending. A full queue spills synchronously
    /// to disk. Pending in-memory data can be lost on process failure; completed WAL files are
    /// replayed on restart. Retention can discard old batches, and retries can produce duplicates.
    /// </para>
    /// </remarks>
    [Description("EventHub: Sends device-framed measurements to Azure Event Hub")]
    public class EventHubOutputAdapter : OutputAdapterBase
    {
        #region [ Members ]

        // Nested Types

        /// <summary>
        /// Holds the pre-computed device acronym and signal name for a measurement.
        /// </summary>
        internal struct SignalInfo
        {
            /// <summary>
            /// Device acronym derived from the measurement tag.
            /// </summary>
            public string DeviceAcronym;

            /// <summary>
            /// Channel name after applying the configured naming options.
            /// </summary>
            public string SignalName;

            /// <summary>
            /// Channel name escaped for inclusion in JSON.
            /// </summary>
            public string EscapedSignalName;
        }

        // A queued batch owns either its event list or the path to its persisted body.
        private struct WalBatch
        {
            public List<string> Events;
            public string WalFilePath;
            public int EventCount;
            public DateTime RetryAfter;
        }

        // Constants

        private const string DefaultTimestampFormat = "Both";
        private const int DefaultMaxBatchSize = 800;
        private const int DefaultSenderThreads = 4;
        private const int DefaultMaxBatchIntervalMs = 1000;
        private const bool DefaultStripDevicePrefix = true;
        private const string DefaultDevicePrefixDelimiter = "-";
        private const bool DefaultStripPointIdSuffix = true;
        private const bool DefaultIncludeQuality = true;
        private const bool DefaultIncludeTimestampEpochMs = true;
        private const int DefaultMaxQueuedBatches = 100;
        private const int DefaultMaxRetries = 5;
        private const int DefaultRetryBaseDelayMs = 1000;
        private const long DefaultMaxWalSizeMB = 1024;
        private const string DefaultWalDirectory = "";

        // Fields

        private Dictionary<MeasurementKey, SignalInfo> m_signalLookup;
        private readonly object m_signalLock = new object();
        private readonly object m_connectionLock = new object();
        private bool m_broadcastMode;
        private bool m_stopRequested;
        private bool m_disposing;
        private HashSet<string> m_sourceFilterExact;
        private List<string> m_sourceFilterPrefixes;
        private string m_eventHubUrl;
        private string m_sendUri;
        private string m_sasKeyName;
        private string m_sasKey;
        private List<string> m_pendingEvents;
        private readonly object m_batchLock = new object();
        private Timer m_flushTimer;
        private long m_totalEventsSent;
        private int m_lazyResolveCount;

        // Send infrastructure
        private ConcurrentQueue<WalBatch> m_sendQueue;
        private AutoResetEvent m_sendSignal;
        private Thread[] m_sendThreads;
        private volatile bool m_stopSending;
        private CancellationTokenSource m_sendCancellation;
        private ConcurrentDictionary<string, WalBatch> m_deferredRetries;

        // Helper classes
        private WriteAheadLog m_wal;
        private EventHubHttpClient m_httpClient;

        // Object pools
        private ConcurrentBag<List<string>> m_listPool;
        private Dictionary<(string device, long timestamp), List<IMeasurement>> m_framesDict;
        private List<List<IMeasurement>> m_activeFrameLists;
        private ConcurrentBag<List<IMeasurement>> m_frameListPool;

        // Metrics
        private long m_walReplayCount;
        private long m_walSpillCount;

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the Azure Event Hub connection string.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Azure Event Hub connection string (Endpoint=sb://...;SharedAccessKeyName=...;SharedAccessKey=...).")]
        public string EventHubConnectionString
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the Event Hub name (entity path).
        /// </summary>
        [ConnectionStringParameter]
        [Description("Event Hub name (entity path). Overrides EntityPath in connection string if both are set.")]
        public string EventHubName
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the timestamp format in the JSON payload: ISO8601, EpochMs, or Both.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultTimestampFormat)]
        [Description("Timestamp format: ISO8601, EpochMs, or Both.")]
        public string TimestampFormat
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum number of events per batch before sending.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMaxBatchSize)]
        [Description("Maximum number of events per batch before sending.")]
        public int MaxBatchSize
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum milliseconds to hold a batch before flushing.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMaxBatchIntervalMs)]
        [Description("Maximum milliseconds to hold a batch before flushing.")]
        public int MaxBatchIntervalMs
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets whether to strip the DeviceAcronym_ prefix from signal reference keys.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultStripDevicePrefix)]
        [Description("When true, strips the device prefix from signal reference keys in measurements.")]
        public bool StripDevicePrefix
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the delimiter between the device prefix and the signal name in signal reference keys.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultDevicePrefixDelimiter)]
        [Description("Delimiter between device prefix and signal name in signal references (e.g., '_' for DEVICE_Signal, '-' for DEVICE-Signal).")]
        public string DevicePrefixDelimiter
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets whether to strip the trailing :PointID suffix from signal references.
        /// Enable this when using standard GSF signal references (e.g., DEVICE-FQ:1).
        /// Disable this when channel names contain colons (e.g., STATION_A:Frequency).
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultStripPointIdSuffix)]
        [Description("When true, strips trailing :PointID suffix from signal references. Disable for channel names that contain colons.")]
        public bool StripPointIdSuffix
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets whether to include the Quality field in the JSON payload.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultIncludeQuality)]
        [Description("When true, includes the Quality field in the JSON payload.")]
        public bool IncludeQuality
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets whether to include the TimestampEpochMs field in the JSON payload.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultIncludeTimestampEpochMs)]
        [Description("When true, includes TimestampEpochMs in the JSON payload.")]
        public bool IncludeTimestampEpochMs
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the application-level source filter for broadcast mode.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Comma-separated list of <see cref="MeasurementKey.Source"/> values to accept.
        /// Supports exact names and trailing-wildcard patterns:
        /// </para>
        /// <list type="bullet">
        ///   <item><description><c>PMU_1001</c>: exact match on source "PMU_1001"</description></item>
        ///   <item><description><c>PMU_1001,PMU_1002</c>: exact match on either source</description></item>
        ///   <item><description><c>PMU_*</c>: matches any source starting with "PMU_"</description></item>
        ///   <item><description><c>PMU_*,RTU_*</c>: matches sources starting with "PMU_" or "RTU_"</description></item>
        /// </list>
        /// <para>
        /// When set with no resolved input keys, the adapter operates in broadcast mode: it receives
        /// all measurements from the GSF routing engine and filters by source at the application level.
        /// </para>
        /// <para>
        /// Use this instead of <c>Sourceids</c> or <c>InputMeasurementKeys</c> when working with
        /// dynamically-created measurements from input adapters that are not registered in
        /// the <c>ActiveMeasurements</c> database table. GSF's <c>Sourceids</c> queries
        /// <c>ActiveMeasurements</c> at initialization time, so it cannot route measurements
        /// that are created dynamically after startup.
        /// </para>
        /// </remarks>
        [ConnectionStringParameter]
        [DefaultValue("")]
        [Description("Comma-separated source names to accept in broadcast mode. " +
                     "Supports wildcards (e.g., PMU_* matches all sources starting with PMU_). " +
                     "Use instead of Sourceids for auto-created measurements not in ActiveMeasurements.")]
        public string SourceFilter
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the number of parallel sender threads.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultSenderThreads)]
        [Description("Number of parallel sender threads.")]
        public int SenderThreads
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum batches queued in memory for sending.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMaxQueuedBatches)]
        [Description("Maximum batches queued in memory for sending before spilling to WAL only.")]
        public int MaxQueuedBatches
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum retry attempts per batch within one transient-failure send cycle.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMaxRetries)]
        [Description("Maximum retry attempts per batch within one send cycle on transient HTTP failure.")]
        public int MaxRetries
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the base delay in milliseconds for exponential retry backoff.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultRetryBaseDelayMs)]
        [Description("Base delay in milliseconds for exponential retry backoff.")]
        public int RetryBaseDelayMs
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the maximum WAL directory size in MB.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultMaxWalSizeMB)]
        [Description("Maximum WAL directory size in MB. Oldest batches dropped when exceeded.")]
        public long MaxWalSizeMB
        {
            get;
            set;
        }

        /// <summary>
        /// Gets or sets the WAL directory path.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(DefaultWalDirectory)]
        [Description("WAL directory path. Defaults to {AppDir}/EventHubWal/{AdapterName}/.")]
        public string WalDirectory
        {
            get;
            set;
        }

        /// <summary>
        /// Returns true: measurements are being archived to Event Hub.
        /// </summary>
        public override bool OutputIsForArchive => true;

        /// <summary>
        /// Returns false: connection is synchronous.
        /// </summary>
        protected override bool UseAsyncConnect => false;

        /// <summary>
        /// Gets or sets input keys while preserving broadcast routing selected at initialization.
        /// </summary>
        [ConnectionStringParameter]
        [DefaultValue(null)]
        [Description("Defines primary keys of input measurements; accepts a filter expression, measurement key, point tag or Guid.")]
        [CustomConfigurationEditor("GSF.TimeSeries.UI.WPF.dll", "GSF.TimeSeries.UI.Editors.MeasurementEditor")]
        public override MeasurementKey[] InputMeasurementKeys
        {
            get => base.InputMeasurementKeys;
            set => base.InputMeasurementKeys = m_broadcastMode && (value == null || value.Length == 0) ? null : value;
        }

        /// <summary>
        /// Gets or sets the signal lookup dictionary used by adapter tests.
        /// </summary>
        internal Dictionary<MeasurementKey, SignalInfo> SignalLookup
        {
            get => m_signalLookup;
            set => m_signalLookup = value;
        }

        /// <summary>
        /// Gets the detailed status of this adapter.
        /// </summary>
        public override string Status
        {
            get
            {
                StringBuilder status = new StringBuilder();

                status.Append(base.Status);
                status.AppendLine();
                status.AppendFormat("         Event Hub name: {0}", EventHubName);
                status.AppendLine();
                status.AppendFormat("          Event Hub URL: {0}", m_httpClient?.EventHubUrl ?? m_eventHubUrl ?? "(not connected)");
                status.AppendLine();
                status.AppendFormat("       Timestamp format: {0}", TimestampFormat);
                status.AppendLine();
                status.AppendFormat("        Max batch size: {0}", MaxBatchSize);
                status.AppendLine();
                status.AppendFormat("    Max batch interval: {0}ms", MaxBatchIntervalMs);
                status.AppendLine();
                status.AppendFormat("     Strip device prefix: {0}", StripDevicePrefix);
                status.AppendLine();
                status.AppendFormat("  Device prefix delim.: {0}", DevicePrefixDelimiter);
                status.AppendLine();
                status.AppendFormat("  Strip :PointID suffix: {0}", StripPointIdSuffix);
                status.AppendLine();
                status.AppendFormat("        Include quality: {0}", IncludeQuality);
                status.AppendLine();
                status.AppendFormat("  Include epoch millis: {0}", IncludeTimestampEpochMs);
                status.AppendLine();
                status.AppendFormat("         Source filter: {0}",
                    (m_sourceFilterExact != null || m_sourceFilterPrefixes != null) ? SourceFilter : "(none - standard routing)");
                status.AppendLine();
                status.AppendFormat("           Max retries: {0}", MaxRetries);
                status.AppendLine();
                status.AppendFormat("     Retry base delay: {0}ms", RetryBaseDelayMs);
                status.AppendLine();
                status.AppendFormat("       Max WAL size MB: {0}", MaxWalSizeMB);
                status.AppendLine();
                status.AppendFormat("       Sender threads: {0}", SenderThreads);
                status.AppendLine();
                status.AppendFormat("          Queue depth: {0}", m_sendQueue?.Count ?? 0);
                status.AppendLine();
                status.AppendFormat("       WAL spill count: {0}", m_walSpillCount);
                status.AppendLine();
                status.AppendFormat("       Live events sent: {0}", m_totalEventsSent);
                status.AppendLine();

                if (m_wal != null)
                {
                    status.AppendFormat("         WAL directory: {0}", m_wal.WalPath);
                    status.AppendLine();
                    status.AppendFormat("        WAL file count: {0}", m_wal.PendingFileCount);
                    status.AppendLine();
                    status.AppendFormat(" Replay batches queued: {0}", m_walReplayCount);
                    status.AppendLine();
                    status.AppendFormat("      Dropped batches: {0}", m_wal.DroppedBatches);
                    status.AppendLine();
                }

                lock (m_signalLock)
                {
                    if (m_signalLookup != null)
                    {
                        int lazyCount = m_lazyResolveCount;
                        int preRegistered = m_signalLookup.Count - lazyCount;
                        status.AppendFormat("   Signal lookup count: {0} ({1} pre-registered, {2} lazily resolved)",
                            m_signalLookup.Count, preRegistered, lazyCount);
                        status.AppendLine();
                    }
                }

                return status.ToString();
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Stops the adapter and releases its owned sender resources.
        /// </summary>
        /// <param name="disposing">Whether managed resources should be released.</param>
        protected override void Dispose(bool disposing)
        {
            lock (m_connectionLock)
            {
                if (disposing && !IsDisposed)
                {
                    m_disposing = true;
                    Stop();
                    // Base Stop reports disconnect errors; retry cleanup before disposing its resources.
                    AttemptDisconnection();
                }

                base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Returns a short status string for the adapter.
        /// </summary>
        /// <param name="maxLength">Maximum display width.</param>
        /// <returns>A summary of sent live events.</returns>
        public override string GetShortStatus(int maxLength)
        {
            return $"Sent {m_totalEventsSent} live events to Event Hub '{EventHubName}'.".CenterText(maxLength);
        }

        /// <summary>
        /// Initializes the adapter: parses connection string parameters, builds signal lookup,
        /// and allocates reusable containers.
        /// </summary>
        public override void Initialize()
        {
            m_broadcastMode = false;
            new ConnectionStringParser().ParseConnectionString(ConnectionString, this);

            base.Initialize();

            if (string.IsNullOrWhiteSpace(EventHubConnectionString))
                throw new InvalidOperationException("EventHubConnectionString is required.");

            ValidateConfiguration();

            ParseSourceFilter(SourceFilter);

            // When SourceFilter is set and no explicit InputMeasurementKeys filter was resolved
            // by the framework, switch to broadcast mode. In broadcast mode, the GSF routing
            // engine delivers ALL measurements to ProcessMeasurements(), and we filter by
            // measurement.Key.Source at the application level.
            bool hasSourceFilter = m_sourceFilterExact != null || m_sourceFilterPrefixes != null;

            if (hasSourceFilter && (InputMeasurementKeys == null || InputMeasurementKeys.Length == 0))
            {
                m_broadcastMode = true;
                InputMeasurementKeys = null;
                OnStatusMessage(MessageLevel.Info,
                    $"Broadcast mode enabled; filtering by SourceFilter: {SourceFilter}");
            }

            ParseEventHubConnectionString();

            // Allocate reusable per-call containers once
            m_framesDict = new Dictionary<(string device, long timestamp), List<IMeasurement>>();
            m_activeFrameLists = new List<List<IMeasurement>>();
            m_frameListPool = new ConcurrentBag<List<IMeasurement>>();

            if (hasSourceFilter)
            {
                // Broadcast mode: all signals resolved lazily in ResolveSignalInfo().
                // No pre-registration needed since InputMeasurementKeys is null.
                m_signalLookup = new Dictionary<MeasurementKey, SignalInfo>();
                m_lazyResolveCount = 0;
            }
            else
            {
                BuildSignalLookup();
            }

            string mode = hasSourceFilter ? "broadcast+SourceFilter" : "standard routing";
            OnStatusMessage(MessageLevel.Info,
                $"Configured for Event Hub '{EventHubName}' at {m_eventHubUrl}: " +
                $"{m_signalLookup.Count} pre-registered measurements, mode={mode}");
        }

        /// <summary>
        /// Rebuilds the signal lookup when the framework recalculates routing tables.
        /// </summary>
        protected override void OnInputMeasurementKeysUpdated()
        {
            // Publish the complete lookup before notifying routing consumers. Broadcast updates
            // retain lazily resolved entries instead of replacing them with an empty lookup.
            if (m_signalLookup != null && InputMeasurementKeys is { Length: > 0 })
                BuildSignalLookup();

            base.OnInputMeasurementKeysUpdated();
        }

        // Validate numeric ranges and normalize supported timestamp format names.
        private void ValidateConfiguration()
        {
            if (MaxBatchSize <= 0 || MaxBatchIntervalMs <= 0)
                throw new InvalidOperationException("MaxBatchSize and MaxBatchIntervalMs must be positive.");

            if (MaxQueuedBatches < 0 || MaxRetries < 0 || MaxRetries == int.MaxValue || RetryBaseDelayMs < 0)
                throw new InvalidOperationException("Queue capacity, retries and retry delay must be nonnegative; MaxRetries must be less than Int32.MaxValue.");

            if (MaxWalSizeMB < 0 || MaxWalSizeMB > long.MaxValue / (1024 * 1024))
                throw new InvalidOperationException("MaxWalSizeMB is outside the supported range.");

            if (string.Equals(TimestampFormat, "ISO8601", StringComparison.OrdinalIgnoreCase))
                TimestampFormat = "ISO8601";
            else if (string.Equals(TimestampFormat, "EpochMs", StringComparison.OrdinalIgnoreCase))
                TimestampFormat = "EpochMs";
            else if (string.Equals(TimestampFormat, "Both", StringComparison.OrdinalIgnoreCase))
                TimestampFormat = "Both";
            else
                throw new InvalidOperationException("TimestampFormat must be ISO8601, EpochMs or Both.");
        }

        /// <summary>
        /// Initializes the HTTP client and WAL, queues persisted batches, and starts sender
        /// threads and the flush timer.
        /// </summary>
        protected override void AttemptConnection()
        {
            lock (m_connectionLock)
            {
                if (m_stopRequested || m_disposing || IsDisposed)
                    throw new OperationCanceledException("The adapter was stopped before the connection attempt could begin.");

                // A failed shutdown retains ownership. Finish it before replacing any shared state.
                StopSenders();

                try
                {
                    int threadCount = Math.Max(1, SenderThreads);
                    m_httpClient = new EventHubHttpClient(m_eventHubUrl, m_sendUri, m_sasKeyName, m_sasKey);
                    m_httpClient.ConfigureServicePoint(threadCount);

                    string walPath = string.IsNullOrWhiteSpace(WalDirectory)
                        ? Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "EventHubWal", Name ?? "default")
                        : WalDirectory;

                    m_wal = new WriteAheadLog(walPath);
                    m_sendQueue = new ConcurrentQueue<WalBatch>();
                    m_deferredRetries = new ConcurrentDictionary<string, WalBatch>();
                    m_listPool = new ConcurrentBag<List<string>>();
                    m_sendSignal = new AutoResetEvent(false);
                    m_sendCancellation = new CancellationTokenSource();
                    m_stopSending = false;

                    lock (m_batchLock)
                        m_pendingEvents = RentList();

                    ReplayWalFiles();

                    m_sendThreads = new Thread[threadCount];
                    for (int i = 0; i < threadCount; i++)
                    {
                        m_sendThreads[i] = new Thread(SendLoop)
                        {
                            Name = $"EventHubSender_{i}",
                            IsBackground = true
                        };
                        m_sendThreads[i].Start();
                    }

                    // The token also makes callbacks queued by an old timer harmless after reconnect.
                    m_flushTimer = new Timer(OnFlushTimer, m_sendCancellation.Token, MaxBatchIntervalMs, MaxBatchIntervalMs);
                    OnStatusMessage(MessageLevel.Info,
                        $"Connected to Event Hub '{EventHubName}' via REST API. " +
                        $"Sender threads: {threadCount}, WAL: {m_wal.WalPath}");
                }
                catch
                {
                    StopSenders();
                    throw;
                }
            }
        }

        /// <summary>
        /// Cancels sending, waits for workers and persists outstanding batches before releasing resources.
        /// </summary>
        protected override void AttemptDisconnection()
        {
            lock (m_connectionLock)
                StopSenders();
        }

        /// <summary>
        /// Starts the framework connection cycle, allowing a new sender connection.
        /// </summary>
        public override void Start()
        {
            lock (m_connectionLock)
            {
                if (m_disposing)
                    throw new ObjectDisposedException(Name);

                base.Start();
                m_stopRequested = false;
            }
        }

        /// <summary>
        /// Stops the connection cycle and prevents a previously queued attempt from reopening it.
        /// </summary>
        public override void Stop()
        {
            lock (m_connectionLock)
            {
                m_stopRequested = true;
                base.Stop();
            }
        }

        /// <summary>
        /// Starts framework processing only if the connection is still current and enabled.
        /// </summary>
        protected override void OnConnected()
        {
            lock (m_connectionLock)
            {
                if (!m_stopRequested && !m_disposing && !IsDisposed && Enabled && m_sendThreads != null)
                    base.OnConnected();
            }
        }

        // Retain queued work and shared resources until sender shutdown succeeds.
        private void StopSenders()
        {
            m_stopSending = true;
            m_flushTimer?.Dispose();
            m_flushTimer = null;
            m_sendCancellation?.Cancel();
            m_sendSignal?.Set();

            Stopwatch wait = Stopwatch.StartNew();
            if (m_sendThreads != null)
            {
                foreach (Thread thread in m_sendThreads)
                {
                    if (thread == null || !thread.IsAlive)
                        continue;

                    TimeSpan remaining = TimeSpan.FromSeconds(30) - wait.Elapsed;
                    if (thread == Thread.CurrentThread || remaining <= TimeSpan.Zero || !thread.Join(remaining))
                        throw new InvalidOperationException("A sender has not stopped; connection resources are retained and cannot be replaced.");
                }
            }

            // Storage failures retain the original lists/queue for a later cleanup attempt.
            lock (m_batchLock)
            {
                if (m_pendingEvents is { Count: > 0 })
                {
                    m_wal.Write(BuildBatchBody(m_pendingEvents));
                    ReturnList(m_pendingEvents);
                }

                m_pendingEvents = null;
            }

            while (m_sendQueue != null && m_sendQueue.TryDequeue(out WalBatch batch))
            {
                if (batch.Events == null)
                    continue;

                try
                {
                    m_wal.Write(BuildBatchBody(batch.Events));
                    ReturnList(batch.Events);
                }
                catch (Exception ex)
                {
                    m_sendQueue.Enqueue(batch);
                    throw new IOException("Shutdown could not persist a queued batch; it remains in memory.", ex);
                }
            }

            m_sendThreads = null;
            m_sendQueue = null;
            m_deferredRetries = null;
            m_sendSignal?.Dispose();
            m_sendSignal = null;
            m_sendCancellation?.Dispose();
            m_sendCancellation = null;
        }

        /// <summary>
        /// Core processing: groups measurements by (device, timestamp), builds JSON events,
        /// and enqueues them for background sending. Backpressure can require synchronous WAL I/O.
        /// </summary>
        /// <param name="measurements">Measurements supplied by the framework for this callback.</param>
        protected override void ProcessMeasurements(IMeasurement[] measurements)
        {
            lock (m_signalLock)
                ProcessMeasurementsCore(measurements);
        }

        // Group only measurements supplied by this callback while holding the signal lock.
        private void ProcessMeasurementsCore(IMeasurement[] measurements)
        {
            ReturnFrameLists();
            m_framesDict.Clear();

            foreach (IMeasurement measurement in measurements)
            {
                if (!m_signalLookup.TryGetValue(measurement.Key, out SignalInfo info))
                {
                    // Lazily resolve measurements not pre-registered at Initialize() time.
                    info = ResolveSignalInfo(measurement);

                    if (info.SignalName == null)
                        continue;

                    m_signalLookup[measurement.Key] = info;
                    Interlocked.Increment(ref m_lazyResolveCount);

                    int count = m_lazyResolveCount;
                    if (count == 1)
                    {
                        OnStatusMessage(MessageLevel.Info,
                            $"Resolving auto-created measurements; first: {info.DeviceAcronym}/{info.SignalName}");
                    }
                    else if (count % 100 == 0)
                    {
                        OnStatusMessage(MessageLevel.Info,
                            $"Lazily resolved {count} measurements so far");
                    }
                }

                var key = (info.DeviceAcronym, measurement.Timestamp.Value);

                if (!m_framesDict.TryGetValue(key, out List<IMeasurement> list))
                {
                    list = RentFrameList();
                    m_framesDict[key] = list;
                }

                list.Add(measurement);
            }

            foreach (KeyValuePair<(string device, long timestamp), List<IMeasurement>> kvp in m_framesDict)
            {
                string json = BuildJson(kvp.Key.device, kvp.Key.timestamp, kvp.Value);

                lock (m_batchLock)
                {
                    if (m_pendingEvents == null)
                        continue;

                    m_pendingEvents.Add(json);

                    if (m_pendingEvents.Count >= MaxBatchSize)
                    {
                        try
                        {
                            SwapAndSignal();
                        }
                        catch (Exception ex)
                        {
                            OnProcessException(MessageLevel.Error, new IOException("Batch handoff failed; events remain pending in memory.", ex));
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Parses the Event Hub connection string to extract endpoint, SAS key name, and SAS key.
        /// </summary>
        private void ParseEventHubConnectionString()
        {
            string endpoint = null;
            string entityPath = null;

            foreach (string segment in EventHubConnectionString.Split(';'))
            {
                string trimmed = segment.Trim();
                if (string.IsNullOrEmpty(trimmed))
                    continue;

                int eqIdx = trimmed.IndexOf('=');
                if (eqIdx < 0)
                    continue;

                string key = trimmed.Substring(0, eqIdx).Trim();
                string value = trimmed.Substring(eqIdx + 1).Trim();

                if (key.Equals("Endpoint", StringComparison.OrdinalIgnoreCase))
                    endpoint = value;
                else if (key.Equals("SharedAccessKeyName", StringComparison.OrdinalIgnoreCase))
                    m_sasKeyName = value;
                else if (key.Equals("SharedAccessKey", StringComparison.OrdinalIgnoreCase))
                    m_sasKey = value;
                else if (key.Equals("EntityPath", StringComparison.OrdinalIgnoreCase))
                    entityPath = value;
            }

            if (string.IsNullOrEmpty(endpoint))
                throw new InvalidOperationException("Endpoint not found in EventHubConnectionString.");

            if (string.IsNullOrEmpty(m_sasKeyName) || string.IsNullOrEmpty(m_sasKey))
                throw new InvalidOperationException("SharedAccessKeyName and SharedAccessKey are required in EventHubConnectionString.");

            // Use EventHubName property; fall back to EntityPath from connection string
            string hubName = !string.IsNullOrWhiteSpace(EventHubName) ? EventHubName : entityPath;
            if (string.IsNullOrEmpty(hubName))
                throw new InvalidOperationException("EventHubName is required (set as parameter or as EntityPath in connection string).");

            EventHubName = hubName;

            // Convert sb:// to https://
            string baseUrl = endpoint.Replace("sb://", "https://").TrimEnd('/');
            m_eventHubUrl = $"{baseUrl}/{hubName}";
            m_sendUri = $"{m_eventHubUrl}/messages?timeout=60&api-version=2014-01";
        }

        /// <summary>
        /// Pre-registers signal info for measurements known at initialization time.
        /// </summary>
        private void BuildSignalLookup()
        {
            Dictionary<MeasurementKey, SignalInfo> lookup = new Dictionary<MeasurementKey, SignalInfo>();
            HashSet<string> devices = new HashSet<string>();

            MeasurementKey[] keys = InputMeasurementKeys;

            foreach (MeasurementKey key in keys ?? Array.Empty<MeasurementKey>())
            {
                string delimiter = DevicePrefixDelimiter ?? "_";

                // Use TagName as the signal reference so lazily resolved measurements and
                // pre-registered keys derive names the same way.
                string signalRef = key.Metadata.TagName;

                if (string.IsNullOrEmpty(signalRef))
                    continue;

                string device = null;
                int delimIdx = signalRef.IndexOf(delimiter, StringComparison.Ordinal);
                if (delimIdx > 0)
                    device = signalRef.Substring(0, delimIdx);

                string signalName = DeriveSignalName(signalRef, device, delimiter);

                lookup[key] = new SignalInfo
                {
                    DeviceAcronym = device,
                    SignalName = signalName,
                    EscapedSignalName = EscapeJsonString(signalName)
                };

                if (!string.IsNullOrEmpty(device))
                    devices.Add(device);
            }

            lock (m_signalLock)
            {
                m_signalLookup = lookup;
                m_lazyResolveCount = 0;
            }

            OnStatusMessage(MessageLevel.Info,
                $"Signal lookup built: {lookup.Count} measurements across {devices.Count} devices");
        }

        /// <summary>
        /// Resolves signal info for a measurement not known at initialization time.
        /// Uses only <see cref="MeasurementMetadata.TagName"/> to identify the measurement.
        /// </summary>
        /// <param name="measurement">Measurement whose source and tag are examined.</param>
        /// <returns>The derived identity, or a default value when the measurement cannot be resolved.</returns>
        private SignalInfo ResolveSignalInfo(IMeasurement measurement)
        {
            if (!MatchesSourceFilter(measurement.Key.Source))
                return default;

            string tagName = measurement.Metadata.TagName;

            if (string.IsNullOrEmpty(tagName))
                return default;

            string delimiter = DevicePrefixDelimiter ?? "_";
            string device = null;

            int delimIdx = tagName.IndexOf(delimiter, StringComparison.Ordinal);
            if (delimIdx > 0)
                device = tagName.Substring(0, delimIdx);

            string signalName = DeriveSignalName(tagName, device, delimiter);

            return new SignalInfo
            {
                DeviceAcronym = device,
                SignalName = signalName,
                EscapedSignalName = EscapeJsonString(signalName)
            };
        }

        /// <summary>
        /// Applies prefix stripping and :PointID suffix stripping to derive the final signal name.
        /// </summary>
        /// <param name="signalRef">Measurement tag used as the starting channel name.</param>
        /// <param name="device">Device prefix derived from the tag.</param>
        /// <param name="delimiter">Separator between the device prefix and channel name.</param>
        /// <returns>The channel name after applying the configured stripping options.</returns>
        private string DeriveSignalName(string signalRef, string device, string delimiter)
        {
            string signalName = signalRef;

            if (StripDevicePrefix && !string.IsNullOrEmpty(device) && signalRef.StartsWith(device + delimiter))
                signalName = signalRef.Substring(device.Length + delimiter.Length);

            if (StripPointIdSuffix)
            {
                int colonIdx = signalName.LastIndexOf(':');
                if (colonIdx > 0)
                    signalName = signalName.Substring(0, colonIdx);
            }

            return signalName;
        }

        /// <summary>
        /// Parses a source filter and initializes the internal filter state.
        /// </summary>
        /// <param name="filter">Comma-separated source names or prefixes ending in an asterisk.</param>
        internal void ParseSourceFilter(string filter)
        {
            SourceFilter = filter;
            m_sourceFilterExact = null;
            m_sourceFilterPrefixes = null;

            if (string.IsNullOrWhiteSpace(filter))
                return;

            foreach (string source in filter.Split(','))
            {
                string trimmed = source.Trim();

                if (trimmed.Length == 0)
                    continue;

                if (trimmed.EndsWith("*"))
                {
                    string prefix = trimmed.Substring(0, trimmed.Length - 1);

                    if (prefix.Length > 0)
                    {
                        m_sourceFilterPrefixes ??= new List<string>();
                        m_sourceFilterPrefixes.Add(prefix);
                    }
                }
                else
                {
                    m_sourceFilterExact ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    m_sourceFilterExact.Add(trimmed);
                }
            }
        }

        /// <summary>
        /// Determines whether a measurement source matches the configured filter.
        /// </summary>
        /// <param name="source">Measurement source to match.</param>
        /// <returns><c>true</c> when a configured source name or prefix matches; otherwise, <c>false</c>.</returns>
        internal bool MatchesSourceFilter(string source)
        {
            if (m_sourceFilterExact == null && m_sourceFilterPrefixes == null)
                return false;

            if (string.IsNullOrEmpty(source))
                return false;

            if (m_sourceFilterExact != null && m_sourceFilterExact.Contains(source))
                return true;

            if (m_sourceFilterPrefixes != null)
            {
                foreach (string prefix in m_sourceFilterPrefixes)
                {
                    if (source.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                        return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Builds JSON for measurements grouped by device and timestamp within one callback.
        /// </summary>
        /// <param name="device">Device name to include in the event.</param>
        /// <param name="timestamp">Measurement timestamp in UTC ticks.</param>
        /// <param name="measurements">Measurements grouped into this event.</param>
        /// <returns>The event body with the configured timestamps, channel names and quality.</returns>
        internal string BuildJson(string device, long timestamp, List<IMeasurement> measurements)
        {
            lock (m_signalLock)
                return BuildJsonCore(device, timestamp, measurements);
        }

        // Serialize the group while the caller holds the signal lookup lock.
        private string BuildJsonCore(string device, long timestamp, List<IMeasurement> measurements)
        {
            StringBuilder sb = new StringBuilder(512);
            sb.Append("{\"DeviceID\":\"").Append(EscapeJsonString(device)).Append('"');

            DateTime dt = new DateTime(timestamp, DateTimeKind.Utc);

            if (TimestampFormat == "ISO8601" || TimestampFormat == "Both")
                sb.Append(",\"Timestamp\":\"").Append(dt.ToString("o")).Append('"');

            if (IncludeTimestampEpochMs && (TimestampFormat == "EpochMs" || TimestampFormat == "Both"))
            {
                long epochMs = (long)(dt - s_epoch).TotalMilliseconds;
                sb.Append(",\"TimestampEpochMs\":").Append(epochMs);
            }

            sb.Append(",\"Measurements\":{");
            bool first = true;
            int qualityAggregate = 0;

            foreach (IMeasurement m in measurements)
            {
                if (!m_signalLookup.TryGetValue(m.Key, out SignalInfo info))
                    continue;

                if (!first)
                    sb.Append(',');
                first = false;

                sb.Append('"').Append(info.EscapedSignalName).Append("\":");

                double value = m.AdjustedValue;
                if (double.IsNaN(value))
                    sb.Append("\"NaN\"");
                else if (double.IsPositiveInfinity(value))
                    sb.Append("\"Infinity\"");
                else if (double.IsNegativeInfinity(value))
                    sb.Append("\"-Infinity\"");
                else
                    sb.Append(value.ToString(CultureInfo.InvariantCulture));

                qualityAggregate |= (int)m.StateFlags;
            }

            sb.Append('}');
            sb.Append(",\"MeasurementCount\":").Append(measurements.Count);

            if (IncludeQuality)
                sb.Append(",\"Quality\":").Append(qualityAggregate);

            sb.Append('}');
            return sb.ToString();
        }

        /// <summary>
        /// Swaps the current pending events list with a fresh one and enqueues the full list
        /// for background sending. When the queue exceeds MaxQueuedBatches, spills to WAL
        /// to bound memory. Must be called under m_batchLock.
        /// </summary>
        private void SwapAndSignal()
        {
            List<string> ready = m_pendingEvents;
            List<string> next = RentList();

            try
            {
                WalBatch batch = new WalBatch
                {
                    Events = ready,
                    EventCount = ready.Count
                };

                if (m_sendQueue.Count >= MaxQueuedBatches)
                {
                    // Keep the original list until publication succeeds.
                    batch.WalFilePath = m_wal.Write(BuildBatchBody(ready));
                    batch.Events = null;
                    Interlocked.Increment(ref m_walSpillCount);
                }

                m_sendQueue.Enqueue(batch);
                m_pendingEvents = next;
                if (batch.Events == null)
                    ReturnList(ready);

                m_sendSignal.Set();
            }
            catch
            {
                ReturnList(next);
                throw;
            }
        }

        /// <summary>
        /// Sends queued batches and schedules retained transient failures without blocking newer work.
        /// </summary>
        private void SendLoop()
        {
            int walSizeCheckCounter = 0;
            CancellationToken cancellation = m_sendCancellation.Token;

            while (!cancellation.IsCancellationRequested)
            {
                QueueDueRetries();

                if (!m_sendQueue.TryDequeue(out WalBatch batch))
                {
                    // Periodically revisit delayed retries even with no new measurements.
                    m_sendSignal.WaitOne(Math.Min(MaxBatchIntervalMs, 1000));
                    continue;
                }

                string walFile = batch.WalFilePath;
                bool acquired = false;
                bool retryLater = false;

                try
                {
                    cancellation.ThrowIfCancellationRequested();
                    string body;

                    if (batch.Events != null)
                    {
                        body = BuildBatchBody(batch.Events);
                        walFile = m_wal.Write(body, acquire: true);
                        acquired = true;
                        batch.WalFilePath = walFile;
                        ReturnList(batch.Events);
                        batch.Events = null;
                    }
                    else
                    {
                        // Retention may have removed a queued file. It is already counted as a drop.
                        acquired = m_wal.TryAcquire(walFile);
                        if (!acquired)
                            continue;

                        body = m_wal.Read(walFile);
                    }

                    SendWithRetry(body, walFile, batch.EventCount);
                }
                catch (OperationCanceledException) when (cancellation.IsCancellationRequested)
                {
                    if (batch.Events != null)
                        m_sendQueue.Enqueue(batch);
                }
                catch (Exception ex)
                {
                    if (batch.Events != null)
                    {
                        m_sendQueue.Enqueue(batch);
                        OnProcessException(MessageLevel.Error, new IOException("WAL publication failed; batch remains queued in memory.", ex));
                        cancellation.WaitHandle.WaitOne(Math.Max(1000, Math.Min(RetryBaseDelayMs, 60000)));
                    }
                    else
                    {
                        if (ex is WebException webException && EventHubHttpClient.IsTransient(webException))
                        {
                            batch.RetryAfter = DateTime.UtcNow.AddMilliseconds(Math.Max(1000, Math.Min(RetryBaseDelayMs, 60000)));
                            retryLater = true;
                        }

                        OnProcessException(MessageLevel.Error, new InvalidOperationException($"Send failed; WAL file retained: {walFile}", ex));
                    }
                }
                finally
                {
                    if (acquired)
                        m_wal.Release(walFile);
                }

                // Publish eligibility only after this attempt has released the file.
                if (retryLater)
                    m_deferredRetries[walFile] = batch;

                if (++walSizeCheckCounter % 100 == 0)
                {
                    try
                    {
                        m_wal.EnforceSizeLimit(MaxWalSizeMB);
                    }
                    catch (Exception ex)
                    {
                        OnProcessException(MessageLevel.Error, new IOException("WAL retention failed.", ex));
                    }
                }
            }
        }

        // Requeue persisted batches after their retry delay has elapsed.
        private void QueueDueRetries()
        {
            DateTime now = DateTime.UtcNow;
            foreach (KeyValuePair<string, WalBatch> entry in m_deferredRetries)
            {
                if (entry.Value.RetryAfter <= now && m_deferredRetries.TryRemove(entry.Key, out WalBatch batch))
                    m_sendQueue.Enqueue(batch);
            }
        }

        /// <summary>
        /// Sends one retry cycle. On success, deletes the WAL file; cancellation retains it.
        /// </summary>
        /// <param name="batchBody">Serialized REST batch body to send.</param>
        /// <param name="walFile">Persisted batch file to delete after a successful send.</param>
        /// <param name="eventCount">Number of live events, or a negative value for a replayed batch.</param>
        private void SendWithRetry(string batchBody, string walFile, int eventCount)
        {
            CancellationToken cancellation = m_sendCancellation.Token;
            for (int attempt = 0; attempt <= MaxRetries; attempt++)
            {
                cancellation.ThrowIfCancellationRequested();

                try
                {
                    m_httpClient.Send(batchBody, cancellation);

                    if (!string.IsNullOrEmpty(walFile))
                    {
                        try
                        {
                            m_wal.Delete(walFile);
                        }
                        catch (Exception ex)
                        {
                            OnProcessException(MessageLevel.Warning, ex);
                        }
                    }

                    if (eventCount > 0)
                        Interlocked.Add(ref m_totalEventsSent, eventCount);
                    return;
                }
                catch (WebException ex) when (EventHubHttpClient.IsTransient(ex) && attempt < MaxRetries)
                {
                    int delay = (int)Math.Min(int.MaxValue, RetryBaseDelayMs * Math.Pow(2.0D, Math.Min(attempt, 31)));
                    OnStatusMessage(MessageLevel.Warning,
                        $"Transient send failure (attempt {attempt + 1}/{MaxRetries + 1}), retrying in {delay}ms: {ex.Message}");
                    if (cancellation.WaitHandle.WaitOne(delay))
                        cancellation.ThrowIfCancellationRequested();
                }
            }
        }

        /// <summary>
        /// Scans the WAL directory for persisted batch files from a previous run and enqueues
        /// them for resend.
        /// </summary>
        private void ReplayWalFiles()
        {
            string[] files = m_wal.GetPendingFiles();
            m_walReplayCount = files.Length;

            foreach (string file in files)
            {
                m_sendQueue.Enqueue(new WalBatch
                {
                    Events = null,
                    WalFilePath = file,
                    EventCount = -1
                });
            }

            if (files.Length > 0)
            {
                // Set sequence counter past existing files
                string lastFile = Path.GetFileNameWithoutExtension(files[files.Length - 1]);
                if (long.TryParse(lastFile, out long lastSeq))
                    m_wal.SetSequencePast(lastSeq);

                OnStatusMessage(MessageLevel.Info, $"WAL replay: {files.Length} batches queued for resend");
                m_sendSignal.Set();
            }
        }

        // Flush pending events only while the timer's connection remains active.
        private void OnFlushTimer(object state)
        {
            try
            {
                lock (m_batchLock)
                {
                    if (m_stopSending || (state is CancellationToken token && token.IsCancellationRequested))
                        return;

                    if (m_pendingEvents is { Count: > 0 })
                        SwapAndSignal();
                }
            }
            catch (Exception ex)
            {
                OnProcessException(MessageLevel.Error, new IOException("Timer flush failed; events remain pending in memory.", ex));
            }
        }

        // Object pool: List<string> for pending events batches

        private List<string> RentList()
        {
            if (m_listPool != null && m_listPool.TryTake(out List<string> list))
            {
                list.Clear();
                return list;
            }

            return new List<string>(MaxBatchSize);
        }

        private void ReturnList(List<string> list)
        {
            if (list != null)
            {
                list.Clear();
                m_listPool?.Add(list);
            }
        }

        // Object pool: List<IMeasurement> for per-frame grouping

        private List<IMeasurement> RentFrameList()
        {
            if (m_frameListPool.TryTake(out List<IMeasurement> list))
                list.Clear();
            else
                list = new List<IMeasurement>();

            m_activeFrameLists.Add(list);
            return list;
        }

        private void ReturnFrameLists()
        {
            foreach (List<IMeasurement> list in m_activeFrameLists)
                m_frameListPool.Add(list);

            m_activeFrameLists.Clear();
        }

        #endregion

        #region [ Static ]

        // Static Fields

        private static readonly DateTime s_epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Static Methods

        /// <summary>
        /// Builds the Event Hub REST API batch body JSON from a list of event JSON strings.
        /// </summary>
        /// <param name="events">Serialized event bodies to include in the batch.</param>
        /// <returns>The REST batch envelope containing the escaped event bodies.</returns>
        internal static string BuildBatchBody(List<string> events)
        {
            StringBuilder sb = new StringBuilder(events.Count * 512);
            sb.Append('[');

            for (int i = 0; i < events.Count; i++)
            {
                if (i > 0)
                    sb.Append(',');

                sb.Append("{\"Body\":\"").Append(EscapeJsonString(events[i]))
                    .Append("\",\"BrokerProperties\":{\"ContentType\":\"application/json\"}}");
            }

            sb.Append(']');
            return sb.ToString();
        }

        /// <summary>
        /// Escapes text for inclusion in a JSON string value or property name.
        /// </summary>
        /// <param name="value">Text to escape.</param>
        /// <returns>The escaped text, or the original value when no escaping is required.</returns>
        internal static string EscapeJsonString(string value)
        {
            if (string.IsNullOrEmpty(value))
                return value;

            int escapeIndex = 0;
            while (escapeIndex < value.Length && value[escapeIndex] >= ' ' && value[escapeIndex] != '"' && value[escapeIndex] != '\\')
                escapeIndex++;

            if (escapeIndex == value.Length)
                return value;

            StringBuilder sb = new StringBuilder(value.Length + 4);
            foreach (char c in value)
            {
                switch (c)
                {
                    case '"':
                        sb.Append("\\\"");
                        break;
                    case '\\':
                        sb.Append("\\\\");
                        break;
                    case '\n':
                        sb.Append("\\n");
                        break;
                    case '\r':
                        sb.Append("\\r");
                        break;
                    case '\t':
                        sb.Append("\\t");
                        break;
                    default:
                        if (c < ' ')
                            sb.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                        else
                            sb.Append(c);
                        break;
                }
            }
            return sb.ToString();
        }

        #endregion
    }
}
