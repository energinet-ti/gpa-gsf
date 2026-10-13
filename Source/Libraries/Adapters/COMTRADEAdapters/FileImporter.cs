//******************************************************************************************************
//  FileImporter.cs - Gbtc
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
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using GSF;
using GSF.COMTRADE;
using GSF.Diagnostics;
using GSF.Threading;
using GSF.TimeSeries;
using GSF.TimeSeries.Adapters;
using GSF.Units.EE;
using Timer = System.Timers.Timer;

namespace COMTRADEAdapters
{
    /// <summary>
    /// Reads COMTRADE .cfg/.dat file pairs and publishes measurements into the GSF adapter pipeline.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This adapter operates in two modes depending on the connection string parameters:
    /// </para>
    /// <list type="bullet">
    ///   <item><description>
    ///     <b>Single-file mode</b> - Set <see cref="FileName"/> to a .cfg path. The adapter parses the
    ///     COMTRADE file pair, creates measurements from channel metadata (when <see cref="AutoCreateMeasurements"/>
    ///     is <c>true</c>), and replays samples at the configured <c>ProcessingInterval</c>.
    ///   </description></item>
    ///   <item><description>
    ///     <b>Directory watch mode</b> - Set <see cref="WatchDirectory"/> to a folder path. The adapter
    ///     monitors for new .cfg files, processes them sequentially, and optionally moves or deletes
    ///     completed files via <see cref="ProcessedDirectory"/> or <see cref="DeleteAfterProcessing"/>.
    ///   </description></item>
    /// </list>
    /// <para>
    /// The adapter progresses through the following states: <see cref="FileImporterState.Idle"/> to
    /// <see cref="FileImporterState.Loading"/> to <see cref="FileImporterState.Replaying"/> to
    /// <see cref="FileImporterState.Completed"/>. In watch mode, the cycle repeats for each queued file.
    /// The <see cref="GetShortStatus"/> method returns status beginning with "Completed" after replay
    /// and cleanup finish, allowing callers to poll for completion.
    /// </para>
    /// <para>
    /// Example connection string:
    /// <code>
    /// FileName=C:\Data\event.cfg; SimulateTimestamp=True; ProcessingInterval=0; AutoCreateMeasurements=True
    /// </code>
    /// </para>
    /// </remarks>
    [Description("COMTRADE: Reads measurements from COMTRADE .cfg/.dat files")]
    public class FileImporter : InputAdapterBase
    {
        #region [ Members ]

        // Constants
        private const int FileOpenRetries = 3;
        private const int FileOpenRetryDelayMs = 1000;
        private const int DefaultProcessingInterval = 33;

        // Connection string parameters
        private int m_framesPerTick = 1;

        // Runtime state
        private volatile FileImporterState m_state = FileImporterState.Idle;
        private string m_lastError;
        private Parser m_parser;
        private Schema m_schema;
        private Timer m_publishTimer;
        private bool m_publishStartRequested;
        private int m_pendingRoutingNotifications;
        private IMeasurement[] m_measurementTemplate;
        private long m_framesPublished;
        private long m_totalFrames;
        private Ticks m_replayStartTime;
        private string m_currentFileName;
        private string m_currentFilePath;
        private string m_currentDatPath;
        private string m_completedStatus;
        private string m_lastFileResult;
        private double[] m_sentinelValues;
        private readonly Dictionary<string, MeasurementKey> m_automaticMeasurementKeys = new Dictionary<string, MeasurementKey>(StringComparer.Ordinal);
        private readonly Dictionary<string, string> m_automaticTagIdentities = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        private bool m_disposed;
        private readonly object m_syncRoot = new object();
        private long m_generation;
        private bool m_connectionRequested;
        private List<Action> m_notifications;

        // Directory watch
        private FileSystemWatcher m_fileWatcher;
        private Queue<string> m_pendingFiles;
        private System.Threading.Timer m_settleTimer;
        private readonly Dictionary<string, DateTime> m_pendingDeadlines = new Dictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> m_seenFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private long m_watchGeneration;

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets or sets the path to the COMTRADE .cfg file.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Path to the COMTRADE .cfg file")]
        [DefaultValue("")]
        public string FileName { get; set; }

        /// <summary>
        /// Gets or sets the directory to monitor for new .cfg files.
        /// </summary>
        [ConnectionStringParameter]
        [Description("Directory to monitor for new .cfg files")]
        [DefaultValue("")]
        public string WatchDirectory { get; set; }

        /// <summary>
        /// Gets or sets the file filter for directory watch mode.
        /// </summary>
        [ConnectionStringParameter]
        [Description("File filter for directory watch mode")]
        [DefaultValue("*.cfg")]
        public string WatchFilter { get; set; } = "*.cfg";

        /// <summary>
        /// Gets or sets whether to replace original timestamps with current UTC time.
        /// </summary>
        [ConnectionStringParameter]
        [Description("When true, replaces original timestamps with current UTC time")]
        [DefaultValue(false)]
        public bool SimulateTimestamp { get; set; }

        /// <summary>
        /// Gets or sets whether to loop the file continuously on EOF.
        /// </summary>
        [ConnectionStringParameter]
        [Description("When true, loops the file continuously on EOF")]
        [DefaultValue(false)]
        public bool AutoRepeat { get; set; }

        /// <summary>
        /// Gets or sets whether to create measurement definitions from COMTRADE channel metadata.
        /// </summary>
        [ConnectionStringParameter]
        [Description("When true, creates measurement definitions from COMTRADE channel metadata")]
        [DefaultValue(true)]
        public bool AutoCreateMeasurements { get; set; } = true;

        /// <summary>
        /// Gets or sets the template for auto-created measurement tags.
        /// </summary>
        /// <remarks>
        /// Supported tokens: <c>{Adapter}</c> (adapter name), <c>{Channel}</c> (COMTRADE channel name),
        /// <c>{Index}</c> (zero-based channel index), <c>{Unit}</c> (engineering units or "Digital"),
        /// <c>{DeviceID}</c> (COMTRADE recording device IDCODE, e.g., "1001").
        /// </remarks>
        [ConnectionStringParameter]
        [Description("Template for auto-created measurement tags. Tokens: {Adapter}, {Channel}, {Index}, {Unit}, {DeviceID}")]
        [DefaultValue("{DeviceID}_{Channel}")]
        public string MeasurementTagTemplate { get; set; } = "{DeviceID}_{Channel}";

        /// <summary>
        /// Gets or sets whether to calculate timestamps from sample rate.
        /// </summary>
        [ConnectionStringParameter]
        [Description("When true, calculates timestamps from sample rate rather than per-sample timestamps")]
        [DefaultValue(true)]
        public bool InferTimeFromSampleRates { get; set; } = true;

        /// <summary>
        /// Gets or sets whether to delete files after successful replay.
        /// </summary>
        [ConnectionStringParameter]
        [Description("When true, deletes .cfg and .dat files after successful replay")]
        [DefaultValue(false)]
        public bool DeleteAfterProcessing { get; set; }

        /// <summary>
        /// Gets or sets the directory to move completed files to.
        /// </summary>
        [ConnectionStringParameter]
        [Description("If set, moves completed files to this directory instead of deleting them")]
        [DefaultValue("")]
        public string ProcessedDirectory { get; set; }

        /// <summary>
        /// Gets or sets the settle time in milliseconds for new files in watch mode.
        /// </summary>
        /// <remarks>
        /// When a new file is detected by the <see cref="FileSystemWatcher"/>, the adapter waits this
        /// duration before reading it, allowing the writing process to finish flushing data to disk.
        /// </remarks>
        [ConnectionStringParameter]
        [Description("Milliseconds to wait after detecting a new file before reading it")]
        [DefaultValue(2000)]
        public int FileSettleTimeMs { get; set; } = 2000;

        /// <summary>
        /// Gets or sets the number of COMTRADE frames to read per timer tick.
        /// </summary>
        /// <remarks>
        /// Higher values replay data faster by batching multiple samples into each timer invocation.
        /// For example, with 50 Hz data and <c>ProcessingInterval=0</c>, a value of 50 replays
        /// approximately one second of data per timer tick. Default is 1 (one frame per tick).
        /// </remarks>
        [ConnectionStringParameter]
        [Description("Number of COMTRADE frames to read per timer tick. Higher values increase replay speed.")]
        [DefaultValue(1)]
        public int FramesPerTick
        {
            get => m_framesPerTick;
            set => m_framesPerTick = value < 1 ? 1 : value;
        }

        /// <summary>
        /// Gets or sets whether to skip bad analog values (NaN, Infinity, and binary sentinels).
        /// </summary>
        /// <remarks>
        /// When <c>true</c> (the default), measurements with bad values are not published.
        /// This prevents NaN serialization issues in downstream adapters and filters out the
        /// -32768 (INT16_MIN) missing-data sentinel of COMTRADE Binary files.
        /// <see cref="Binary32UsesInt16Sentinel"/> selects the sentinel for Binary32 files.
        /// When <c>false</c>, all values are published as-is.
        /// </remarks>
        [ConnectionStringParameter]
        [Description("When true, skips NaN, Infinity, and configured binary sentinel values instead of publishing them")]
        [DefaultValue(true)]
        public bool SkipBadValues { get; set; } = true;

        /// <summary>
        /// Gets or sets whether Binary32 files mark missing data with the 16-bit INT16_MIN sentinel.
        /// </summary>
        /// <remarks>
        /// Enable for Binary32 files whose writer marks missing data with INT16_MIN (-32768).
        /// Disable for standard Binary32 files, where -32768 is valid and INT32_MIN is the missing-data sentinel. Only applies
        /// when <see cref="SkipBadValues"/> is enabled; Binary always uses INT16_MIN.
        /// </remarks>
        [ConnectionStringParameter]
        [Description("When true, Binary32 uses the 16-bit INT16_MIN sentinel; when false, uses INT32_MIN")]
        [DefaultValue(false)]
        public bool Binary32UsesInt16Sentinel { get; set; }

        /// <summary>
        /// Gets flag that determines if the data input connects asynchronously.
        /// </summary>
        protected override bool UseAsyncConnect => false;

        /// <summary>
        /// Gets flag indicating if this adapter supports temporal processing.
        /// </summary>
        public override bool SupportsTemporalProcessing => false;

        /// <summary>
        /// Returns the detailed status of the adapter.
        /// </summary>
        public override string Status
        {
            get
            {
                StringBuilder status = new StringBuilder();

                status.AppendLine($"            Current state: {m_state}");
                status.AppendLine($"             Current file: {m_currentFileName ?? "(none)"}");
                status.AppendLine($"          Watch directory: {WatchDirectory ?? "(none)"}");
                status.AppendLine($"      Processing interval: {ProcessingInterval}ms");
                status.AppendLine($"       Simulate timestamp: {SimulateTimestamp}");
                status.AppendLine($"              Auto repeat: {AutoRepeat}");
                status.AppendLine($" Auto create measurements: {AutoCreateMeasurements}");
                status.AppendLine($"    Infer time from rates: {InferTimeFromSampleRates}");
                status.AppendLine($"          Skip bad values: {SkipBadValues}");
                status.AppendLine($" Binary32 INT16 sentinel: {Binary32UsesInt16Sentinel}");
                status.AppendLine($"          Frames per tick: {m_framesPerTick}");

                if (m_state == FileImporterState.Replaying || m_state == FileImporterState.Completed)
                {
                    double pct = m_totalFrames > 0 ? (double)m_framesPublished / m_totalFrames * 100.0 : 0;
                    status.AppendLine($"         Frames published: {m_framesPublished:N0} / {m_totalFrames:N0} ({pct:F1}%)");
                }

                if (!string.IsNullOrEmpty(m_lastFileResult))
                    status.AppendLine($"         Last file result: {m_lastFileResult}");

                if (!string.IsNullOrEmpty(m_lastError))
                    status.AppendLine($"               Last error: {m_lastError}");

                status.Append(base.Status);

                return status.ToString();
            }
        }

        #endregion

        #region [ Methods ]

        #region [ Adapter Lifecycle ]

        /// <summary>
        /// Initializes the adapter from connection string parameters.
        /// </summary>
        public override void Initialize()
        {
            base.Initialize();
            ExecuteSynchronized(() => InitializeCore());
        }

        private void InitializeCore()
        {
            ThrowIfDisposed();
            m_generation++;

            Dictionary<string, string> settings = Settings;

            // Parse connection string parameters
            settings.TryGetValue(nameof(FileName), out string setting);
            FileName = setting?.Trim();

            settings.TryGetValue(nameof(WatchDirectory), out setting);
            WatchDirectory = setting?.Trim();

            WatchFilter = settings.TryGetValue(nameof(WatchFilter), out setting) && !string.IsNullOrWhiteSpace(setting)
                ? setting.Trim() : "*.cfg";

            if (settings.TryGetValue(nameof(SimulateTimestamp), out setting))
                SimulateTimestamp = setting.ParseBoolean();

            if (settings.TryGetValue(nameof(AutoRepeat), out setting))
                AutoRepeat = setting.ParseBoolean();

            AutoCreateMeasurements = !settings.TryGetValue(nameof(AutoCreateMeasurements), out setting) || setting.ParseBoolean();

            MeasurementTagTemplate = settings.TryGetValue(nameof(MeasurementTagTemplate), out setting) && !string.IsNullOrWhiteSpace(setting)
                ? setting.Trim() : "{DeviceID}_{Channel}";

            InferTimeFromSampleRates = !settings.TryGetValue(nameof(InferTimeFromSampleRates), out setting) || setting.ParseBoolean();

            if (settings.TryGetValue(nameof(DeleteAfterProcessing), out setting))
                DeleteAfterProcessing = setting.ParseBoolean();

            settings.TryGetValue(nameof(ProcessedDirectory), out setting);
            ProcessedDirectory = setting?.Trim();

            FileSettleTimeMs = settings.TryGetValue(nameof(FileSettleTimeMs), out setting)
                ? int.Parse(setting) : 2000;

            SkipBadValues = !settings.TryGetValue(nameof(SkipBadValues), out setting) || setting.ParseBoolean();
            Binary32UsesInt16Sentinel = settings.TryGetValue(nameof(Binary32UsesInt16Sentinel), out setting) && setting.ParseBoolean();

            m_framesPerTick = settings.TryGetValue(nameof(FramesPerTick), out setting) && int.TryParse(setting, out int fpt) && fpt >= 1
                ? fpt : 1;

            // Clean up any previous state
            StopPublishTimer();
            StopDirectoryWatch();
            CloseParser();

            // Reset counters
            m_framesPublished = 0;
            m_totalFrames = 0;
            m_lastError = null;
            m_completedStatus = null;

            // Initialize based on configuration
            if (!string.IsNullOrWhiteSpace(FileName))
            {
                LoadFile(FileName);
            }
            else
            {
                m_state = FileImporterState.Idle;
            }
        }

        /// <summary>
        /// Starts or restarts the adapter using the base connection timer.
        /// </summary>
        public override void Start()
        {
            ExecuteSynchronized(() =>
            {
                ThrowIfDisposed();
                // Base Start can call our Stop override when restarting an enabled adapter.
                base.Start();
                m_connectionRequested = true;
            });
        }

        /// <summary>
        /// Stops the adapter and cancels pending connection attempts.
        /// </summary>
        public override void Stop()
        {
            ExecuteSynchronized(() =>
            {
                m_connectionRequested = false;
                base.Stop();
            });
        }

        /// <summary>
        /// Attempts to connect the adapter (starts publishing or watching).
        /// </summary>
        protected override void AttemptConnection()
        {
            ExecuteSynchronized(() =>
            {
                if (m_disposed || !m_connectionRequested || !Enabled)
                    return;

                if (!string.IsNullOrWhiteSpace(FileName))
                {
                    if (m_parser is null || m_state != FileImporterState.Replaying)
                        LoadFile(FileName);

                    if (m_state == FileImporterState.Replaying)
                        StartPublishTimer();
                }
                else if (!string.IsNullOrWhiteSpace(WatchDirectory))
                {
                    StartDirectoryWatch();
                }
            });
        }

        /// <summary>
        /// Accepts a connection notification only while the connection is still requested.
        /// </summary>
        protected override void OnConnected()
        {
            ExecuteSynchronized(() =>
            {
                if (!m_disposed && m_connectionRequested && Enabled)
                    base.OnConnected();
            });
        }

        /// <summary>
        /// Attempts to disconnect the adapter, retaining its selected mode for a later start.
        /// </summary>
        protected override void AttemptDisconnection()
        {
            ExecuteSynchronized(() =>
            {
                m_connectionRequested = false;
                m_generation++;
                StopPublishTimer();
                StopDirectoryWatch();
                CloseParser();
                m_state = FileImporterState.Idle;
            });
        }

        #endregion

        #region [ Status and Commands ]

        /// <summary>
        /// Returns a short status message for the adapter.
        /// </summary>
        /// <param name="maxLength">Maximum number of characters in the returned status string.</param>
        /// <returns>
        /// A status string reflecting the current <see cref="FileImporterState"/>. The
        /// <see cref="FileImporterState.Completed"/> state always contains the literal string
        /// "Completed", allowing callers to poll for completion.
        /// </returns>
        public override string GetShortStatus(int maxLength)
        {
            lock (m_syncRoot)
                return GetShortStatusCore(maxLength);
        }

        private string GetShortStatusCore(int maxLength)
        {
            switch (m_state)
            {
                case FileImporterState.Idle:
                    return "Idle".TruncateRight(maxLength);

                case FileImporterState.Loading:
                    return $"Loading {m_currentFileName}".TruncateRight(maxLength);

                case FileImporterState.Replaying:
                    long published = Interlocked.Read(ref m_framesPublished);
                    double pct = m_totalFrames > 0 ? (double)published / m_totalFrames * 100.0 : 0;
                    return $"Replaying {m_currentFileName} - Frame {published}/{m_totalFrames} ({pct:F0}%)"
                        .TruncateRight(maxLength);

                case FileImporterState.Completed:
                    return (m_completedStatus ?? "Completed").TruncateRight(maxLength);

                case FileImporterState.Watching:
                    return ($"Watching {WatchDirectory}" + (string.IsNullOrEmpty(m_lastError) ? "" : $"; last error: {m_lastError}")).TruncateRight(maxLength);

                case FileImporterState.Error:
                    return $"Error - {m_lastError}".TruncateRight(maxLength);

                default:
                    return "Unknown state".TruncateRight(maxLength);
            }
        }

        /// <summary>
        /// Returns the current adapter status string.
        /// </summary>
        /// <returns>
        /// A status string reflecting the current <see cref="FileImporterState"/>. Contains the
        /// literal string "Completed" when replay finishes, so an external caller can poll for it.
        /// </returns>
        [AdapterCommand("Returns the current adapter status.", "Administrator", "Editor")]
        public string GetStatus()
        {
            return GetShortStatus(256);
        }

        /// <summary>
        /// Aborts the current operation and returns to <see cref="FileImporterState.Idle"/> state.
        /// </summary>
        /// <remarks>
        /// Stops the publishing timer, directory watcher, and closes the active parser.
        /// </remarks>
        [AdapterCommand("Stops the current replay and returns to Idle state.", "Administrator", "Editor")]
        public void Abort()
        {
            ExecuteSynchronized(() => AbortCore());
        }

        private void AbortCore()
        {
            m_connectionRequested = false;
            // Stop also cancels InputAdapterBase's delayed connection timer and invokes
            // AttemptDisconnection, preventing a pending Start from undoing the abort.
            base.Stop();
            OnStatusMessage(MessageLevel.Info, "Adapter aborted - now idle");
        }

        /// <summary>
        /// Loads and replays a COMTRADE file. Stops any in-progress replay first.
        /// </summary>
        /// <param name="cfgFilePath">Path to the .cfg file. The matching .dat file must be in the same directory.</param>
        /// <remarks>
        /// External callers start a replay with this command, for example:
        /// <code>INVOKE COMTRADE_IMPORT ReplayFile "C:\Recordings\event.cfg"</code>
        /// </remarks>
        [AdapterCommand("Loads and replays a COMTRADE .cfg file.", "Administrator", "Editor")]
        public void ReplayFile(string cfgFilePath)
        {
            ExecuteSynchronized(() => ReplayFileCore(cfgFilePath));
        }

        private void ReplayFileCore(string cfgFilePath)
        {
            ThrowIfDisposed();
            m_generation++;
            StopPublishTimer();
            StopDirectoryWatch();
            CloseParser();

            FileName = cfgFilePath;
            LoadFile(cfgFilePath);

            if (m_state == FileImporterState.Replaying)
                StartPublishTimer();
        }

        /// <summary>
        /// Returns the current COMTRADE-to-measurement channel mapping.
        /// </summary>
        /// <returns>
        /// A multi-line string listing each channel index, name, units, and mapped signal ID;
        /// or "No channel map loaded" if no schema is active.
        /// </returns>
        [AdapterCommand("Returns the current COMTRADE-to-measurement channel mapping.", "Administrator", "Editor")]
        public string GetChannelMap()
        {
            lock (m_syncRoot)
                return GetChannelMapCore();
        }

        private string GetChannelMapCore()
        {
            if (m_schema is null || m_measurementTemplate is null)
                return "No channel map loaded";

            StringBuilder sb = new StringBuilder();
            int index = 0;

            for (int i = 0; i < m_schema.TotalAnalogChannels; i++)
            {
                AnalogChannel channel = m_schema.AnalogChannels[i];
                string signalId = index < m_measurementTemplate.Length
                    ? m_measurementTemplate[index].Key.SignalID.ToString()
                    : "unmapped";
                sb.AppendLine($"{index}: {channel.Name} ({channel.Units}) -> {signalId}");
                index++;
            }

            for (int i = 0; i < m_schema.TotalDigitalChannels; i++)
            {
                DigitalChannel channel = m_schema.DigitalChannels[i];
                string signalId = index < m_measurementTemplate.Length
                    ? m_measurementTemplate[index].Key.SignalID.ToString()
                    : "unmapped";
                sb.AppendLine($"{index}: {channel.Name} (Digital) -> {signalId}");
                index++;
            }

            return sb.ToString();
        }

        #endregion

        #region [ File Loading ]

        /// <summary>
        /// Loads a COMTRADE .cfg file and prepares for replay.
        /// </summary>
        private void LoadFile(string cfgFilePath)
        {
            ExecuteSynchronized(() => LoadFileCore(cfgFilePath));
        }

        private void LoadFileCore(string cfgFilePath)
        {
            ThrowIfDisposed();
            m_generation++;
            StopPublishTimer();
            CloseParser();
            m_schema = null;
            m_measurementTemplate = null;
            m_lastError = null;
            m_framesPublished = 0;
            m_totalFrames = 0;
            m_state = FileImporterState.Loading;
            m_currentFilePath = cfgFilePath;
            m_currentDatPath = null;
            m_currentFileName = Path.GetFileName(cfgFilePath);
            m_completedStatus = null;

            OnStatusMessage(MessageLevel.Info, $"Loading {m_currentFileName}...");

            try
            {
                // Validate CFG file exists
                if (!File.Exists(cfgFilePath))
                    throw new FileNotFoundException($"CFG file not found: {cfgFilePath}");

                // Find the .dat file (check multiple case variants)
                string basePath = Path.Combine(
                    Path.GetDirectoryName(cfgFilePath) ?? "",
                    Path.GetFileNameWithoutExtension(cfgFilePath));

                string datFilePath = FindDatFile(basePath);

                if (datFilePath is null)
                    throw new FileNotFoundException($"DAT file not found: {basePath}.dat");

                m_currentDatPath = datFilePath;

                Schema schema = null;
                OpenWithRetry(() => schema = new Schema(cfgFilePath), cfgFilePath, "File");
                m_schema = schema;

                Parser parser = new Parser
                {
                    Schema = schema,
                    FileName = datFilePath,
                    InferTimeFromSampleRates = InferTimeFromSampleRates
                };

                m_parser = parser;
                OpenWithRetry(() =>
                {
                    // Release any segments opened by a previous, partially failed attempt.
                    parser.CloseFiles();
                    parser.OpenFiles();
                }, datFilePath, "Data file");

                // Build measurement map
                BuildMeasurementMap(schema);

                // Binary files mark missing data with INT16_MIN. Binary32 files use INT16_MIN or
                // INT32_MIN, as selected by Binary32UsesInt16Sentinel.
                if (schema.FileType == FileType.Binary || schema.FileType == FileType.Binary32)
                {
                    m_sentinelValues = new double[schema.TotalAnalogChannels];
                    double rawSentinel = schema.FileType == FileType.Binary32 && !Binary32UsesInt16Sentinel
                        ? int.MinValue : short.MinValue;

                    for (int i = 0; i < schema.TotalAnalogChannels; i++)
                    {
                        AnalogChannel channel = schema.AnalogChannels[i];
                        m_sentinelValues[i] = rawSentinel * channel.Multiplier + channel.Adder;
                    }
                }
                else
                {
                    m_sentinelValues = null;
                }

                // Set frame tracking
                m_totalFrames = schema.TotalSamples;
                m_framesPublished = 0;
                m_replayStartTime = DateTime.UtcNow.Ticks;

                m_state = FileImporterState.Replaying;

                int totalChannels = schema.TotalAnalogChannels + schema.TotalDigitalChannels;
                OnStatusMessage(MessageLevel.Info,
                    $"Loaded {m_currentFileName}: {totalChannels} channels, {m_totalFrames} samples");
            }
            catch (Exception ex)
            {
                CloseParser();
                m_state = FileImporterState.Error;
                m_lastError = ex is FileNotFoundException
                    ? ex.Message
                    : $"Failed to load COMTRADE file: {ex.Message}";
                m_lastFileResult = m_lastError;
                OnProcessException(MessageLevel.Error, new InvalidOperationException(m_lastError, ex));
            }
        }

        private void OpenWithRetry(Action open, string path, string description)
        {
            for (int attempt = 1; ; attempt++)
            {
                try
                {
                    open();
                    return;
                }
                catch (IOException ex)
                {
                    if (attempt == FileOpenRetries)
                        throw new IOException($"File locked after {FileOpenRetries} retries: {path}", ex);

                    OnStatusMessage(MessageLevel.Warning,
                        $"{description} locked (attempt {attempt}/{FileOpenRetries}), retrying in {FileOpenRetryDelayMs}ms...");
                    Thread.Sleep(FileOpenRetryDelayMs);
                }
            }
        }

        private long ReadTimestamp()
        {
            return SimulateTimestamp ? DateTime.UtcNow.Ticks : m_parser.Timestamp.Ticks;
        }

        /// <summary>
        /// Builds the measurement map from the COMTRADE schema.
        /// </summary>
        private void BuildMeasurementMap(Schema schema)
        {
            int totalChannels = schema.TotalAnalogChannels + schema.TotalDigitalChannels;

            if (AutoCreateMeasurements)
            {
                string[] identities = new string[totalChannels];
                string[] tags = new string[totalChannels];
                string[] suffixes = new string[totalChannels];
                string deviceId = schema.DeviceID ?? "";

                for (int i = 0; i < schema.TotalAnalogChannels; i++)
                {
                    AnalogChannel channel = schema.AnalogChannels[i];
                    string channelName = string.IsNullOrWhiteSpace(channel.ChannelName) ? channel.Name : channel.ChannelName;

                    if (channel.SignalKind == SignalKind.Magnitude)
                        channelName += "_Mag";
                    else if (channel.SignalKind == SignalKind.Angle)
                        channelName += "_Ang";

                    suffixes[i] = "A" + channel.Index.ToString(CultureInfo.InvariantCulture);
                    identities[i] = GetChannelIdentity(schema, "A" + channel.SignalKind + ":" + channel.PhaseID, channel.Name);
                    tags[i] = FormatMeasurementTag(deviceId, channelName, i, channel.Units);
                }

                for (int i = 0; i < schema.TotalDigitalChannels; i++)
                {
                    DigitalChannel channel = schema.DigitalChannels[i];
                    int index = schema.TotalAnalogChannels + i;
                    string channelName = string.IsNullOrWhiteSpace(channel.ChannelName) ? channel.Name : channel.ChannelName;
                    suffixes[index] = "D" + channel.Index.ToString(CultureInfo.InvariantCulture);
                    identities[index] = GetChannelIdentity(schema, "D:" + channel.PhaseID, channel.Name);
                    tags[index] = FormatMeasurementTag(deviceId, channelName, index, "Digital");
                }

                // Channel names identify recordings independently of their column order.
                // Only indistinguishable duplicate names require a channel-index tie-breaker.
                HashSet<string> duplicateIdentities = new HashSet<string>(identities
                    .GroupBy(identity => identity, StringComparer.Ordinal)
                    .Where(group => group.Count() > 1).Select(group => group.Key), StringComparer.Ordinal);

                for (int i = 0; i < identities.Length; i++)
                {
                    if (duplicateIdentities.Contains(identities[i]))
                        identities[i] += "\0" + suffixes[i];
                }

                // Reserve valid requested tags before generating suffixes so disambiguation
                // never steals an otherwise valid channel's name.
                HashSet<string> requestedTags = new HashSet<string>(tags, StringComparer.OrdinalIgnoreCase);
                HashSet<string> usedTags = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                IMeasurement[] measurements = new IMeasurement[totalChannels];

                for (int i = 0; i < totalChannels; i++)
                {
                    string tag = tags[i];
                    int duplicate = 0;

                    while (usedTags.Contains(tag) ||
                           m_automaticTagIdentities.TryGetValue(tag, out string owner) && owner != identities[i])
                    {
                        string candidate = tags[i] + "_" + suffixes[i];

                        if (++duplicate > 1)
                            candidate += "_" + duplicate.ToString(CultureInfo.InvariantCulture);

                        if (requestedTags.Contains(candidate))
                            continue;

                        tag = candidate;
                    }

                    usedTags.Add(tag);
                    MeasurementKey key = ResolveAutomaticMeasurement(identities[i], tag, (ulong)(i + 1));
                    measurements[i] = new Measurement { Metadata = key.Metadata.ChangeTagName(tag) };
                    m_automaticTagIdentities[tag] = identities[i];
                }

                m_measurementTemplate = measurements;

                // Avoid rebuilding host routes for another recording of unchanged channels.
                if (OutputMeasurements is null || OutputMeasurements.Length != measurements.Length ||
                    OutputMeasurements.Where((measurement, index) => measurement.Key != measurements[index].Key ||
                        measurement.TagName != measurements[index].TagName ||
                        measurement.Multiplier != measurements[index].Multiplier ||
                        measurement.Adder != measurements[index].Adder ||
                        measurement.Metadata.MeasurementValueFilter != measurements[index].Metadata.MeasurementValueFilter).Any())
                    OutputMeasurements = m_measurementTemplate;
            }
            else
            {
                // Pre-registered mode: use existing OutputMeasurements
                if (OutputMeasurements != null && OutputMeasurements.Length > 0)
                {
                    if (OutputMeasurements.Length != totalChannels)
                    {
                        int mapCount = Math.Min(totalChannels, OutputMeasurements.Length);
                        OnStatusMessage(MessageLevel.Warning,
                            $"Channel count mismatch: COMTRADE has {totalChannels} channels, " +
                            $"OutputMeasurements has {OutputMeasurements.Length}. " +
                            $"Mapping {mapCount} channels.");
                    }

                    m_measurementTemplate = OutputMeasurements;
                }
                else
                {
                    OnStatusMessage(MessageLevel.Warning,
                        "AutoCreateMeasurements is false and no OutputMeasurements defined. " +
                        "No measurements will be published.");
                    m_measurementTemplate = Array.Empty<IMeasurement>();
                }
            }
        }

        private string FormatMeasurementTag(string deviceId, string channelName, int index, string units)
        {
            return MeasurementTagTemplate
                .Replace("{Adapter}", Name)
                .Replace("{Channel}", channelName ?? "")
                .Replace("{Index}", index.ToString(CultureInfo.InvariantCulture))
                .Replace("{Unit}", units ?? "")
                .Replace("{DeviceID}", deviceId);
        }

        private static string GetChannelIdentity(Schema schema, string channelType, string channelName)
        {
            // Recording-device codes identify COMTRADE sources, not host database devices.
            return string.Join("\0", schema.StationName, schema.DeviceID, channelType, channelName);
        }

        private MeasurementKey ResolveAutomaticMeasurement(string identity, string tag, ulong preferredID)
        {
            MeasurementKey key = FindHostMeasurement(tag);

            if (key != MeasurementKey.Undefined)
            {
                if (m_automaticMeasurementKeys.Any(entry => entry.Key != identity && entry.Value == key))
                    throw new InvalidOperationException($"Host measurement '{tag}' is already mapped to another COMTRADE channel.");

                m_automaticMeasurementKeys[identity] = key;
                return key;
            }

            if (m_automaticMeasurementKeys.TryGetValue(identity, out key))
                return key;

            Guid signalID = GetRuntimeSignalID(identity);
            key = MeasurementKey.LookUpBySignalID(signalID);

            if (key != MeasurementKey.Undefined)
            {
                m_automaticMeasurementKeys.Add(identity, key);
                return key;
            }

            // Preserve Name:channel keys for the first recording. Never overwrite keys
            // already claimed by another recording or registered by the host.
            ulong id = preferredID;
            key = MeasurementKey.LookUpBySource(Name, id);

            if (key != MeasurementKey.Undefined &&
                string.Equals(key.Metadata.TagName, tag, StringComparison.OrdinalIgnoreCase) &&
                !m_automaticMeasurementKeys.ContainsValue(key))
            {
                m_automaticMeasurementKeys[identity] = key;
                return key;
            }

            while (key != MeasurementKey.Undefined)
                key = MeasurementKey.LookUpBySource(Name, ++id);

            key = MeasurementKey.CreateOrUpdate(signalID, Name, id);
            m_automaticMeasurementKeys.Add(identity, key);
            return key;
        }

        private Guid GetRuntimeSignalID(string identity)
        {
            // Runtime identities remain stable when an adapter is reconstructed, without
            // requiring database writes. Registered host identities always take precedence.
            using (SHA256 hash = SHA256.Create())
            {
                byte[] digest = hash.ComputeHash(Encoding.UTF8.GetBytes("COMTRADEAdapters.FileImporter\0" + Name + "\0" + identity));
                byte[] guid = new byte[16];
                Array.Copy(digest, guid, guid.Length);
                return new Guid(guid);
            }
        }

        private MeasurementKey FindHostMeasurement(string tag)
        {
            DataTable measurements = DataSource?.Tables["ActiveMeasurements"];

            if (measurements is null || !measurements.Columns.Contains("PointTag") ||
                !measurements.Columns.Contains("SignalID") || !measurements.Columns.Contains("ID"))
                return MeasurementKey.Undefined;

            DataRow match = null;

            foreach (DataRow row in measurements.Rows)
            {
                if (row.RowState == DataRowState.Deleted ||
                    !string.Equals(Convert.ToString(row["PointTag"], CultureInfo.InvariantCulture), tag, StringComparison.OrdinalIgnoreCase))
                    continue;

                if (match != null)
                    throw new InvalidOperationException($"Host metadata contains multiple measurements for point tag '{tag}'.");

                match = row;
            }

            if (match is null)
                return MeasurementKey.Undefined;

            if (!Guid.TryParse(Convert.ToString(match["SignalID"], CultureInfo.InvariantCulture), out Guid signalID) || signalID == Guid.Empty)
                throw new InvalidOperationException($"Host metadata has no valid signal ID for point tag '{tag}'.");

            MeasurementKey key = MeasurementKey.LookUpOrCreate(signalID, Convert.ToString(match["ID"], CultureInfo.InvariantCulture));

            if (key == MeasurementKey.Undefined)
                throw new InvalidOperationException($"Host metadata has no valid measurement key for point tag '{tag}'.");

            // Reusing the host SignalID also retains its existing Device association in
            // ActiveMeasurements. Runtime-only signals intentionally do not invent one.
            return key;
        }

        #endregion

        #region [ Frame Publishing ]

        /// <summary>
        /// Publishes the next frame from the COMTRADE parser.
        /// </summary>
        private void PublishNextFrame()
        {
            PublishFrameBatch(null);
        }

        private void PublishFrameBatch(long? expectedGeneration)
        {
            ExecuteSynchronized(() =>
            {
                if (m_disposed || m_state != FileImporterState.Replaying || m_parser is null ||
                    (expectedGeneration.HasValue && expectedGeneration.Value != m_generation))
                    return;

                long generation = m_generation;
                int channelCount = m_measurementTemplate.Length;
                List<IMeasurement> measurements = new List<IMeasurement>();
                int framesRead = 0;
                bool endOfFile = false;
                Exception failure = null;

                try
                {
                    for (int frame = 0; frame < m_framesPerTick; frame++)
                    {
                        if (!m_parser.ReadNext())
                        {
                            endOfFile = true;
                            break;
                        }

                        long timestamp = ReadTimestamp();
                        double[] values = m_parser.Values;
                        int count = Math.Min(values.Length, channelCount);

                        for (int i = 0; i < count; i++)
                        {
                            double value = values[i];

                            if (SkipBadValues && IsBadValue(value, i))
                                continue;

                            measurements.Add(new Measurement
                            {
                                Metadata = m_measurementTemplate[i].Metadata,
                                Timestamp = timestamp,
                                Value = value
                            });
                        }

                        framesRead++;
                    }
                }
                catch (Exception ex)
                {
                    failure = ex;
                }

                // Deliver valid records even if a later record in this batch is malformed. The
                // continuation rechecks its generation after subscribers run lifecycle commands.
                m_notifications.Add(() =>
                {
                    lock (m_syncRoot)
                    {
                        if (m_disposed || generation != m_generation)
                            return;

                        Interlocked.Add(ref m_framesPublished, framesRead);
                    }

                    if (measurements.Count > 0)
                        OnNewMeasurements(measurements);

                    ExecuteSynchronized(() =>
                    {
                        if (m_disposed || generation != m_generation)
                            return;

                        if (failure != null)
                            HandleReadFailure(failure);
                        else if (endOfFile)
                            HandleEndOfFile();
                    });
                });
            });
        }

        private void HandleReadFailure(Exception exception)
        {
            StopPublishTimer();
            CloseParser();
            m_state = FileImporterState.Error;
            m_lastError = $"Replay failed for {m_currentFileName} after {m_framesPublished} of {m_totalFrames} frames: {exception.Message}";
            m_lastFileResult = m_lastError;
            OnProcessException(MessageLevel.Error, new InvalidOperationException(m_lastError, exception));

            if (m_fileWatcher != null)
                ProcessNextPendingFile();
        }

        /// <summary>
        /// Returns <c>true</c> if the value should be skipped: NaN, Infinity,
        /// or the configured scaled sentinel for analog channels in Binary and Binary32 formats.
        /// </summary>
        private bool IsBadValue(double value, int channelIndex)
        {
            if (double.IsNaN(value) || double.IsInfinity(value))
                return true;

            // For Binary/Binary32 files, check the pre-computed sentinel for analog channels
            if (m_sentinelValues != null && channelIndex < m_sentinelValues.Length)
            {
                // Use bit-exact comparison: sentinel values are computed from the same
                // integer arithmetic the parser uses, so no rounding tolerance is needed.
                // ReSharper disable once CompareOfFloatsByEqualityOperator
                return value == m_sentinelValues[channelIndex];
            }

            return false;
        }

        /// <summary>
        /// Handles the end-of-file condition after all samples have been read.
        /// </summary>
        private void HandleEndOfFile()
        {
            StopPublishTimer();
            long framesPublished = Interlocked.Read(ref m_framesPublished);
            bool truncated = m_totalFrames > 0 && framesPublished < m_totalFrames;

            if (truncated)
                OnStatusMessage(MessageLevel.Warning, $"{framesPublished} of {m_totalFrames} frames published (truncated)");

            CloseParser();

            if (AutoRepeat && m_fileWatcher is null)
            {
                LoadFile(m_currentFilePath);

                if (m_state == FileImporterState.Replaying)
                    StartPublishTimer();

                return;
            }

            // A truncated recording is cleaned up like a complete one.
            // Finish cleanup before exposing Completed to polling clients.
            string cleanupError = PostProcessFile();
            Ticks elapsed = DateTime.UtcNow.Ticks - m_replayStartTime;
            m_completedStatus = $"Completed {m_currentFileName} - {framesPublished} of {m_totalFrames} frames published in {elapsed.ToElapsedTimeString(2)}";

            if (truncated)
                m_completedStatus += " (truncated)";

            if (cleanupError != null)
                m_completedStatus += $"; cleanup failed: {cleanupError}";

            m_lastFileResult = m_completedStatus;
            m_state = FileImporterState.Completed;
            OnStatusMessage(cleanupError is null ? MessageLevel.Info : MessageLevel.Warning, m_completedStatus);

            if (m_fileWatcher != null)
                ProcessNextPendingFile();
            else
            {
                long generation = m_generation;
                m_notifications.Add(() =>
                {
                    if (Interlocked.Read(ref m_generation) == generation && !m_disposed)
                        OnProcessingComplete();
                });
            }
        }

        #endregion

        #region [ Timer Management ]

        private void StartPublishTimer()
        {
            if (m_publishTimer != null)
                return;

            long generation = m_generation;
            LongSynchronizedOperation operation = new LongSynchronizedOperation(
                () => PublishFrameBatch(generation), ex => OnProcessException(MessageLevel.Warning, ex));

            int interval = ProcessingInterval;

            if (interval <= 0)
                interval = interval == 0 ? 1 : DefaultProcessingInterval;

            m_publishStartRequested = false;
            m_publishTimer = new Timer(interval);
            m_publishTimer.Elapsed += (sender, args) => operation.RunOnce();
            m_publishTimer.AutoReset = true;
            Timer timer = m_publishTimer;
            Action start = () => ExecuteSynchronized(() =>
            {
                if (!m_disposed && generation == m_generation && ReferenceEquals(timer, m_publishTimer))
                {
                    m_publishStartRequested = true;
                    TryStartPublishTimer();
                }
            });

            if (Monitor.IsEntered(m_syncRoot))
                m_notifications.Add(start);
            else
                start();
        }

        // Called under m_syncRoot after a requested timer start or routing notification.
        private void TryStartPublishTimer()
        {
            if (!m_disposed && m_publishStartRequested && m_pendingRoutingNotifications == 0 &&
                m_state == FileImporterState.Replaying)
                m_publishTimer?.Start();
        }

        private void StopPublishTimer()
        {
            m_publishStartRequested = false;
            Timer timer = m_publishTimer;

            if (timer != null)
            {
                timer.Stop();
                timer.Dispose();
                m_publishTimer = null;
            }
        }

        #endregion

        #region [ Directory Watch ]

        /// <summary>
        /// Starts monitoring a directory for new COMTRADE files.
        /// </summary>
        private void StartDirectoryWatch()
        {
            if (string.IsNullOrWhiteSpace(WatchDirectory))
                return;

            StopDirectoryWatch();

            if (!Directory.Exists(WatchDirectory))
            {
                m_state = FileImporterState.Error;
                m_lastError = $"Watch directory not found: {WatchDirectory}";
                OnProcessException(MessageLevel.Error, new DirectoryNotFoundException(m_lastError));
                return;
            }

            try
            {
                m_pendingFiles = new Queue<string>();

                // Observe DAT writes as well as CFG delivery. Subscribe and enable before scanning so
                // files delivered during the scan cannot fall between the scan and watcher startup.
                m_fileWatcher = new FileSystemWatcher(WatchDirectory, "*")
                {
                    NotifyFilter = NotifyFilters.FileName | NotifyFilters.LastWrite | NotifyFilters.Size
                };
                m_fileWatcher.Created += OnFileCreated;
                m_fileWatcher.Changed += OnFileCreated;
                m_fileWatcher.Renamed += OnFileRenamed;
                m_fileWatcher.EnableRaisingEvents = true;

                string[] existingFiles = Directory.GetFiles(WatchDirectory, WatchFilter)
                    .OrderBy(file => file, StringComparer.OrdinalIgnoreCase)
                    .ToArray();

                foreach (string file in existingFiles)
                    QueuePendingFile(file, false);

                m_state = FileImporterState.Watching;
                OnStatusMessage(MessageLevel.Info,
                    $"Watching {WatchDirectory} ({existingFiles.Length} existing files queued)");

                ProcessNextPendingFile();
            }
            catch (Exception ex)
            {
                StopDirectoryWatch();
                m_state = FileImporterState.Error;
                m_lastError = $"Failed to watch {WatchDirectory}: {ex.Message}";
                OnProcessException(MessageLevel.Error, new InvalidOperationException(m_lastError, ex));
            }
        }

        private void OnFileCreated(object sender, FileSystemEventArgs e)
        {
            ExecuteSynchronized(() =>
            {
                if (m_disposed || m_pendingFiles is null || !ReferenceEquals(sender, m_fileWatcher))
                    return;

                if (string.Equals(Path.GetExtension(e.FullPath), ".dat", StringComparison.OrdinalIgnoreCase))
                {
                    string basePath = Path.ChangeExtension(Path.GetFullPath(e.FullPath), null);

                    foreach (string cfg in m_pendingDeadlines.Keys.Where(path =>
                        string.Equals(Path.ChangeExtension(path, null), basePath, StringComparison.OrdinalIgnoreCase)).ToArray())
                        m_pendingDeadlines[cfg] = DateTime.UtcNow.AddMilliseconds(Math.Max(0, FileSettleTimeMs));
                }
                else if (MatchesWatchFilter(e.Name))
                {
                    QueuePendingFile(e.FullPath, true);
                }

                SchedulePendingFiles();
            });
        }

        private void OnFileRenamed(object sender, RenamedEventArgs e)
        {
            OnFileCreated(sender, e);
        }

        private bool MatchesWatchFilter(string fileName)
        {
            string expression = "^" + System.Text.RegularExpressions.Regex.Escape(WatchFilter)
                .Replace("\\*", ".*").Replace("\\?", ".") + "$";

            return System.Text.RegularExpressions.Regex.IsMatch(fileName, expression,
                System.Text.RegularExpressions.RegexOptions.IgnoreCase | System.Text.RegularExpressions.RegexOptions.CultureInvariant);
        }

        private void QueuePendingFile(string filePath, bool newlyObserved)
        {
            filePath = Path.GetFullPath(filePath);
            DateTime readyAt = newlyObserved ? DateTime.UtcNow : File.GetLastWriteTimeUtc(filePath);
            string datPath = FindDatFile(Path.ChangeExtension(filePath, null));

            if (datPath != null)
            {
                DateTime datWriteTime = File.GetLastWriteTimeUtc(datPath);

                if (datWriteTime > readyAt)
                    readyAt = datWriteTime;
            }

            readyAt = readyAt.AddMilliseconds(Math.Max(0, FileSettleTimeMs));

            if (m_pendingDeadlines.TryGetValue(filePath, out DateTime pending))
            {
                if (readyAt > pending)
                    m_pendingDeadlines[filePath] = readyAt;

                return;
            }

            // Keep completed/failed paths in the session's set too: duplicate delivery events must
            // not replay an already consumed file when file retention is enabled.
            if (!m_seenFiles.Add(filePath))
                return;

            m_pendingDeadlines.Add(filePath, readyAt);
            m_pendingFiles.Enqueue(filePath);

            if (newlyObserved)
                OnStatusMessage(MessageLevel.Info, $"New file detected: {Path.GetFileName(filePath)}");
        }

        /// <summary>
        /// Processes the next ready pair, retaining incomplete pairs without blocking other files.
        /// </summary>
        private void ProcessNextPendingFile()
        {
            if (m_pendingFiles is null || m_disposed)
                return;

            m_state = FileImporterState.Watching;
            int pendingCount = m_pendingFiles.Count;

            while (pendingCount-- > 0 && m_pendingFiles.Count > 0)
            {
                string nextFile = m_pendingFiles.Dequeue();

                if (!m_pendingDeadlines.TryGetValue(nextFile, out DateTime pending))
                {
                    pending = DateTime.MinValue;
                    m_pendingDeadlines.Add(nextFile, pending);
                    m_seenFiles.Add(nextFile);
                }

                DateTime now = DateTime.UtcNow;
                string datPath = FindDatFile(Path.ChangeExtension(nextFile, null));

                if (!File.Exists(nextFile) || datPath is null)
                {
                    // A single timer retries missing pairs at a bounded cadence. DAT delivery
                    // resets this deadline and applies the full settle interval to that pair.
                    m_pendingDeadlines[nextFile] = now.AddMilliseconds(Math.Max(250, Math.Min(1000, FileSettleTimeMs)));
                    m_pendingFiles.Enqueue(nextFile);
                    continue;
                }

                DateTime latestWrite = File.GetLastWriteTimeUtc(nextFile);
                DateTime datWrite = File.GetLastWriteTimeUtc(datPath);

                if (datWrite > latestWrite)
                    latestWrite = datWrite;

                DateTime readyAt = latestWrite.AddMilliseconds(Math.Max(0, FileSettleTimeMs));

                if (readyAt > pending)
                    m_pendingDeadlines[nextFile] = pending = readyAt;

                if (pending > now)
                {
                    m_pendingFiles.Enqueue(nextFile);
                    continue;
                }

                m_pendingDeadlines.Remove(nextFile);
                LoadFile(nextFile);

                if (m_state == FileImporterState.Replaying)
                {
                    StartPublishTimer();
                    return;
                }

                if (m_state != FileImporterState.Error)
                    return;

                OnStatusMessage(MessageLevel.Warning, $"Skipping {Path.GetFileName(nextFile)}: {m_lastError}");
                m_state = FileImporterState.Watching;
            }

            SchedulePendingFiles();
        }

        private void SchedulePendingFiles()
        {
            m_settleTimer?.Dispose();
            m_settleTimer = null;

            if (m_fileWatcher is null || m_state != FileImporterState.Watching || m_pendingDeadlines.Count == 0)
                return;

            DateTime nextReady = m_pendingDeadlines.Values.Min();
            int delay = (int)Math.Min(int.MaxValue, Math.Max(1, Math.Ceiling((nextReady - DateTime.UtcNow).TotalMilliseconds)));
            long generation = m_watchGeneration;
            FileSystemWatcher watcher = m_fileWatcher;

            m_settleTimer = new System.Threading.Timer(state => ExecuteSynchronized(() =>
            {
                if (m_disposed || generation != m_watchGeneration || !ReferenceEquals(watcher, m_fileWatcher))
                    return;

                if (m_state == FileImporterState.Watching)
                    ProcessNextPendingFile();
            }), null, delay, Timeout.Infinite);
        }

        private void StopDirectoryWatch()
        {
            m_watchGeneration++;
            FileSystemWatcher watcher = m_fileWatcher;
            m_fileWatcher = null;
            m_settleTimer?.Dispose();
            m_settleTimer = null;
            m_pendingFiles = null;
            m_pendingDeadlines.Clear();
            m_seenFiles.Clear();

            if (watcher != null)
            {
                watcher.EnableRaisingEvents = false;
                watcher.Created -= OnFileCreated;
                watcher.Changed -= OnFileCreated;
                watcher.Renamed -= OnFileRenamed;
                watcher.Dispose();
            }
        }

        #endregion

        #region [ File Post-Processing ]

        /// <summary>
        /// Moves or deletes the current file after successful replay, returning any cleanup error.
        /// ProcessedDirectory takes precedence over DeleteAfterProcessing.
        /// </summary>
        private string PostProcessFile()
        {
            if (string.IsNullOrWhiteSpace(m_currentFilePath))
                return null;

            string cfgPath = m_currentFilePath;
            string fileName = Path.GetFileName(cfgPath);

            try
            {
                string datPath = m_currentDatPath ?? FindDatFile(Path.ChangeExtension(cfgPath, null));

                if (!string.IsNullOrWhiteSpace(ProcessedDirectory))
                {
                    string destCfg = Path.Combine(ProcessedDirectory, Path.GetFileName(cfgPath));
                    string destDat = datPath is null ? null : Path.Combine(ProcessedDirectory, Path.GetFileName(datPath));

                    // Check both destinations before moving either source, so a DAT collision
                    // cannot leave a recording split between the input and processed folders.
                    if (File.Exists(destCfg) || Directory.Exists(destCfg))
                        throw new IOException($"Destination already exists: {destCfg}");

                    if (destDat != null && (File.Exists(destDat) || Directory.Exists(destDat)))
                        throw new IOException($"Destination already exists: {destDat}");

                    Directory.CreateDirectory(ProcessedDirectory);

                    bool movedConfiguration = false;

                    try
                    {
                        if (File.Exists(cfgPath))
                        {
                            File.Move(cfgPath, destCfg);
                            movedConfiguration = true;
                        }

                        if (datPath != null && File.Exists(datPath))
                            File.Move(datPath, destDat);
                    }
                    catch (Exception moveError)
                    {
                        // Pair moves are not atomic. Restore the CFG when the DAT move fails
                        // so a recoverable error does not needlessly split the input pair.
                        if (movedConfiguration)
                        {
                            try
                            {
                                File.Move(destCfg, cfgPath);
                            }
                            catch (Exception rollbackError)
                            {
                                throw new IOException($"{moveError.Message}; could not restore CFG: {rollbackError.Message}", moveError);
                            }
                        }

                        throw;
                    }

                    OnStatusMessage(MessageLevel.Info, $"Moved {fileName} to {ProcessedDirectory}");
                }
                else if (DeleteAfterProcessing)
                {
                    if (File.Exists(cfgPath))
                        File.Delete(cfgPath);

                    if (datPath != null && File.Exists(datPath))
                        File.Delete(datPath);

                    OnStatusMessage(MessageLevel.Info, $"Deleted {fileName}");
                }

                return null;
            }
            catch (Exception ex)
            {
                string error = $"Failed to post-process {fileName}: {ex.Message}";
                OnProcessException(MessageLevel.Warning, new InvalidOperationException(error, ex));
                return error;
            }
        }

        /// <summary>
        /// Finds the .dat file matching a base path, checking multiple case variants.
        /// </summary>
        internal static string FindDatFile(string basePath)
        {
            string[] extensions = { ".dat", ".DAT", ".Dat" };

            foreach (string ext in extensions)
            {
                string path = basePath + ext;

                if (File.Exists(path))
                    return path;
            }

            return null;
        }

        #endregion

        #region [ Cleanup ]

        private void CloseParser()
        {
            Parser parser = m_parser;
            m_parser = null;

            try
            {
                // Parser.Dispose closes its files, including partially opened segments.
                parser?.Dispose();
            }
            catch
            {
                // Preserve the original replay result if parser cleanup fails.
            }
        }

        /// <summary>
        /// Releases the unmanaged resources used by the <see cref="FileImporter"/> and optionally releases the managed resources.
        /// </summary>
        /// <param name="disposing"><c>true</c> to release both managed and unmanaged resources; <c>false</c> to release only unmanaged resources.</param>
        protected override void Dispose(bool disposing)
        {
            bool disposeBase = false;

            try
            {
                ExecuteSynchronized(() =>
                {
                    if (m_disposed)
                        return;

                    m_disposed = true;
                    m_generation++;
                    disposeBase = true;

                    if (disposing)
                    {
                        StopPublishTimer();
                        StopDirectoryWatch();
                        CloseParser();
                        m_state = FileImporterState.Idle;
                    }
                });
            }
            finally
            {
                if (disposeBase)
                    base.Dispose(disposing);
            }
        }

        /// <summary>
        /// Notifies the host of output mapping changes outside the replay state lock.
        /// </summary>
        protected override void OnOutputMeasurementsUpdated()
        {
            ExecuteSynchronized(() =>
            {
                m_pendingRoutingNotifications++;
                m_notifications.Add(() =>
                {
                    try
                    {
                        base.OnOutputMeasurementsUpdated();
                    }
                    finally
                    {
                        ExecuteSynchronized(() =>
                        {
                            m_pendingRoutingNotifications--;
                            TryStartPublishTimer();
                        });
                    }
                });
            });
        }

        private void ThrowIfDisposed()
        {
            if (m_disposed)
                throw new ObjectDisposedException(nameof(FileImporter));
        }

        // Keep parser, queue and lifecycle mutations serialized. User/host event handlers are
        // dispatched after releasing the lock so they can call commands, even on another thread.
        private void ExecuteSynchronized(Action action)
        {
            if (Monitor.IsEntered(m_syncRoot))
            {
                action();
                return;
            }

            List<Action> notifications;

            lock (m_syncRoot)
            {
                m_notifications = new List<Action>();

                try
                {
                    action();
                    notifications = m_notifications;
                }
                finally
                {
                    m_notifications = null;
                }
            }

            foreach (Action notification in notifications)
                notification();
        }

        /// <summary>
        /// Dispatches framework status callbacks outside the replay state lock.
        /// </summary>
        /// <param name="level">Severity level of the status message.</param>
        /// <param name="status">Status message to publish.</param>
        /// <param name="eventName">Optional name identifying the event.</param>
        /// <param name="flags">Flags associated with the message.</param>
        protected override void OnStatusMessage(MessageLevel level, string status, string eventName = null, MessageFlags flags = MessageFlags.None)
        {
            if (Monitor.IsEntered(m_syncRoot))
                m_notifications.Add(() => base.OnStatusMessage(level, status, eventName, flags));
            else
                base.OnStatusMessage(level, status, eventName, flags);
        }

        /// <summary>
        /// Dispatches framework exception callbacks outside the replay state lock.
        /// </summary>
        /// <param name="level">Severity level of the exception.</param>
        /// <param name="exception">Exception to publish.</param>
        /// <param name="eventName">Optional name identifying the event.</param>
        /// <param name="flags">Flags associated with the exception.</param>
        protected override void OnProcessException(MessageLevel level, Exception exception, string eventName = null, MessageFlags flags = MessageFlags.None)
        {
            if (Monitor.IsEntered(m_syncRoot))
                m_notifications.Add(() => base.OnProcessException(level, exception, eventName, flags));
            else
                base.OnProcessException(level, exception, eventName, flags);
        }

        #endregion

        #endregion
    }

    /// <summary>
    /// Defines the operational states of the <see cref="FileImporter"/>.
    /// </summary>
    internal enum FileImporterState
    {
        /// <summary>No file assigned.</summary>
        Idle,
        /// <summary>Parsing CFG/DAT files.</summary>
        Loading,
        /// <summary>Publishing measurement frames.</summary>
        Replaying,
        /// <summary>All samples published.</summary>
        Completed,
        /// <summary>Monitoring directory for new files.</summary>
        Watching,
        /// <summary>An error occurred.</summary>
        Error
    }
}
