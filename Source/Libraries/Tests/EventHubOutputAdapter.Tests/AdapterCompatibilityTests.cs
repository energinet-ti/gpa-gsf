//******************************************************************************************************
//  AdapterCompatibilityTests.cs - Gbtc
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
//  10/07/2026 - Frederik Fast
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
using System.Globalization;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Web.Script.Serialization;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Tests routing, JSON serialization and configuration validation.
    /// </summary>
    [TestClass]
    public class AdapterCompatibilityTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves dynamic names and separate grouping for each processing callback.
        /// </summary>
        [TestMethod]
        public void ProcessMeasurementsPreservesDynamicNamesAndCallbackGrouping()
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.DevicePrefixDelimiter = "_";
            adapter.StripDevicePrefix = true;
            adapter.StripPointIdSuffix = true;
            adapter.ParseSourceFilter("PMU_*");
            adapter.SignalLookup = new Dictionary<MeasurementKey, EventHubOutputAdapter.SignalInfo>();
            SetField(adapter, "m_framesDict", new Dictionary<(string, long), List<IMeasurement>>());
            SetField(adapter, "m_activeFrameLists", new List<List<IMeasurement>>());
            SetField(adapter, "m_frameListPool", new ConcurrentBag<List<IMeasurement>>());
            List<string> pending = new List<string>();
            SetField(adapter, "m_pendingEvents", pending);
            long timestamp = new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc).Ticks;
            IMeasurement[] measurements =
            {
                new Measurement { Metadata = MeasurementKey.CreateOrUpdate(Guid.NewGuid(), "PMU_1001", 1).Metadata.ChangeTagName("1001_FQ:1"), Value = 50.0D, Timestamp = timestamp },
                new Measurement { Metadata = MeasurementKey.CreateOrUpdate(Guid.NewGuid(), "PMU_1001", 2).Metadata.ChangeTagName("1001_DF:2"), Value = 0.25D, Timestamp = timestamp },
                new Measurement { Metadata = MeasurementKey.CreateOrUpdate(Guid.NewGuid(), "STAT", 1).Metadata.ChangeTagName("STAT_Counter"), Value = 1.0D, Timestamp = timestamp }
            };
            MethodInfo process = typeof(EventHubOutputAdapter).GetMethod("ProcessMeasurements", BindingFlags.Instance | BindingFlags.NonPublic);

            try
            {
                process.Invoke(adapter, new object[] { measurements });
                process.Invoke(adapter, new object[] { new IMeasurement[] { measurements[0] } });

                Assert.AreEqual(2, pending.Count, "Separate callbacks must retain separate event grouping.");
                JavaScriptSerializer parser = new JavaScriptSerializer();
                Dictionary<string, object> first = parser.Deserialize<Dictionary<string, object>>(pending[0]);
                Dictionary<string, object> second = parser.Deserialize<Dictionary<string, object>>(pending[1]);
                Assert.AreEqual("1001", first["DeviceID"]);
                Assert.AreEqual(2, first["MeasurementCount"]);
                Assert.AreEqual(1, second["MeasurementCount"]);
                Dictionary<string, object> values = (Dictionary<string, object>)first["Measurements"];
                Assert.AreEqual(50.0D, Convert.ToDouble(values["FQ"], CultureInfo.InvariantCulture));
                Assert.AreEqual(0.25D, Convert.ToDouble(values["DF"], CultureInfo.InvariantCulture));
            }
            finally
            {
                // This fixture initialized callback state only, without opening a WAL or connection.
                SetField(adapter, "m_pendingEvents", null);
            }
        }

        /// <summary>
        /// Preserves the selected routing mode when framework metadata is refreshed.
        /// </summary>
        /// <param name="broadcastMode">Whether broadcast routing was selected at initialization.</param>
        [DataTestMethod]
        [DataRow(true)]
        [DataRow(false)]
        public void RefreshMetadataPreservesPreviouslySelectedRoutingMode(bool broadcastMode)
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.ConnectionString = string.Empty;
            typeof(EventHubOutputAdapter).GetField("m_broadcastMode", BindingFlags.Instance | BindingFlags.NonPublic)
                .SetValue(adapter, broadcastMode);
            adapter.InputMeasurementKeys = null;

            // RefreshMetadata must not replace broadcast routing (null input keys) with an empty array.
            adapter.RefreshMetadata();

            if (broadcastMode)
            {
                Assert.IsNull(adapter.InputMeasurementKeys);

                MeasurementKey key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "PMU_TEST", 1);
                adapter.InputMeasurementKeys = new[] { key };
                CollectionAssert.AreEqual(new[] { key }, adapter.InputMeasurementKeys,
                    "An explicit nonempty routing update must remain effective.");
            }
            else
            {
                Assert.IsNotNull(adapter.InputMeasurementKeys);
                Assert.AreEqual(0, adapter.InputMeasurementKeys.Length);
            }
        }

        /// <summary>
        /// Keeps numeric output independent of culture while preserving general-format precision.
        /// </summary>
        [TestMethod]
        public void BuildJsonUsesInvariantCultureWithoutChangingGeneralPrecision()
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            List<IMeasurement> measurements = SetSignal(adapter, "PMU", "FQ", 1.2345678901234567D);
            long timestamp = new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc).Ticks;
            CultureInfo originalCulture = Thread.CurrentThread.CurrentCulture;

            try
            {
                Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("en-US");
                string baseline = adapter.BuildJson("PMU", timestamp, measurements);
                Thread.CurrentThread.CurrentCulture = CultureInfo.GetCultureInfo("de-DE");
                string actual = adapter.BuildJson("PMU", timestamp, measurements);

                Assert.AreEqual(baseline, actual);
                StringAssert.Contains(actual, "\"FQ\":1.23456789012346");
                Dictionary<string, object> frame = new JavaScriptSerializer().Deserialize<Dictionary<string, object>>(actual);
                Dictionary<string, object> values = (Dictionary<string, object>)frame["Measurements"];
                Assert.AreEqual(1.23456789012346D, Convert.ToDouble(values["FQ"], CultureInfo.InvariantCulture));
            }
            finally
            {
                Thread.CurrentThread.CurrentCulture = originalCulture;
            }
        }

        /// <summary>
        /// Escapes all JSON control characters in device and signal names.
        /// </summary>
        [TestMethod]
        public void BuildJsonEscapesEveryJsonControlCharacter()
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            StringBuilder characters = new StringBuilder("é\"\\");

            for (char character = '\0'; character < ' '; character++)
                characters.Append(character);

            string device = "PMU_" + characters;
            string signal = "FQ_" + characters;
            List<IMeasurement> measurements = SetSignal(adapter, device, signal, 50.25D);
            string json = adapter.BuildJson(device, new DateTime(2026, 10, 7, 12, 0, 0, DateTimeKind.Utc).Ticks, measurements);

            foreach (char character in json)
                Assert.IsTrue(character >= ' ', "JSON contains an unescaped control character.");

            Dictionary<string, object> frame = new JavaScriptSerializer().Deserialize<Dictionary<string, object>>(json);
            Assert.AreEqual(device, frame["DeviceID"]);
            Dictionary<string, object> values = (Dictionary<string, object>)frame["Measurements"];
            Assert.AreEqual(50.25D, Convert.ToDouble(values[signal], CultureInfo.InvariantCulture));
        }

        /// <summary>
        /// Preserves accepted zero settings and normalizes timestamp-format names.
        /// </summary>
        /// <param name="senderThreads">Configured sender thread count.</param>
        /// <param name="timestampFormat">Timestamp format supplied to configuration validation.</param>
        /// <param name="expectedFormat">Expected canonical timestamp-format name.</param>
        [DataTestMethod]
        [DataRow(0, "epochms", "EpochMs")]
        [DataRow(-1, "iso8601", "ISO8601")]
        [DataRow(4, "both", "Both")]
        public void ValidateConfigurationPreservesAcceptedSettingsAndCanonicalizesTimestampFormat(
            int senderThreads, string timestampFormat, string expectedFormat)
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.MaxQueuedBatches = 0;
            adapter.RetryBaseDelayMs = 0;
            adapter.SenderThreads = senderThreads;
            adapter.MaxWalSizeMB = 0;
            adapter.TimestampFormat = timestampFormat;

            Invoke(adapter, "ValidateConfiguration");

            Assert.AreEqual(0, adapter.MaxQueuedBatches);
            Assert.AreEqual(0, adapter.RetryBaseDelayMs);
            Assert.AreEqual(0L, adapter.MaxWalSizeMB);
            Assert.AreEqual(senderThreads, adapter.SenderThreads);
            Assert.AreEqual(expectedFormat, adapter.TimestampFormat);
        }

        /// <summary>
        /// Rejects a batch size that cannot hold an event.
        /// </summary>
        /// <param name="batchSize">Invalid event count to validate.</param>
        [DataTestMethod]
        [DataRow(0)]
        [DataRow(-1)]
        public void ValidateConfigurationRejectsNonpositiveBatchSize(int batchSize)
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.MaxBatchSize = batchSize;

            TargetInvocationException exception = Assert.ThrowsException<TargetInvocationException>(
                () => Invoke(adapter, "ValidateConfiguration"));

            Assert.IsInstanceOfType(exception.InnerException, typeof(InvalidOperationException));
        }

        /// <summary>
        /// Rejects a retry count that would silently prevent sends.
        /// </summary>
        [TestMethod]
        public void ValidateConfigurationRejectsNegativeRetries()
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.MaxRetries = -1;

            TargetInvocationException exception = Assert.ThrowsException<TargetInvocationException>(
                () => Invoke(adapter, "ValidateConfiguration"));

            Assert.IsInstanceOfType(exception.InnerException, typeof(InvalidOperationException));
        }

        /// <summary>
        /// Uses the connection-string entity path unless a hub name is explicitly configured.
        /// </summary>
        /// <param name="hubName">Explicit hub name, or null to use the entity path.</param>
        /// <param name="expectedHub">Expected Event Hub name.</param>
        [DataTestMethod]
        [DataRow(null, "connection-hub")]
        [DataRow("explicit-hub", "explicit-hub")]
        public void ParseEventHubConnectionStringUsesEntityPathUnlessHubNameIsExplicit(string hubName, string expectedHub)
        {
            using EventHubOutputAdapter adapter = CreateAdapter();
            adapter.EventHubConnectionString = "Endpoint=sb://example.servicebus.windows.net/;" +
                "SharedAccessKeyName=test-policy;SharedAccessKey=ZmFrZQ==;EntityPath=connection-hub";
            adapter.EventHubName = hubName;

            Invoke(adapter, "ParseEventHubConnectionString");

            Assert.AreEqual(expectedHub, adapter.EventHubName);
            Assert.AreEqual("https://example.servicebus.windows.net/" + expectedHub + "/messages?timeout=60&api-version=2014-01",
                typeof(EventHubOutputAdapter).GetField("m_sendUri", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(adapter));
        }

        #endregion

        #region [ Static ]

        private static EventHubOutputAdapter CreateAdapter()
        {
            return new EventHubOutputAdapter
            {
                MaxBatchSize = 800,
                MaxBatchIntervalMs = 1000,
                MaxQueuedBatches = 100,
                MaxRetries = 5,
                RetryBaseDelayMs = 1000,
                MaxWalSizeMB = 1024,
                SenderThreads = 4,
                TimestampFormat = "Both",
                IncludeTimestampEpochMs = true,
                IncludeQuality = true
            };
        }

        private static List<IMeasurement> SetSignal(EventHubOutputAdapter adapter, string device, string signal, double value)
        {
            MeasurementKey key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "PMU_TEST", 1);
            adapter.SignalLookup = new Dictionary<MeasurementKey, EventHubOutputAdapter.SignalInfo>
            {
                [key] = new EventHubOutputAdapter.SignalInfo
                {
                    DeviceAcronym = device,
                    SignalName = signal,
                    EscapedSignalName = EventHubOutputAdapter.EscapeJsonString(signal)
                }
            };

            return new List<IMeasurement> { new Measurement { Metadata = key.Metadata, Value = value } };
        }

        private static void Invoke(EventHubOutputAdapter adapter, string methodName)
        {
            MethodInfo method = typeof(EventHubOutputAdapter).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(method);
            method.Invoke(adapter, null);
        }

        private static void SetField(EventHubOutputAdapter adapter, string name, object value)
        {
            typeof(EventHubOutputAdapter).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic).SetValue(adapter, value);
        }

        #endregion
    }
}
