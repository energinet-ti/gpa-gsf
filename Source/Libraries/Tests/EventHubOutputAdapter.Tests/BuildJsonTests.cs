//******************************************************************************************************
//  BuildJsonTests.cs - Gbtc
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
using System.Collections.Generic;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks event fields, values, timestamps and quality flags.
    /// </summary>
    [TestClass]
    public class BuildJsonTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves the default JSON fields for a standard device frame.
        /// </summary>
        [TestMethod]
        public void DefaultGsfStandardDeviceFrame()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("Both", includeQuality: true);
            var keyFQ = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            var keyDF = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 2);
            var keyPM1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 3);
            var keyPA1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 4);
            SetupLookup(adapter,
                (keyFQ, "SHELBY", "FQ"),
                (keyDF, "SHELBY", "DF"),
                (keyPM1, "SHELBY", "PM1"),
                (keyPA1, "SHELBY", "PA1"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(keyFQ, 59.966, MeasurementStateFlags.Normal),
                CreateMeasurement(keyDF, 0.31, MeasurementStateFlags.Normal),
                CreateMeasurement(keyPM1, 230000.0, MeasurementStateFlags.Normal),
                CreateMeasurement(keyPA1, 30.25, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            StringAssert.Contains(json, "\"DeviceID\":\"SHELBY\"");
            StringAssert.Contains(json, "\"Timestamp\":\"2026-02-20T13:00:00.0000000Z\"");
            StringAssert.Contains(json, "\"TimestampEpochMs\":");
            StringAssert.Contains(json, "\"FQ\":59.966");
            StringAssert.Contains(json, "\"DF\":0.31");
            StringAssert.Contains(json, "\"PM1\":230000");
            StringAssert.Contains(json, "\"PA1\":30.25");
            StringAssert.Contains(json, "\"MeasurementCount\":4");
            StringAssert.Contains(json, "\"Quality\":0");
        }

        /// <summary>
        /// Preserves custom channel names and values in a device frame.
        /// </summary>
        [TestMethod]
        public void ColonChannelNamesDeviceFrame()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("Both", includeQuality: true);
            var keyFQ = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            var keyDF = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 2);
            var keyV1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 3);
            var keyVA1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 4);
            var keyIA1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 5);
            SetupLookup(adapter,
                (keyFQ, "1001", "STATION_A:Frequency"),
                (keyDF, "1001", "STATION_A:df/dt"),
                (keyV1, "1001", "STATION_A:V1"),
                (keyVA1, "1001", "STATION_A:VA1"),
                (keyIA1, "1001", "STATION_A:IA1"));

            var dt = new DateTime(2026, 2, 20, 10, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(keyFQ, 50.01, MeasurementStateFlags.Normal),
                CreateMeasurement(keyDF, 0.01, MeasurementStateFlags.Normal),
                CreateMeasurement(keyV1, 230000.0, MeasurementStateFlags.Normal),
                CreateMeasurement(keyVA1, 30.25, MeasurementStateFlags.Normal),
                CreateMeasurement(keyIA1, 400.5, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("1001", dt.Ticks, measurements);

            StringAssert.Contains(json, "\"DeviceID\":\"1001\"");
            StringAssert.Contains(json, "\"STATION_A:Frequency\":50.01");
            StringAssert.Contains(json, "\"STATION_A:df/dt\":0.01");
            StringAssert.Contains(json, "\"STATION_A:V1\":230000");
            StringAssert.Contains(json, "\"STATION_A:VA1\":30.25");
            StringAssert.Contains(json, "\"STATION_A:IA1\":400.5");
            StringAssert.Contains(json, "\"MeasurementCount\":5");
            StringAssert.Contains(json, "\"Quality\":0");
        }

        /// <summary>
        /// Includes the ISO timestamp without epoch milliseconds when requested.
        /// </summary>
        [TestMethod]
        public void Iso8601OnlyNoEpoch()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("ISO8601", includeQuality: true);
            var key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            SetupLookup(adapter, (key, "SHELBY", "FQ"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(key, 60.0, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            StringAssert.Contains(json, "\"Timestamp\":\"");
            Assert.IsFalse(json.Contains("TimestampEpochMs"));
        }

        /// <summary>
        /// Includes epoch milliseconds without an ISO timestamp when requested.
        /// </summary>
        [TestMethod]
        public void EpochMsOnlyNoIso()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("EpochMs", includeQuality: true);
            var key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            SetupLookup(adapter, (key, "SHELBY", "FQ"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(key, 60.0, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            StringAssert.Contains(json, "\"TimestampEpochMs\":");
            Assert.IsFalse(json.Contains("\"Timestamp\":\""));
        }

        /// <summary>
        /// Omits the quality field when it is disabled.
        /// </summary>
        [TestMethod]
        public void QualityOmittedWhenDisabled()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("Both", includeQuality: false);
            var key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            SetupLookup(adapter, (key, "SHELBY", "FQ"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(key, 60.0, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            Assert.IsFalse(json.Contains("\"Quality\":"));
        }

        /// <summary>
        /// Combines measurement quality flags with a bitwise OR.
        /// </summary>
        [TestMethod]
        public void AggregatesQualityBitwiseOr()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("Both", includeQuality: true);
            var key1 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            var key2 = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 2);
            SetupLookup(adapter,
                (key1, "SHELBY", "FQ"),
                (key2, "SHELBY", "PM1"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(key1, 60.0, MeasurementStateFlags.Normal),
                CreateMeasurement(key2, 100.0, MeasurementStateFlags.BadData)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            int qualityVal = (int)MeasurementStateFlags.BadData;
            StringAssert.Contains(json, $"\"Quality\":{qualityVal}");
        }

        /// <summary>
        /// Preserves the string representation of a non-finite value.
        /// </summary>
        [TestMethod]
        public void NaNSerializedAsString()
        {
            using EventHubOutputAdapter adapter = CreateAdapter("Both", includeQuality: true);
            var key = MeasurementKey.LookUpOrCreate(Guid.NewGuid(), "TEST", 1);
            SetupLookup(adapter, (key, "SHELBY", "FQ"));

            var dt = new DateTime(2026, 2, 20, 13, 0, 0, DateTimeKind.Utc);
            var measurements = new List<IMeasurement>
            {
                CreateMeasurement(key, double.NaN, MeasurementStateFlags.Normal)
            };

            string json = adapter.BuildJson("SHELBY", dt.Ticks, measurements);

            StringAssert.Contains(json, "\"FQ\":\"NaN\"");
        }

        #endregion

        #region [ Static ]

        private static EventHubOutputAdapter CreateAdapter(string timestampFormat, bool includeQuality)
        {
            return new EventHubOutputAdapter
            {
                TimestampFormat = timestampFormat,
                IncludeQuality = includeQuality,
                IncludeTimestampEpochMs = true,
                StripDevicePrefix = true
            };
        }

        private static void SetupLookup(EventHubOutputAdapter adapter, params (MeasurementKey key, string device, string signal)[] entries)
        {
            adapter.SignalLookup = new Dictionary<MeasurementKey, EventHubOutputAdapter.SignalInfo>();
            foreach (var (key, device, signal) in entries)
            {
                adapter.SignalLookup[key] = new EventHubOutputAdapter.SignalInfo
                {
                    DeviceAcronym = device,
                    SignalName = signal,
                    EscapedSignalName = EventHubOutputAdapter.EscapeJsonString(signal)
                };
            }
        }

        private static IMeasurement CreateMeasurement(MeasurementKey key, double value, MeasurementStateFlags flags)
        {
            return new Measurement
            {
                Metadata = key.Metadata,
                Value = value,
                StateFlags = flags
            };
        }

        #endregion
    }
}
