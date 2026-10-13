//******************************************************************************************************
//  LazyResolutionTests.cs - Gbtc
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
using System.ComponentModel;
using System.Reflection;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks dynamic tag resolution with broadcast routing and the default naming options.
    /// </summary>
    [TestClass]
    public class LazyResolutionTests
    {
        #region [ Methods ]

        /// <summary>
        /// Resolves representative tags with a prefix source filter, an underscore delimiter and the default naming options.
        /// </summary>
        /// <param name="source">Measurement source to match against the filter.</param>
        /// <param name="tag">Measurement tag to resolve.</param>
        /// <param name="expectedDevice">Expected device prefix, or null when rejected.</param>
        /// <param name="expectedSignal">Expected signal name, or null when rejected.</param>
        [DataTestMethod]
        [DataRow("PMU_1001", "1001_Frequency", "1001", "Frequency")]
        [DataRow("pmu_1001", "1001_FQ:1", "1001", "FQ")]
        [DataRow("PMU_1001", "1001_STATION_A:Frequency", "1001", "STATION_A")]
        [DataRow("STAT", "1001_Frequency", null, null)]
        public void BroadcastDefaultsResolveTags(string source, string tag, string expectedDevice, string expectedSignal)
        {
            using EventHubOutputAdapter adapter = new EventHubOutputAdapter { DevicePrefixDelimiter = "_" };

            // Initialization applies these attributes. Read them here without connecting to a host or Event Hub.
            PropertyInfo prefixProperty = typeof(EventHubOutputAdapter).GetProperty(nameof(EventHubOutputAdapter.StripDevicePrefix));
            PropertyInfo suffixProperty = typeof(EventHubOutputAdapter).GetProperty(nameof(EventHubOutputAdapter.StripPointIdSuffix));
            adapter.StripDevicePrefix = (bool)prefixProperty.GetCustomAttribute<DefaultValueAttribute>().Value;
            adapter.StripPointIdSuffix = (bool)suffixProperty.GetCustomAttribute<DefaultValueAttribute>().Value;
            Assert.IsTrue(adapter.StripDevicePrefix, "StripDevicePrefix should default to true.");
            Assert.IsTrue(adapter.StripPointIdSuffix, "StripPointIdSuffix should default to true.");
            adapter.ParseSourceFilter("PMU_*");

            MeasurementKey key = MeasurementKey.CreateOrUpdate(Guid.NewGuid(), source, 1);
            IMeasurement measurement = new Measurement { Metadata = key.Metadata.ChangeTagName(tag) };
            MethodInfo resolve = typeof(EventHubOutputAdapter).GetMethod("ResolveSignalInfo", BindingFlags.Instance | BindingFlags.NonPublic);
            Assert.IsNotNull(resolve);
            EventHubOutputAdapter.SignalInfo info = (EventHubOutputAdapter.SignalInfo)resolve.Invoke(adapter, new object[] { measurement });

            Assert.AreEqual(expectedDevice, info.DeviceAcronym);
            Assert.AreEqual(expectedSignal, info.SignalName);
        }

        #endregion
    }
}
