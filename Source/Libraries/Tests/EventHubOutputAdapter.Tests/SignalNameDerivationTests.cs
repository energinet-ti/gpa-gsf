//******************************************************************************************************
//  SignalNameDerivationTests.cs - Gbtc
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

using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks the device-prefix and suffix naming options.
    /// </summary>
    [TestClass]
    public class SignalNameDerivationTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves the configured prefix and suffix stripping behavior.
        /// </summary>
        /// <param name="device">Device prefix used when deriving the signal name.</param>
        /// <param name="tag">Measurement tag to resolve.</param>
        /// <param name="stripPrefix">Whether the device prefix should be removed.</param>
        /// <param name="stripSuffix">Whether the final colon suffix should be removed.</param>
        /// <param name="delimiter">Delimiter between the device and signal name.</param>
        /// <param name="expected">Expected result for this input.</param>
        [DataTestMethod]
        [DataRow("SHELBY", "SHELBY-FQ:1", true, true, "-", "FQ")]
        [DataRow("SHELBY", "SHELBY-FQ:1", false, true, "-", "SHELBY-FQ")]
        [DataRow("OTHER", "SHELBY-FQ:1", true, true, "-", "SHELBY-FQ")]
        [DataRow("1001", "1001_STATION_A:df/dt", true, false, "_", "STATION_A:df/dt")]
        [DataRow("99999", "1001_STATION_A:Frequency", true, false, "_", "1001_STATION_A:Frequency")]
        public void DeriveSignalNamePreservesNamingOptions(string device, string tag,
            bool stripPrefix, bool stripSuffix, string delimiter, string expected)
        {
            using EventHubOutputAdapter adapter = new EventHubOutputAdapter
            {
                StripDevicePrefix = stripPrefix,
                StripPointIdSuffix = stripSuffix,
                DevicePrefixDelimiter = delimiter
            };
            MethodInfo method = typeof(EventHubOutputAdapter).GetMethod("DeriveSignalName", BindingFlags.Instance | BindingFlags.NonPublic);

            Assert.AreEqual(expected, method.Invoke(adapter, new object[] { tag, device, delimiter }));
        }

        #endregion
    }
}
