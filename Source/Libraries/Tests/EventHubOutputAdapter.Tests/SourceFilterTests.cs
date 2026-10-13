//******************************************************************************************************
//  SourceFilterTests.cs - Gbtc
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

using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks exact and prefix source matching for dynamic measurements.
    /// </summary>
    [TestClass]
    public class SourceFilterTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves exact and mixed source selections without regard to case.
        /// </summary>
        /// <param name="filter">Configured source filter.</param>
        /// <param name="accepted">Source that should match the filter.</param>
        /// <param name="rejected">Source that should not match the filter.</param>
        [DataTestMethod]
        [DataRow("PMU_1001", "pmu_1001", "PMU_1002")]
        [DataRow("PMU_1001, PMU_1002", "PMU_1002", "PMU_1003")]
        [DataRow("PMU_*, RTU_001", "RTU_001", "RTU_002")]
        public void MatchesSourceFilterPreservesExactAndMixedSelection(string filter, string accepted, string rejected)
        {
            using EventHubOutputAdapter adapter = new EventHubOutputAdapter();
            adapter.ParseSourceFilter(filter);

            Assert.IsTrue(adapter.MatchesSourceFilter(accepted));
            Assert.IsFalse(adapter.MatchesSourceFilter(rejected));
            Assert.IsFalse(adapter.MatchesSourceFilter(null));
            Assert.IsFalse(adapter.MatchesSourceFilter(""));
        }

        /// <summary>
        /// Rejects dynamically resolved measurements when no source filter is configured.
        /// </summary>
        [TestMethod]
        public void MissingFilterRejectsDynamicMeasurements()
        {
            using EventHubOutputAdapter adapter = new EventHubOutputAdapter();
            Assert.IsFalse(adapter.MatchesSourceFilter("PMU_1001"));
            adapter.ParseSourceFilter("");
            Assert.IsFalse(adapter.MatchesSourceFilter("PMU_1001"));
        }

        #endregion
    }
}
