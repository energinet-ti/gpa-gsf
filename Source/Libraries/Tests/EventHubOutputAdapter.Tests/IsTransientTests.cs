//******************************************************************************************************
//  IsTransientTests.cs - Gbtc
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

using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks classification of transport failures for retry.
    /// </summary>
    [TestClass]
    public class IsTransientTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves transient and permanent transport-failure classifications.
        /// </summary>
        /// <param name="status">Transport status to classify.</param>
        /// <param name="expected">Expected result for this input.</param>
        [DataTestMethod]
        [DataRow(WebExceptionStatus.Timeout, true)]
        [DataRow(WebExceptionStatus.ConnectFailure, true)]
        [DataRow(WebExceptionStatus.NameResolutionFailure, true)]
        [DataRow(WebExceptionStatus.SendFailure, false)]
        [DataRow(WebExceptionStatus.ProtocolError, false)]
        public void IsTransientClassifiesTransportFailures(WebExceptionStatus status, bool expected)
        {
            Assert.AreEqual(expected, EventHubHttpClient.IsTransient(new WebException("Transport failure", status)));
        }

        #endregion
    }
}
