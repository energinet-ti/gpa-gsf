//******************************************************************************************************
//  BuildBatchBodyTests.cs - Gbtc
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

using System.Collections.Generic;
using System.Web.Script.Serialization;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks event bodies and the REST batch envelope.
    /// </summary>
    [TestClass]
    public class BuildBatchBodyTests
    {
        #region [ Methods ]

        /// <summary>
        /// Embeds event JSON strings in the REST batch envelope.
        /// </summary>
        [TestMethod]
        public void BuildBatchBodyPreservesEventsAndRestEnvelope()
        {
            List<string> events = new List<string>
            {
                "{\"DeviceID\":\"SHELBY\",\"Measurements\":{\"FQ\":\"NaN\"}}",
                "{\"DeviceID\":\"1001\",\"Measurements\":{\"FQ\":50}}"
            };
            string body = EventHubOutputAdapter.BuildBatchBody(events);
            List<Dictionary<string, object>> batch = new JavaScriptSerializer().Deserialize<List<Dictionary<string, object>>>(body);

            Assert.AreEqual(events.Count, batch.Count);

            for (int i = 0; i < events.Count; i++)
            {
                Assert.AreEqual(events[i], batch[i]["Body"]);
                Assert.AreEqual("application/json", ((Dictionary<string, object>)batch[i]["BrokerProperties"])["ContentType"]);
            }
        }

        /// <summary>
        /// Serializes an empty batch as an empty JSON array.
        /// </summary>
        [TestMethod]
        public void EmptyListProducesEmptyArray()
        {
            Assert.AreEqual("[]", EventHubOutputAdapter.BuildBatchBody(new List<string>()));
        }

        #endregion
    }
}
