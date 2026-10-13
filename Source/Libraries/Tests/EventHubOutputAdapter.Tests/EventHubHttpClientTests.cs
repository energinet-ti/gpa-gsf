//******************************************************************************************************
//  EventHubHttpClientTests.cs - Gbtc
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
using System.Net;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Checks the request format and HTTP error classification against a local server.
    /// </summary>
    [TestClass]
    public class EventHubHttpClientTests
    {
        #region [ Methods ]

        /// <summary>
        /// Preserves the REST request headers and batch body.
        /// </summary>
        [TestMethod]
        public void SendPreservesRestRequestAndBatchBody()
        {
            using TestHttpServer server = new TestHttpServer(new[] { 201 });
            EventHubHttpClient client = CreateClient(server.Url);
            const string Body = "[{\"Body\":\"{\\\"DeviceID\\\":\\\"PMU_é\\\"}\"}]";

            client.Send(Body);

            Assert.AreEqual(Body, server.RequestBodies[0]);
            StringAssert.StartsWith(server.RequestHeaders, "POST /hub/messages?api-version=2014-01 HTTP/1.1\r\n");
            StringAssert.Contains(server.RequestHeaders.ToLowerInvariant(), "content-type: application/vnd.microsoft.servicebus.json\r\n");
            StringAssert.Contains(server.RequestHeaders,
                "SharedAccessSignature sr=" + Uri.EscapeDataString(server.Url.ToLowerInvariant()) + "&sig=");
            StringAssert.Contains(server.RequestHeaders, "&skn=test-key");
        }

        /// <summary>
        /// Keeps HTTP status and error details available after disposing the response.
        /// </summary>
        /// <param name="statusCode">HTTP error status returned by the local server.</param>
        /// <param name="transient">Whether the HTTP status should be retried.</param>
        [DataTestMethod]
        [DataRow(400, false)]
        [DataRow(429, true)]
        [DataRow(503, true)]
        public void SendErrorRetainsStatusAndDetailsAfterResponseDisposal(int statusCode, bool transient)
        {
            using TestHttpServer server = new TestHttpServer(new[] { statusCode });
            EventHubHttpClient client = CreateClient(server.Url);
            WebException exception = null;

            try
            {
                client.Send("[]");
            }
            catch (WebException ex)
            {
                exception = ex;
            }

            Assert.IsNotNull(exception);
            Assert.AreEqual(WebExceptionStatus.ProtocolError, exception.Status);
            Assert.AreEqual(transient, EventHubHttpClient.IsTransient(exception));
            StringAssert.Contains(exception.Message, "Event Hub returned " + statusCode + ": local response");
        }

        #endregion

        #region [ Static ]

        private static EventHubHttpClient CreateClient(string url)
        {
            EventHubHttpClient client = new EventHubHttpClient(url, url + "/messages?api-version=2014-01", "test-key", "test-secret");
            client.ConfigureServicePoint();
            return client;
        }

        #endregion
    }
}
