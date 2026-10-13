//******************************************************************************************************
//  EventHubHttpClient.cs - Gbtc
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
//  02/23/2026 - Frederik Fast
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
using System.Globalization;
using System.IO;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;

namespace EventHubOutputAdapter
{
    /// <summary>
    /// Self-contained HTTP client for Azure Event Hub REST API with automatic SAS token
    /// management. Uses framework HTTP and cryptography APIs without additional dependencies.
    /// </summary>
    internal class EventHubHttpClient
    {
        #region [ Members ]

        // Nested Types

        /// <summary>
        /// Retains the HTTP status after the response and its stream have been disposed.
        /// </summary>
        private sealed class EventHubResponseException : WebException
        {
            #region [ Constructors ]

            /// <summary>
            /// Creates an exception containing the Event Hub response details.
            /// </summary>
            /// <param name="statusCode">HTTP status returned by Event Hub.</param>
            /// <param name="responseBody">Response body returned by Event Hub.</param>
            /// <param name="innerException">Original request exception.</param>
            public EventHubResponseException(HttpStatusCode statusCode, string responseBody, WebException innerException)
                : base($"Event Hub returned {(int)statusCode}: {responseBody}", innerException,
                    innerException.Status, innerException.Response)
            {
                StatusCode = statusCode;
            }

            #endregion

            #region [ Properties ]

            /// <summary>
            /// Gets the HTTP status captured before the response was disposed.
            /// </summary>
            public HttpStatusCode StatusCode
            {
                get;
            }

            #endregion
        }

        // Constants
        private const int SasTokenLifetimeMinutes = 30;
        private const int SasTokenRenewBeforeExpiryMinutes = 5;

        // Fields
        private readonly string m_eventHubUrl;
        private readonly string m_sendUri;
        private readonly string m_sasKeyName;
        private readonly string m_sasKey;
        private readonly object m_tokenLock = new object();
        private string m_sasToken;
        private DateTime m_sasTokenExpiry;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new Event Hub HTTP client.
        /// </summary>
        /// <param name="eventHubUrl">Event Hub URL used as the SAS token audience.</param>
        /// <param name="sendUri">REST endpoint for sending batches.</param>
        /// <param name="sasKeyName">Shared access policy name.</param>
        /// <param name="sasKey">Shared access policy key.</param>
        public EventHubHttpClient(string eventHubUrl, string sendUri, string sasKeyName, string sasKey)
        {
            m_eventHubUrl = eventHubUrl;
            m_sendUri = sendUri;
            m_sasKeyName = sasKeyName;
            m_sasKey = sasKey;
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets the Event Hub URL for status display.
        /// </summary>
        public string EventHubUrl => m_eventHubUrl;

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Configures ServicePoint connection pooling for the Event Hub endpoint.
        /// </summary>
        /// <param name="connectionLimit">Maximum simultaneous connections to the endpoint.</param>
        public void ConfigureServicePoint(int connectionLimit = 4)
        {
            ServicePoint sp = ServicePointManager.FindServicePoint(new Uri(m_eventHubUrl));
            sp.ConnectionLimit = connectionLimit;
            sp.Expect100Continue = false;
        }

        /// <summary>
        /// Sends a pre-built batch body to Event Hub via HTTP POST with auto-renewed SAS token.
        /// </summary>
        /// <param name="batchBody">Serialized REST batch body to send.</param>
        /// <param name="cancellationToken">Cancels the active request when the adapter stops.</param>
        public void Send(string batchBody, CancellationToken cancellationToken = default)
        {
            cancellationToken.ThrowIfCancellationRequested();
            string token = GetOrRenewSasToken();

            byte[] body = Encoding.UTF8.GetBytes(batchBody);

            HttpWebRequest request = (HttpWebRequest)WebRequest.Create(m_sendUri);
            request.Method = "POST";
            request.ContentType = "application/vnd.microsoft.servicebus.json";
            request.ContentLength = body.Length;
            request.Headers.Add("Authorization", token);

            using CancellationTokenRegistration registration = cancellationToken.Register(request.Abort);

            try
            {
                using (Stream stream = request.GetRequestStream())
                    stream.Write(body, 0, body.Length);

                try
                {
                    using HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                }
                catch (WebException ex) when (ex.Response is HttpWebResponse errorResponse)
                {
                    using (errorResponse)
                    using (StreamReader reader = new StreamReader(errorResponse.GetResponseStream() ?? Stream.Null))
                    {
                        string responseBody = reader.ReadToEnd();

                        throw new EventHubResponseException(errorResponse.StatusCode, responseBody, ex);
                    }
                }
            }
            catch (Exception ex) when (cancellationToken.IsCancellationRequested &&
                (ex is WebException || ex is IOException || ex is ObjectDisposedException))
            {
                throw new OperationCanceledException("Event Hub send was canceled.", ex, cancellationToken);
            }
        }

        /// <summary>
        /// Returns a valid SAS token, generating a new one if the current token is expired or missing.
        /// Token and expiry are read and renewed together under the lock for parallel sender threads.
        /// </summary>
        /// <returns>SAS token for the configured Event Hub URL.</returns>
        private string GetOrRenewSasToken()
        {
            lock (m_tokenLock)
            {
                if (m_sasToken != null && DateTime.UtcNow < m_sasTokenExpiry)
                    return m_sasToken;

                DateTime expiry = DateTime.UtcNow.AddMinutes(SasTokenLifetimeMinutes);
                long expiryEpoch = (long)(expiry - s_epoch).TotalSeconds;
                string expiryStr = expiryEpoch.ToString(CultureInfo.InvariantCulture);

                string encodedUri = Uri.EscapeDataString(m_eventHubUrl.ToLowerInvariant());
                string stringToSign = encodedUri + "\n" + expiryStr;

                using HMACSHA256 hmac = new HMACSHA256(Encoding.UTF8.GetBytes(m_sasKey));
                string signature = Convert.ToBase64String(hmac.ComputeHash(Encoding.UTF8.GetBytes(stringToSign)));

                string token = $"SharedAccessSignature sr={encodedUri}&sig={Uri.EscapeDataString(signature)}&se={expiryStr}&skn={m_sasKeyName}";

                m_sasTokenExpiry = expiry.AddMinutes(-SasTokenRenewBeforeExpiryMinutes);
                m_sasToken = token;

                return token;
            }
        }

        #endregion

        #region [ Static ]

        // Static Fields
        private static readonly DateTime s_epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        // Static Methods

        /// <summary>
        /// Classifies a WebException as transient (worth retrying) or permanent.
        /// </summary>
        /// <param name="ex">Request exception to classify.</param>
        /// <returns>Whether the failure should be retried.</returns>
        internal static bool IsTransient(WebException ex)
        {
            if (ex is EventHubResponseException responseException)
                return responseException.StatusCode == (HttpStatusCode)429 || (int)responseException.StatusCode >= 500;

            if (ex.Response is HttpWebResponse resp)
            {
                int code = (int)resp.StatusCode;

                return code == 429 || code >= 500;
            }

            return ex.Status == WebExceptionStatus.Timeout
                || ex.Status == WebExceptionStatus.ConnectFailure
                || ex.Status == WebExceptionStatus.NameResolutionFailure;
        }

        #endregion
    }
}
