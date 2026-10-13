//******************************************************************************************************
//  TestHttpServer.cs - Gbtc
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
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Serves local HTTP responses, optionally holding the first response for lifecycle checks.
    /// </summary>
    internal sealed class TestHttpServer : IDisposable
    {
        #region [ Members ]

        private readonly TcpListener m_listener = new TcpListener(IPAddress.Loopback, 0);
        private readonly ManualResetEventSlim m_releaseFirst;
        private readonly List<string> m_requests = new List<string>();
        private readonly Task m_server;
        private volatile bool m_stopping;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Starts the local server with the specified response sequence.
        /// </summary>
        /// <param name="responseCodes">HTTP status codes to return in request order.</param>
        /// <param name="holdFirstResponse">Whether to wait before returning the first response.</param>
        public TestHttpServer(int[] responseCodes, bool holdFirstResponse = false)
        {
            m_listener.Start();
            Url = "http://127.0.0.1:" + ((IPEndPoint)m_listener.LocalEndpoint).Port + "/hub";
            m_releaseFirst = new ManualResetEventSlim(!holdFirstResponse);
            m_server = Task.Run(() => Serve(responseCodes));
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets the local Event Hub endpoint used by the tests.
        /// </summary>
        public string Url
        {
            get;
        }

        /// <summary>
        /// Gets the headers from the most recently read request.
        /// </summary>
        public string RequestHeaders
        {
            get;
            private set;
        }

        /// <summary>
        /// Gets the number of request bodies received so far.
        /// </summary>
        public int RequestCount
        {
            get
            {
                lock (m_requests)
                    return m_requests.Count;
            }
        }

        /// <summary>
        /// Gets a snapshot of the received request bodies in arrival order.
        /// </summary>
        public string[] RequestBodies
        {
            get
            {
                lock (m_requests)
                    return m_requests.ToArray();
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Allows the first response to return to the client.
        /// </summary>
        public void ReleaseFirstResponse() => m_releaseFirst.Set();

        /// <summary>
        /// Stops the listener and waits for the local server to finish.
        /// </summary>
        public void Dispose()
        {
            m_stopping = true;
            m_listener.Stop();
            m_releaseFirst.Set();
            m_server.GetAwaiter().GetResult();
            m_releaseFirst.Dispose();
        }

        private void Serve(int[] responseCodes)
        {
            try
            {
                for (int i = 0; i < responseCodes.Length; i++)
                {
                    using TcpClient connection = m_listener.AcceptTcpClient();
                    using NetworkStream stream = connection.GetStream();
                    stream.ReadTimeout = 5000;
                    ReadRequest(stream);
                    if (i == 0)
                        m_releaseFirst.Wait();
                    if (m_stopping)
                        return;

                    byte[] response = Encoding.ASCII.GetBytes("HTTP/1.1 " + responseCodes[i] +
                        " Local\r\nContent-Length: 14\r\nConnection: close\r\n\r\nlocal response");
                    try
                    {
                        stream.Write(response, 0, response.Length);
                    }
                    catch (IOException) when (i == 0)
                    {
                        // Shutdown can abort the first request while its response is held.
                    }
                }
            }
            catch (Exception ex) when (m_stopping && (ex is SocketException || ex is IOException || ex is ObjectDisposedException))
            {
                // Disposal releases any unused accept or pending read.
            }
        }

        private void ReadRequest(NetworkStream stream)
        {
            using BinaryReader reader = new BinaryReader(stream, Encoding.UTF8, leaveOpen: true);
            StringBuilder headers = new StringBuilder();
            while (!headers.ToString().EndsWith("\r\n\r\n", StringComparison.Ordinal))
                headers.Append((char)reader.ReadByte());

            int contentLength = 0;
            RequestHeaders = headers.ToString();
            foreach (string header in RequestHeaders.Split(new[] { "\r\n" }, StringSplitOptions.None))
            {
                if (header.StartsWith("Content-Length:", StringComparison.OrdinalIgnoreCase))
                    contentLength = int.Parse(header.Substring("Content-Length:".Length), CultureInfo.InvariantCulture);
            }

            byte[] body = reader.ReadBytes(contentLength);
            if (body.Length != contentLength)
                throw new EndOfStreamException();
            lock (m_requests)
                m_requests.Add(Encoding.UTF8.GetString(body));
        }

        #endregion
    }
}
