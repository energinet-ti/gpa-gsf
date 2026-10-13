//******************************************************************************************************
//  AdapterLifecycleTests.cs - Gbtc
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
using System.IO;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Exercises batch ownership, retry recovery and worker shutdown through the adapter lifecycle.
    /// </summary>
    [TestClass]
    public class AdapterLifecycleTests
    {
        #region [ Members ]

        /// <summary>
        /// Exposes lifecycle hooks needed by these tests.
        /// </summary>
        private sealed class TestAdapter : EventHubOutputAdapter
        {
            #region [ Properties ]

            /// <summary>
            /// Gets whether the framework processing queue is enabled.
            /// </summary>
            public bool ProcessingEnabled => InternalProcessQueue.Enabled;

            #endregion

            #region [ Methods ]

            /// <summary>
            /// Opens the sender connection through the adapter lifecycle hook.
            /// </summary>
            public void Open() => AttemptConnection();

            /// <summary>
            /// Closes the sender connection through the adapter lifecycle hook.
            /// </summary>
            public void Close() => AttemptDisconnection();

            /// <summary>
            /// Invokes the framework connection-completed callback.
            /// </summary>
            public void ReportConnected() => OnConnected();

            #endregion
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Prevents connection callbacks from restarting a stopped or disposed adapter.
        /// </summary>
        [TestMethod]
        public void StopAndDisposeRejectLateConnectionCallbacks()
        {
            string directory = CreateDirectory();
            try
            {
                using TestAdapter adapter = CreateAdapter("http://127.0.0.1:1/hub", directory);
                adapter.Stop();

                Assert.ThrowsException<OperationCanceledException>(() => adapter.Open());
                Assert.IsNull(GetField<Thread[]>(adapter, "m_sendThreads"));
                Assert.IsFalse(adapter.ProcessingEnabled);
                adapter.ReportConnected();
                Assert.IsFalse(adapter.ProcessingEnabled, "A late connection notification restarted the framework processing queue.");

                adapter.Dispose();
                Assert.ThrowsException<OperationCanceledException>(() => adapter.Open());
                Assert.ThrowsException<ObjectDisposedException>(() => adapter.Start());
                Assert.IsNull(GetField<Thread[]>(adapter, "m_sendThreads"));
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        /// <summary>
        /// Allows live batches to progress before retrying an exhausted transient batch.
        /// </summary>
        [TestMethod]
        public void ExhaustedTransientBatchAllowsLiveTrafficThenRecoversWithoutRestart()
        {
            string directory = CreateDirectory();
            try
            {
                using TestHttpServer server = new TestHttpServer(new[] { 503, 201, 201 }, holdFirstResponse: true);
                using TestAdapter adapter = CreateAdapter(server.Url, directory);
                adapter.Open();
                Enqueue(adapter, "failed");
                WaitUntil(() => server.RequestCount == 1, "The first batch was not sent.");

                Enqueue(adapter, "live");
                server.ReleaseFirstResponse();
                WaitUntil(() => server.RequestCount == 3, "The failed batch was not recovered during the same connection.");
                WaitUntil(() => Directory.GetFiles(directory, "*.wal").Length == 0, "Successful batches remained in the WAL.");

                string[] requests = server.RequestBodies;
                StringAssert.Contains(requests[0], "failed");
                StringAssert.Contains(requests[1], "live");
                Assert.AreEqual(requests[0], requests[2], "Recovery must resend the original batch body.");
                adapter.Close();
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        /// <summary>
        /// Cancels an active request and preserves queued work before reconnecting with new workers.
        /// </summary>
        [TestMethod]
        public void DisconnectCancelsActiveRequestRetainsQueueAndJoinsBeforeReconnect()
        {
            string directory = CreateDirectory();
            try
            {
                using TestHttpServer server = new TestHttpServer(new[] { 201, 201, 201 }, holdFirstResponse: true);
                using TestAdapter adapter = CreateAdapter(server.Url, directory);
                adapter.Open();
                Thread[] oldWorkers = GetField<Thread[]>(adapter, "m_sendThreads");
                Enqueue(adapter, "active");
                WaitUntil(() => server.RequestCount == 1, "The first request did not reach the server.");
                Enqueue(adapter, "queued");

                Task close = Task.Run(() => adapter.Close());
                Assert.IsTrue(close.Wait(TimeSpan.FromSeconds(5)), "Disconnect did not cancel the delayed request.");
                foreach (Thread worker in oldWorkers)
                    Assert.IsFalse(worker.IsAlive, "A worker survived disconnection.");
                Assert.AreEqual(1, server.RequestCount, "A queued request started during disconnection.");
                Assert.AreEqual(2, Directory.GetFiles(directory, "*.wal").Length, "Active and queued work must remain replayable.");
                adapter.Close();

                server.ReleaseFirstResponse();
                adapter.Open();
                Thread[] newWorkers = GetField<Thread[]>(adapter, "m_sendThreads");
                Assert.AreNotSame(oldWorkers[0], newWorkers[0]);
                WaitUntil(() => server.RequestCount == 3, "Reconnect did not replay retained work.");
                WaitUntil(() => Directory.GetFiles(directory, "*.wal").Length == 0, "Replayed batches were not acknowledged.");
                string[] requests = server.RequestBodies;
                StringAssert.Contains(requests[1], "active");
                StringAssert.Contains(requests[2], "queued");

                adapter.Dispose();
                foreach (Thread worker in newWorkers)
                    Assert.IsFalse(worker.IsAlive, "Direct disposal left a worker running.");
            }
            finally
            {
                Directory.Delete(directory, true);
            }
        }

        /// <summary>
        /// Retains a pending batch when WAL publication fails and sends it after storage recovers.
        /// </summary>
        [TestMethod]
        public void FailedSpillRetainsPendingBatchUntilStorageReturns()
        {
            string directory = CreateDirectory();
            try
            {
                using TestHttpServer server = new TestHttpServer(new[] { 201 });
                using TestAdapter adapter = CreateAdapter(server.Url, directory);
                adapter.MaxQueuedBatches = 0;
                adapter.Open();
                List<string> pending = GetField<List<string>>(adapter, "m_pendingEvents");
                Directory.Delete(directory);

                Enqueue(adapter, "retained");

                Assert.AreSame(pending, GetField<List<string>>(adapter, "m_pendingEvents"));
                Assert.AreEqual(1, pending.Count, "Failed publication released the only copy of the batch.");
                Assert.AreEqual(0, server.RequestCount);
                Directory.CreateDirectory(directory);
                Flush(adapter);
                WaitUntil(() => server.RequestCount == 1, "The retained batch did not send after storage recovered.");
                WaitUntil(() => Directory.GetFiles(directory, "*.wal").Length == 0, "The successful spill was not acknowledged.");
                StringAssert.Contains(server.RequestBodies[0], "retained");
                adapter.Close();
            }
            finally
            {
                if (Directory.Exists(directory))
                    Directory.Delete(directory, true);
            }
        }

        #endregion

        #region [ Static ]

        private static string CreateDirectory()
        {
            string directory = Path.Combine(Path.GetTempPath(), "EventHubLifecycleTests_" + Guid.NewGuid().ToString("N"));
            Directory.CreateDirectory(directory);
            return directory;
        }

        private static TestAdapter CreateAdapter(string url, string directory)
        {
            TestAdapter adapter = new TestAdapter
            {
                EventHubName = "test",
                MaxBatchSize = 1,
                MaxBatchIntervalMs = 1000,
                SenderThreads = 1,
                MaxQueuedBatches = 100,
                MaxRetries = 0,
                RetryBaseDelayMs = 0,
                MaxWalSizeMB = 1024,
                WalDirectory = directory
            };

            SetField(adapter, "m_eventHubUrl", url);
            SetField(adapter, "m_sendUri", url + "/messages?api-version=2014-01");
            SetField(adapter, "m_sasKeyName", "test-key");
            SetField(adapter, "m_sasKey", "test-secret");
            return adapter;
        }

        private static void Enqueue(TestAdapter adapter, string id)
        {
            lock (GetField<object>(adapter, "m_batchLock"))
            {
                GetField<List<string>>(adapter, "m_pendingEvents").Add("{\"DeviceID\":\"" + id + "\",\"Measurements\":{\"FQ\":50}}");
                Flush(adapter);
            }
        }

        private static void Flush(TestAdapter adapter)
        {
            typeof(EventHubOutputAdapter).GetMethod("OnFlushTimer", BindingFlags.Instance | BindingFlags.NonPublic)
                .Invoke(adapter, new object[] { null });
        }

        private static T GetField<T>(TestAdapter adapter, string name)
        {
            return (T)typeof(EventHubOutputAdapter).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic).GetValue(adapter);
        }

        private static void SetField(TestAdapter adapter, string name, object value)
        {
            typeof(EventHubOutputAdapter).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic).SetValue(adapter, value);
        }

        private static void WaitUntil(Func<bool> condition, string message)
        {
            Assert.IsTrue(SpinWait.SpinUntil(condition, TimeSpan.FromSeconds(10)), message);
        }

        #endregion
    }
}
