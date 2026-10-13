//******************************************************************************************************
//  ReplaySafetyTests.cs - Gbtc
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
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using GSF.COMTRADE;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class ReplaySafetyTests
    {
        private FileImporter m_importer;
        private string m_directory;
        private List<IMeasurement> m_published;

        [TestInitialize]
        public void Initialize()
        {
            m_directory = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(ReplaySafetyTests));
            m_importer = new FileImporter { Name = "ReplaySafety_" + Guid.NewGuid().ToString("N"), ProcessingInterval = 60000 };
            m_published = new List<IMeasurement>();
            m_importer.NewMeasurements += (sender, args) => m_published.AddRange(args.Argument);
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_directory);
        }

        [TestMethod]
        public void PartialFinalBatch_IsDeliveredExactlyOnceWithOriginalTimestamps()
        {
            string cfg = Create("batch", 5);
            m_importer.FramesPerTick = 3;
            int completed = 0;
            m_importer.ProcessingComplete += (sender, args) => completed++;
            Invoke("LoadFile", cfg);
            Invoke("PublishNextFrame");
            Invoke("PublishNextFrame");
            Invoke("PublishNextFrame");

            CollectionAssert.AreEqual(new[] { 100.0, 200.0, 300.0, 400.0, 500.0 }, m_published.Select(m => m.Value).ToArray());
            Assert.AreEqual(5L, Field<long>("m_framesPublished"));
            Assert.AreEqual(1, completed);
            Assert.IsTrue(m_importer.GetStatus().Contains("Completed"));
            StringAssert.Contains(m_importer.GetShortStatus(256), "Completed");
            Assert.IsTrue(m_importer.GetStatus().Contains("5 of 5"));
            Assert.IsTrue(m_published.All(m => m.Key.Equals(m_published[0].Key)));
            Assert.AreEqual(new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks, (long)m_published[0].Timestamp);
            Assert.IsTrue(m_published.Zip(m_published.Skip(1), (a, b) => a.Timestamp < b.Timestamp).All(x => x));
        }

        [TestMethod]
        public void MalformedRecord_PublishesEarlierBufferedRecordsThenFailsAndRetainsInput()
        {
            string cfg = Create("malformed", 4);
            string dat = Path.ChangeExtension(cfg, ".dat");
            string[] records = File.ReadAllLines(dat);
            File.WriteAllLines(dat, new[] { records[0], records[1], "invalid,sample,value" });
            m_importer.FramesPerTick = 10;
            m_importer.DeleteAfterProcessing = true;
            int completed = 0;
            m_importer.ProcessingComplete += (sender, args) => completed++;
            Invoke("LoadFile", cfg);
            Invoke("PublishNextFrame");
            Invoke("PublishNextFrame");

            CollectionAssert.AreEqual(new[] { 100.0, 200.0 }, m_published.Select(m => m.Value).ToArray());
            Assert.AreEqual(2L, Field<long>("m_framesPublished"));
            StringAssert.Contains(m_importer.GetStatus(), "Error");
            StringAssert.Contains(m_importer.GetStatus(), "2 of 4");
            Assert.IsNull(Field<object>("m_parser"));
            Assert.AreEqual(0, completed);
            Assert.IsTrue(File.Exists(cfg) && File.Exists(dat));

            File.WriteAllLines(dat, records);
            m_importer.ReplayFile(cfg);
            Invoke("PublishNextFrame");
            StringAssert.Contains(m_importer.GetStatus(), "Completed");
            Assert.AreEqual(6, m_published.Count, "An explicit retry should emit all four records.");
        }

        [TestMethod]
        public void ReplacementFromFinalBatchCallback_DoesNotCompleteOrCleanUpNewFile()
        {
            string first = Create("first", 1);
            string second = Create("second", 3);
            m_importer.DeleteAfterProcessing = true;
            m_importer.FramesPerTick = 2;
            bool replaced = false;
            m_importer.NewMeasurements += (sender, args) =>
            {
                if (replaced)
                    return;

                replaced = true;
                m_importer.ReplayFile(second);
            };
            Invoke("LoadFile", first);
            Invoke("PublishNextFrame");

            Assert.AreEqual(second, m_importer.FileName);
            Assert.AreEqual(0L, Field<long>("m_framesPublished"));
            Assert.IsTrue(File.Exists(first) && File.Exists(second));
            StringAssert.Contains(m_importer.GetStatus(), "Replaying second");
            Invoke("PublishNextFrame");
            Invoke("PublishNextFrame");
            Assert.IsTrue(File.Exists(first), "The replaced replay must not run stale cleanup.");
            Assert.IsFalse(File.Exists(second));
            CollectionAssert.AreEqual(new[] { 100.0, 100.0, 200.0, 300.0 }, m_published.Select(m => m.Value).ToArray());
        }

        [TestMethod]
        public void AbortDuringPublication_DoesNotDeadlockOrRunDelayedCleanup()
        {
            string cfg = Create("abort", 1);
            m_importer.FramesPerTick = 2;
            m_importer.DeleteAfterProcessing = true;
            using (ManualResetEventSlim entered = new ManualResetEventSlim())
            using (ManualResetEventSlim release = new ManualResetEventSlim())
            {
                m_importer.NewMeasurements += (sender, args) =>
                {
                    entered.Set();
                    Assert.IsTrue(release.Wait(5000), "Test did not release publication callback.");
                };
                Invoke("LoadFile", cfg);
                Task publish = Task.Run(() => Invoke("PublishNextFrame"));
                Assert.IsTrue(entered.Wait(5000));

                try
                {
                    Task abort = Task.Run(() => m_importer.Abort());
                    Assert.IsTrue(abort.Wait(2000), "Abort must not wait on a subscriber holding the publication callback.");
                }
                finally
                {
                    release.Set();
                }

                Assert.IsTrue(publish.Wait(5000));
            }

            Assert.AreEqual("Idle", m_importer.GetStatus());
            Assert.IsNull(Field<object>("m_parser"));
            Assert.IsTrue(File.Exists(cfg));
            Invoke("PublishNextFrame");
            Assert.AreEqual(1, m_published.Count);
        }

        [TestMethod]
        public void StatusCallback_CanWaitForLifecycleCommandOnAnotherThread()
        {
            bool handled = false;
            m_importer.StatusMessage += (sender, args) =>
            {
                if (handled)
                    return;

                handled = true;
                Assert.IsTrue(Task.Run(() => m_importer.Abort()).Wait(2000));
            };
            m_importer.ReplayFile(Create("status_callback", 3));
            Assert.IsTrue(handled);
            Assert.AreEqual("Idle", m_importer.GetStatus());
        }

        [TestMethod]
        public void DisconnectReconnect_ReloadsSelectedReplayWithoutSwitchingToWatch()
        {
            string cfg = Create("restart", 3);
            m_importer.WatchDirectory = m_directory;
            m_importer.ReplayFile(cfg);
            Invoke("PublishNextFrame");
            m_importer.Stop();
            m_importer.Start();
            Invoke("AttemptConnection");
            Invoke("PublishNextFrame");
            Assert.IsNull(Field<object>("m_fileWatcher"));
            Assert.AreEqual(cfg, m_importer.FileName);
            CollectionAssert.AreEqual(new[] { 100.0, 100.0 }, m_published.Select(m => m.Value).ToArray());
        }

        [TestMethod]
        public void TruncatedEof_CompletesAndCleansUpButShowsCountsAndWarning()
        {
            string cfg = Create("truncated", 4);
            string dat = Path.ChangeExtension(cfg, ".dat");
            File.WriteAllLines(dat, File.ReadAllLines(dat).Take(2));
            m_importer.FramesPerTick = 10;
            m_importer.DeleteAfterProcessing = true;
            Invoke("LoadFile", cfg);
            Invoke("PublishNextFrame");
            Assert.AreEqual(2, m_published.Count);
            StringAssert.Contains(m_importer.GetStatus(), "Completed");
            StringAssert.Contains(m_importer.GetStatus(), "2 of 4");
            StringAssert.Contains(m_importer.GetStatus(), "truncated");
            Assert.IsFalse(File.Exists(cfg) || File.Exists(dat));
        }

        [TestMethod]
        public void CleanupCollision_IsVisibleBeforeCompletionCallbackAndLeavesPairIntact()
        {
            string cfg = Create("collision", 1);
            string destination = Path.Combine(m_directory, "processed");
            Directory.CreateDirectory(destination);
            File.WriteAllText(Path.Combine(destination, "collision.dat"), "existing");
            m_importer.ProcessedDirectory = destination;
            m_importer.FramesPerTick = 2;
            string completedStatus = null;
            m_importer.ProcessingComplete += (sender, args) => completedStatus = m_importer.GetStatus();
            Invoke("LoadFile", cfg);
            Invoke("PublishNextFrame");
            Assert.IsNotNull(completedStatus);
            StringAssert.Contains(completedStatus, "Completed");
            StringAssert.Contains(completedStatus, "cleanup failed");
            Assert.IsTrue(File.Exists(cfg) && File.Exists(Path.ChangeExtension(cfg, ".dat")));
            Assert.AreEqual("existing", File.ReadAllText(Path.Combine(destination, "collision.dat")));
        }

        [TestMethod]
        public void DisposeDuringCallback_PreventsCompletionAndFurtherPublication()
        {
            string cfg = Create("dispose", 2);
            m_importer.FramesPerTick = 3;
            int completed = 0;
            m_importer.ProcessingComplete += (sender, args) => completed++;
            m_importer.NewMeasurements += (sender, args) => m_importer.Dispose();
            Invoke("LoadFile", cfg);
            Invoke("PublishNextFrame");
            Invoke("PublishNextFrame");
            Assert.AreEqual(2, m_published.Count);
            Assert.AreEqual(0, completed);
            Assert.IsNull(Field<object>("m_parser"));
            Assert.ThrowsException<ObjectDisposedException>(() => m_importer.ReplayFile(cfg));
        }

        [TestMethod]
        public void OutputMappingCallback_CompletesBeforeTimerAndCanAbortFromAnotherThread()
        {
            string cfg = Create("mapping_callback", 1);
            m_importer.ProcessingInterval = 0;
            bool routed = false;
            m_importer.OutputMeasurementsUpdated += (sender, args) =>
            {
                routed = true;
                Assert.IsTrue(Task.Run(() => m_importer.Abort()).Wait(2000));
            };
            m_importer.ReplayFile(cfg);
            Assert.IsTrue(routed);
            Assert.AreEqual("Idle", m_importer.GetStatus());
            Assert.IsNull(Field<object>("m_publishTimer"));
            Assert.AreEqual(0, m_published.Count);
        }

        private string Create(string name, int samples)
        {
            return ComtradeTestDataHelper.CreateComtradeFiles(m_directory, name, 1, 0, samples, FileType.Ascii);
        }

        private object Invoke(string name, params object[] args)
        {
            return typeof(FileImporter).GetMethod(name, BindingFlags.Instance | BindingFlags.NonPublic).Invoke(m_importer, args);
        }

        private T Field<T>(string name)
        {
            return (T)typeof(FileImporter).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic).GetValue(m_importer);
        }
    }
}
