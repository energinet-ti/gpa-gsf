//******************************************************************************************************
//  WatchReadinessAndCleanupTests.cs - Gbtc
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
using GSF.COMTRADE;
using GSF.TimeSeries.Adapters;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class WatchReadinessAndCleanupTests
    {
        private const BindingFlags PrivateInstance = BindingFlags.NonPublic | BindingFlags.Instance;
        private FileImporter m_importer;
        private string m_testDirectory;
        private string m_watchDirectory;
        private string m_stagingDirectory;
        private readonly List<double> m_values = new List<double>();

        [TestInitialize]
        public void Setup()
        {
            m_testDirectory = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(WatchReadinessAndCleanupTests));
            m_watchDirectory = Path.Combine(m_testDirectory, "watch");
            m_stagingDirectory = Path.Combine(m_testDirectory, "staging");
            Directory.CreateDirectory(m_watchDirectory);
            Directory.CreateDirectory(m_stagingDirectory);
            m_importer = new FileImporter
            {
                WatchDirectory = m_watchDirectory,
                FileSettleTimeMs = 0,
                ProcessingInterval = 60000,
                FramesPerTick = 10
            };
            typeof(InputAdapterBase).GetProperty("ConnectionAttemptInterval", PrivateInstance).SetValue(m_importer, 20.0);
            m_importer.NewMeasurements += (sender, args) => m_values.AddRange(args.Argument.Select(measurement => measurement.Value));
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_testDirectory);
        }

        [TestMethod]
        public void Completion_DoesNotBypassNextPairsSettleInterval()
        {
            CreatePair(m_watchDirectory, "first", 100);
            StartWatch();
            m_importer.FileSettleTimeMs = 250;
            string second = DeliverPair("second", 200);
            Notify(second);

            Publish();

            Assert.AreEqual(FileImporterState.Watching, State);
            CollectionAssert.AreEqual(new[] { 100.0 }, m_values);
            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 3000), "The settled pair was not retried.");
            Publish();
            CollectionAssert.AreEqual(new[] { 100.0, 200.0 }, m_values);
        }

        [TestMethod]
        public void MissingDat_IsRetainedWithoutBlockingReadyPair_AndRetriedWithoutAnotherEvent()
        {
            StartWatch();
            string staged = CreatePair(m_stagingDirectory, "delayed", 100);
            string delayed = Path.Combine(m_watchDirectory, "delayed.cfg");
            File.Copy(staged, delayed);
            Notify(delayed);
            Invoke("ProcessNextPendingFile");
            Assert.AreEqual(FileImporterState.Watching, State);

            string ready = DeliverPair("ready", 200);
            Notify(ready);
            Invoke("ProcessNextPendingFile");
            Publish();
            CollectionAssert.AreEqual(new[] { 200.0 }, m_values);
            Assert.AreEqual(1, Pending.Count, "An incomplete pair must remain queued.");

            File.Copy(Path.ChangeExtension(staged, ".dat"), Path.ChangeExtension(delayed, ".dat"));

            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 3000), "DAT arrival without another CFG event was lost.");
            Publish();
            CollectionAssert.AreEqual(new[] { 200.0, 100.0 }, m_values);
        }

        [TestMethod]
        public void DatWrite_RestartsOnlyItsPairsSettleInterval()
        {
            StartWatch();
            m_importer.FileSettleTimeMs = 200;
            string path = DeliverPair("writing", 123);
            Notify(path);
            Notify(Path.ChangeExtension(path, ".dat"), WatcherChangeTypes.Changed);
            Invoke("ProcessNextPendingFile");

            Assert.AreEqual(FileImporterState.Watching, State);
            Assert.AreEqual(0, m_values.Count);
            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 3000));
            Publish();
            CollectionAssert.AreEqual(new[] { 123.0 }, m_values);
        }

        [TestMethod]
        public void DuplicateEvents_QueueAndPublishEachPathOncePerWatchSession()
        {
            CreatePair(m_watchDirectory, "first", 100);
            StartWatch();
            string second = DeliverPair("second", 200);
            Notify(second);
            Notify(second);
            Notify(second, WatcherChangeTypes.Changed);
            Assert.AreEqual(1, Pending.Count);

            Publish();
            Publish();
            Notify(second);
            Invoke("ProcessNextPendingFile");

            CollectionAssert.AreEqual(new[] { 100.0, 200.0 }, m_values);
            Assert.AreEqual(0, Pending.Count);
            Assert.AreEqual(FileImporterState.Watching, State);
        }

        [TestMethod]
        public void RenameToFinalNames_ProcessesCompletePair()
        {
            StartWatch();
            string staged = CreatePair(m_stagingDirectory, "renamed", 123);
            string cfg = Path.Combine(m_watchDirectory, "renamed.cfg");
            string dat = Path.ChangeExtension(cfg, ".dat");
            File.Copy(staged, cfg + ".tmp");
            File.Copy(Path.ChangeExtension(staged, ".dat"), dat + ".tmp");
            File.Move(cfg + ".tmp", cfg);
            Invoke("OnFileRenamed", Watcher, new RenamedEventArgs(WatcherChangeTypes.Renamed, m_watchDirectory, "renamed.cfg", "renamed.cfg.tmp"));
            Invoke("ProcessNextPendingFile");
            Assert.AreEqual(FileImporterState.Watching, State);

            File.Move(dat + ".tmp", dat);
            Invoke("OnFileRenamed", Watcher, new RenamedEventArgs(WatcherChangeTypes.Renamed, m_watchDirectory, "renamed.dat", "renamed.dat.tmp"));
            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 3000));
            Publish();

            CollectionAssert.AreEqual(new[] { 123.0 }, m_values);
        }

        [TestMethod]
        public void ActualWatcher_RenameDeliveryPublishesOnce()
        {
            m_importer.FileSettleTimeMs = 50;
            m_importer.ProcessingInterval = 1;
            string staged = CreatePair(m_stagingDirectory, "delivered", 456);
            string cfg = Path.Combine(m_watchDirectory, "delivered.cfg");
            string dat = Path.ChangeExtension(cfg, ".dat");

            using (ManualResetEventSlim published = new ManualResetEventSlim())
            {
                m_importer.NewMeasurements += (sender, args) => published.Set();
                Invoke("StartDirectoryWatch");
                File.Move(staged, cfg + ".tmp");
                File.Move(Path.ChangeExtension(staged, ".dat"), dat + ".tmp");
                File.Move(cfg + ".tmp", cfg);
                File.Move(dat + ".tmp", dat);

                Assert.IsTrue(published.Wait(3000), "The active watcher missed rename-to-final delivery.");
                Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Watching, 3000));
                CollectionAssert.AreEqual(new[] { 456.0 }, m_values);
            }
        }

        [TestMethod]
        public void WatchFilter_IsAppliedToCreatedAndRenamedCfgFiles()
        {
            m_importer.WatchFilter = "event*.cfg";
            StartWatch();
            string ignored = DeliverPair("ignored", 100);
            string accepted = DeliverPair("event1", 200);
            Notify(ignored);
            Notify(accepted);
            Invoke("ProcessNextPendingFile");
            Publish();

            CollectionAssert.AreEqual(new[] { 200.0 }, m_values);
            Assert.IsTrue(File.Exists(ignored));
        }

        [TestMethod]
        public void InitialScan_RespectsFilenameOrder()
        {
            CreatePair(m_watchDirectory, "c", 300);
            CreatePair(m_watchDirectory, "a", 100);
            CreatePair(m_watchDirectory, "b", 200);
            StartWatch();
            Publish();
            Publish();
            Publish();

            CollectionAssert.AreEqual(new[] { 100.0, 200.0, 300.0 }, m_values);
        }

        [TestMethod]
        public void Abort_DiscardsPendingTimerAndRejectsOldWatcherEvents()
        {
            StartWatch();
            FileSystemWatcher oldWatcher = Watcher;
            m_importer.FileSettleTimeMs = 100;
            string path = DeliverPair("aborted", 123);
            Notify(path);
            m_importer.Abort();
            Invoke("OnFileCreated", oldWatcher, new FileSystemEventArgs(WatcherChangeTypes.Created, m_watchDirectory, Path.GetFileName(path)));

            Assert.IsFalse(SpinWait.SpinUntil(() => State != FileImporterState.Idle, 300), "A delayed callback restarted the aborted replay.");
            Assert.IsNull(GetField("m_settleTimer"));
            Assert.IsNull(GetField("m_pendingFiles"));
            Assert.AreEqual(0, m_values.Count);
            Assert.IsTrue(File.Exists(path));
        }

        [TestMethod]
        public void MalformedRead_PreservesFailedPairAndContinuesWithNextReadyFile()
        {
            string failed = ComtradeTestDataHelper.CreateComtradeFiles(m_watchDirectory, "a_failed", 1, 0, 4, FileType.Ascii);
            string dat = Path.ChangeExtension(failed, ".dat");
            string[] records = File.ReadAllLines(dat);
            File.WriteAllLines(dat, new[] { records[0], records[1], "invalid,sample,value" });
            string good = CreatePair(m_watchDirectory, "b_good", 900);
            m_importer.DeleteAfterProcessing = true;
            string error = null;
            m_importer.ProcessException += (sender, args) => error = args.Argument.Message;
            StartWatch();

            Publish();

            CollectionAssert.AreEqual(new[] { 100.0, 200.0 }, m_values);
            StringAssert.Contains(error, "Replay failed for a_failed.cfg");
            Assert.IsTrue(File.Exists(failed) && File.Exists(dat));
            Assert.AreEqual(FileImporterState.Replaying, State);
            Publish();

            CollectionAssert.AreEqual(new[] { 100.0, 200.0, 900.0 }, m_values);
            Assert.AreEqual(FileImporterState.Watching, State);
            Assert.IsTrue(File.Exists(failed) && File.Exists(dat));
            Assert.IsFalse(File.Exists(good) || File.Exists(Path.ChangeExtension(good, ".dat")));
        }

        [TestMethod]
        public void DisconnectReconnect_PreservesWatchModeAndRejectsPreviousWatcher()
        {
            string first = CreatePair(m_watchDirectory, "a_first", 100);
            m_importer.FramesPerTick = 1;
            m_importer.DeleteAfterProcessing = true;
            m_importer.Start();
            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 5000));
            FileSystemWatcher previousWatcher = Watcher;
            previousWatcher.EnableRaisingEvents = false;
            Publish();
            m_importer.Stop();
            Assert.AreEqual(FileImporterState.Idle, State);
            Assert.IsNull(Watcher);
            Assert.IsNull(GetField("m_parser"));
            Assert.IsTrue(File.Exists(first));
            string second = DeliverPair("b_second", 200);
            Invoke("OnFileCreated", previousWatcher, new FileSystemEventArgs(WatcherChangeTypes.Created, m_watchDirectory, Path.GetFileName(second)));
            Assert.AreEqual(FileImporterState.Idle, State);

            m_importer.Start();
            Assert.IsTrue(SpinWait.SpinUntil(() => State == FileImporterState.Replaying, 5000));
            Watcher.EnableRaisingEvents = false;
            Assert.AreNotSame(previousWatcher, Watcher);
            Assert.IsTrue(string.IsNullOrEmpty(m_importer.FileName));
            Assert.AreEqual(m_watchDirectory, m_importer.WatchDirectory);
            m_importer.FramesPerTick = 10;
            Publish();
            Publish();

            CollectionAssert.AreEqual(new[] { 100.0, 100.0, 200.0 }, m_values);
            Assert.AreEqual(FileImporterState.Watching, State);
            Assert.IsFalse(File.Exists(first) || File.Exists(second));
        }

        [TestMethod]
        public void PublicStop_AfterCommandReplayClosesParserEvenWhenBaseIsDisabled()
        {
            string path = CreatePair(m_stagingDirectory, "command", 100);
            m_importer.ReplayFile(path);
            Assert.IsFalse(m_importer.Enabled);
            Assert.AreEqual(FileImporterState.Replaying, State);

            m_importer.Stop();
            Publish();

            Assert.AreEqual(FileImporterState.Idle, State);
            Assert.IsNull(GetField("m_parser"));
            Assert.AreEqual(0, m_values.Count);
            Assert.IsTrue(File.Exists(path));
        }

        [TestMethod]
        public void StopFromConnectionStatus_CancelsAlreadyQueuedBaseConnection()
        {
            using (ManualResetEventSlim stopped = new ManualResetEventSlim())
            {
                m_importer.StatusMessage += (sender, args) =>
                {
                    if (!args.Argument.StartsWith("Attempting connection", StringComparison.Ordinal))
                        return;

                    m_importer.Stop();
                    stopped.Set();
                };
                m_importer.Start();
                Assert.IsTrue(stopped.Wait(5000), "The base connection operation did not run.");
                Assert.IsFalse(SpinWait.SpinUntil(() => State != FileImporterState.Idle || m_importer.IsConnected, 300),
                    "The queued base connection restarted after the Stop command completed.");
                Assert.IsNull(Watcher);
                Assert.IsFalse(m_importer.Enabled);
            }
        }

        [TestMethod]
        public void AbortBeforeBaseConnectionTimer_CancelsDeferredStart()
        {
            typeof(InputAdapterBase).GetProperty("ConnectionAttemptInterval", PrivateInstance).SetValue(m_importer, 200.0);
            m_importer.Start();
            m_importer.Abort();

            Assert.IsFalse(SpinWait.SpinUntil(() => State != FileImporterState.Idle, 500),
                "The pending base connection timer restarted an aborted watch.");
            Assert.IsFalse(m_importer.Enabled);
            Assert.IsFalse(m_importer.IsConnected);
            Assert.IsNull(Watcher);
        }

        [TestMethod]
        public void Move_DatDestinationCollisionPreservesBothSourceFiles()
        {
            string cfg = CreatePair(m_watchDirectory, "collision", 100);
            string destination = Path.Combine(m_testDirectory, "processed");
            Directory.CreateDirectory(destination);
            string existingDat = Path.Combine(destination, "collision.dat");
            File.WriteAllText(existingDat, "existing data");
            m_importer.ProcessedDirectory = destination;
            m_importer.DeleteAfterProcessing = true;
            SetField("m_currentFilePath", cfg);

            string error = (string)Invoke("PostProcessFile");

            StringAssert.Contains(error, "Destination already exists");
            Assert.IsTrue(File.Exists(cfg));
            Assert.IsTrue(File.Exists(Path.ChangeExtension(cfg, ".dat")));
            Assert.IsFalse(File.Exists(Path.Combine(destination, "collision.cfg")));
            Assert.AreEqual("existing data", File.ReadAllText(existingDat));
        }

        [TestMethod]
        public void Move_TakesPrecedenceOverDelete_AndUsesCompletedPath()
        {
            string completed = CreatePair(m_watchDirectory, "completed", 100);
            string configured = CreatePair(m_watchDirectory, "configured", 200);
            string destination = Path.Combine(m_testDirectory, "processed");
            m_importer.FileName = configured;
            m_importer.ProcessedDirectory = destination;
            m_importer.DeleteAfterProcessing = true;
            SetField("m_currentFilePath", completed);

            Assert.IsNull(Invoke("PostProcessFile"));

            Assert.IsTrue(File.Exists(Path.Combine(destination, "completed.cfg")));
            Assert.IsTrue(File.Exists(Path.Combine(destination, "completed.dat")));
            Assert.IsFalse(File.Exists(completed));
            Assert.IsFalse(File.Exists(Path.ChangeExtension(completed, ".dat")));
            Assert.IsTrue(File.Exists(configured));
            Assert.IsTrue(File.Exists(Path.ChangeExtension(configured, ".dat")));
        }

        private string CreatePair(string directory, string name, double value)
        {
            return ComtradeTestDataHelper.CreateComtradeFiles(directory, name, 1, 0, 1, FileType.Ascii,
                analogValueGenerator: (sample, channel) => value);
        }

        private string DeliverPair(string name, double value)
        {
            string source = CreatePair(m_stagingDirectory, name, value);
            string destination = Path.Combine(m_watchDirectory, Path.GetFileName(source));
            File.Copy(source, destination);
            File.Copy(Path.ChangeExtension(source, ".dat"), Path.ChangeExtension(destination, ".dat"));
            return destination;
        }

        private void StartWatch()
        {
            Invoke("StartDirectoryWatch");
            // Deliver events explicitly so platform-specific FileSystemWatcher timing cannot
            // obscure the readiness, retry and invalidation behavior under test.
            Watcher.EnableRaisingEvents = false;
        }

        private void Notify(string path, WatcherChangeTypes change = WatcherChangeTypes.Created)
        {
            Invoke("OnFileCreated", Watcher, new FileSystemEventArgs(change, m_watchDirectory, Path.GetFileName(path)));
        }

        private void Publish()
        {
            Invoke("PublishNextFrame");
        }

        private FileImporterState State => (FileImporterState)GetField("m_state");
        private FileSystemWatcher Watcher => (FileSystemWatcher)GetField("m_fileWatcher");
        private Queue<string> Pending => (Queue<string>)GetField("m_pendingFiles");

        private object GetField(string name)
        {
            lock (typeof(FileImporter).GetField("m_syncRoot", PrivateInstance).GetValue(m_importer))
                return typeof(FileImporter).GetField(name, PrivateInstance).GetValue(m_importer);
        }

        private void SetField(string name, object value)
        {
            typeof(FileImporter).GetField(name, PrivateInstance).SetValue(m_importer, value);
        }

        private object Invoke(string name, params object[] arguments)
        {
            object result = null;
            Action action = () => result = typeof(FileImporter).GetMethod(name, PrivateInstance).Invoke(m_importer, arguments);
            typeof(FileImporter).GetMethod("ExecuteSynchronized", PrivateInstance).Invoke(m_importer, new object[] { action });
            return result;
        }
    }
}
