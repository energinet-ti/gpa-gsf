//******************************************************************************************************
//  DirectoryWatchTests.cs - Gbtc
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
//  02/20/2026 - Frederik Fast
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
using System.IO;
using System.Reflection;
using GSF.COMTRADE;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    /// <summary>
    /// Unit tests for the directory watch functionality of <see cref="FileImporter"/>.
    /// Covers starting a watch with existing/empty/nonexistent directories, the settle timer
    /// debounce behavior for files arriving in quick succession, and the
    /// pending file queue processing including error recovery.
    /// </summary>
    [TestClass]
    public class DirectoryWatchTests
    {
        private FileImporter m_importer;
        private string m_testDir;

        [TestInitialize]
        public void Setup()
        {
            m_importer = new FileImporter { FileSettleTimeMs = 0, ProcessingInterval = 60000 };
            m_testDir = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(DirectoryWatchTests));
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer?.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_testDir);
        }

        #region [ StartDirectoryWatch Tests ]

        [TestMethod]
        public void StartDirectoryWatch_ExistingFiles_QueuesAndProcessesFirst()
        {
            // Arrange - create two COMTRADE files before starting the watch
            ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "file_a", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 3, fileType: FileType.Float32);
            ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "file_b", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 3, fileType: FileType.Float32);

            m_importer.WatchDirectory = m_testDir;

            // Act
            InvokeMethod("StartDirectoryWatch");

            // Assert - first file should be replaying, second should remain queued
            Assert.AreEqual(FileImporterState.Replaying, GetState(),
                "Should be replaying the first queued file");
            var pendingFiles = (Queue<string>)GetField("m_pendingFiles");
            Assert.AreEqual(1, pendingFiles.Count,
                "Second file should remain in queue while first is replaying");
        }

        [TestMethod]
        public void StartDirectoryWatch_EmptyDirectory_TransitionsToWatching()
        {
            // Arrange
            string emptyDir = Path.Combine(m_testDir, "empty");
            Directory.CreateDirectory(emptyDir);
            m_importer.WatchDirectory = emptyDir;

            // Act
            InvokeMethod("StartDirectoryWatch");

            // Assert
            Assert.AreEqual(FileImporterState.Watching, GetState());
        }

        [TestMethod]
        public void StartDirectoryWatch_NonexistentDirectory_TransitionsToError()
        {
            // Arrange
            m_importer.WatchDirectory = Path.Combine(m_testDir, "nonexistent");

            // Act
            InvokeMethod("StartDirectoryWatch");

            // Assert
            Assert.AreEqual(FileImporterState.Error, GetState());
        }

        #endregion

        #region [ OnFileCreated Tests ]

        [TestMethod]
        public void OnFileCreated_MultipleRapidFiles_AllEnqueued()
        {
            // Arrange - simulate an active directory watch
            var pendingFiles = new Queue<string>();
            SetField("m_pendingFiles", pendingFiles);
            SetState(FileImporterState.Replaying);
            m_importer.FileSettleTimeMs = 500;

            string file1 = Path.Combine(m_testDir, "rapid1.cfg");
            string file2 = Path.Combine(m_testDir, "rapid2.cfg");
            string file3 = Path.Combine(m_testDir, "rapid3.cfg");

            // Act - simulate three files arriving in rapid succession
            InvokeOnFileCreated(file1);
            InvokeOnFileCreated(file2);
            InvokeOnFileCreated(file3);

            // Assert - all three files should be enqueued immediately (not lost)
            Assert.AreEqual(3, pendingFiles.Count,
                $"All files must be enqueued even when arriving within the settle window. Got {pendingFiles.Count}.");
        }

        #endregion

        #region [ ProcessNextPendingFile Tests ]

        [TestMethod]
        public void ProcessNextPendingFile_EmptyQueue_TransitionsToWatching()
        {
            // Arrange
            SetField("m_pendingFiles", new Queue<string>());
            SetState(FileImporterState.Completed);

            // Act
            InvokeMethod("ProcessNextPendingFile");

            // Assert
            Assert.AreEqual(FileImporterState.Watching, GetState());
        }

        [TestMethod]
        public void ProcessNextPendingFile_InvalidFile_SkipsToNext()
        {
            // Arrange - queue with a bad file followed by a valid file
            var pendingFiles = new Queue<string>();

            string badCfg = Path.Combine(m_testDir, "bad.cfg");
            File.WriteAllText(badCfg, "not valid comtrade content");
            File.WriteAllText(Path.Combine(m_testDir, "bad.dat"), "dummy data");

            string goodCfg = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "good", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 3, fileType: FileType.Float32);

            pendingFiles.Enqueue(badCfg);
            pendingFiles.Enqueue(goodCfg);
            SetField("m_pendingFiles", pendingFiles);
            SetState(FileImporterState.Watching);

            // Act
            InvokeMethod("ProcessNextPendingFile");

            // Assert - should have skipped the bad file and loaded the good one
            Assert.AreEqual(FileImporterState.Replaying, GetState(),
                "Should be replaying after skipping the invalid file");
        }

        #endregion

        #region [ Reflection Helpers ]

        private static readonly BindingFlags NonPublicInstance = BindingFlags.NonPublic | BindingFlags.Instance;

        private FileImporterState GetState()
        {
            return (FileImporterState)GetFieldInfo("m_state").GetValue(m_importer);
        }

        private void SetState(FileImporterState state)
        {
            GetFieldInfo("m_state").SetValue(m_importer, state);
        }

        private object GetField(string fieldName)
        {
            return GetFieldInfo(fieldName).GetValue(m_importer);
        }

        private void SetField(string fieldName, object value)
        {
            GetFieldInfo(fieldName).SetValue(m_importer, value);
        }

        private static FieldInfo GetFieldInfo(string fieldName)
        {
            FieldInfo field = typeof(FileImporter).GetField(fieldName, NonPublicInstance);
            Assert.IsNotNull(field, $"Field '{fieldName}' not found on FileImporter");
            return field;
        }

        private void InvokeMethod(string methodName)
        {
            MethodInfo method = typeof(FileImporter).GetMethod(methodName, NonPublicInstance);
            Assert.IsNotNull(method, $"Method '{methodName}' not found on FileImporter");
            method.Invoke(m_importer, null);
        }

        private void InvokeOnFileCreated(string filePath)
        {
            MethodInfo method = typeof(FileImporter).GetMethod("OnFileCreated", NonPublicInstance);
            Assert.IsNotNull(method, "Method 'OnFileCreated' not found on FileImporter");

            string directory = Path.GetDirectoryName(filePath) ?? "";
            string name = Path.GetFileName(filePath);
            var args = new FileSystemEventArgs(WatcherChangeTypes.Created, directory, name);
            method.Invoke(m_importer, new object[] { null, args });
        }

        #endregion
    }
}
