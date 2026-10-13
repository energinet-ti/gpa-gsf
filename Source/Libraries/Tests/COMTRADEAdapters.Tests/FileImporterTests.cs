//******************************************************************************************************
//  FileImporterTests.cs - Gbtc
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

using System.IO;
using System.Reflection;
using GSF.COMTRADE;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    /// <summary>
    /// Unit tests for <see cref="FileImporter"/> covering state transitions,
    /// file loading (ASCII/Binary/Float32), frame publishing, post-processing (delete/move),
    /// end-of-file handling, and auto-repeat behavior. Uses reflection to invoke private methods
    /// and inspect internal state without a running adapter host.
    /// </summary>
    [TestClass]
    public class FileImporterTests
    {
        private FileImporter m_importer;
        private string m_testDir;

        [TestInitialize]
        public void Setup()
        {
            m_importer = new FileImporter();
            m_testDir = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(FileImporterTests));
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer?.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_testDir);
        }

        #region [ State Tests ]

        [TestMethod]
        public void DefaultState_IsIdle()
        {
            // Arrange - fresh instance created in Setup

            // Act
            FileImporterState state = GetState();

            // Assert
            Assert.AreEqual(FileImporterState.Idle, state);
        }

        [TestMethod]
        public void Abort_ResetsToIdle()
        {
            // Arrange
            SetState(FileImporterState.Replaying);

            // Act
            m_importer.Abort();

            // Assert
            Assert.AreEqual(FileImporterState.Idle, GetState());
        }

        #endregion

        #region [ GetChannelMap Tests ]

        [TestMethod]
        public void GetChannelMap_NoSchema_ReturnsNoChannelMap()
        {
            // Arrange - fresh instance with no schema loaded

            // Act
            string result = m_importer.GetChannelMap();

            // Assert
            Assert.AreEqual("No channel map loaded", result);
        }

        #endregion

        #region [ ReplayFile Tests ]

        [TestMethod]
        public void ReplayFile_ValidFile_TransitionsToReplaying()
        {
            // Arrange
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "replay_valid", analogChannelCount: 2, digitalChannelCount: 0,
                sampleCount: 10, fileType: FileType.Float32);

            // Act
            m_importer.ReplayFile(cfgPath);

            // Assert
            Assert.AreEqual(FileImporterState.Replaying, GetState());
        }

        [TestMethod]
        public void ReplayFile_MissingFile_TransitionsToError()
        {
            // Arrange
            string missingPath = Path.Combine(m_testDir, "nonexistent.cfg");

            // Act
            m_importer.ReplayFile(missingPath);

            // Assert
            Assert.AreEqual(FileImporterState.Error, GetState());
        }

        [TestMethod]
        public void ReplayFile_SetsFileName()
        {
            // Arrange
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "replay_setname", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 5, fileType: FileType.Float32);

            // Act
            m_importer.ReplayFile(cfgPath);

            // Assert
            Assert.AreEqual(cfgPath, m_importer.FileName);
        }

        [TestMethod]
        public void ReplayFile_ReplacesActiveReplay_ResetsFrameCounter()
        {
            // Arrange - load and partially replay a file
            string firstPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "replay_first", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 10, fileType: FileType.Float32);
            InvokeLoadFile(firstPath);
            InvokePublishNextFrame();
            Assert.AreEqual(1L, (long)GetField("m_framesPublished"), "Precondition: one frame published");

            // Create second file
            string secondPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "replay_second", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 5, fileType: FileType.Float32);

            // Act
            m_importer.ReplayFile(secondPath);

            // Assert
            Assert.AreEqual(FileImporterState.Replaying, GetState());
            Assert.AreEqual(0L, (long)GetField("m_framesPublished"), "Frame counter should reset for new file");
            Assert.AreEqual(5L, (long)GetField("m_totalFrames"), "Total frames should match second file");
        }

        #endregion

        #region [ Properties Tests ]

        [TestMethod]
        public void FramesPerTick_ClampsToMinimumOfOne()
        {
            // Arrange & Act & Assert
            m_importer.FramesPerTick = 0;
            Assert.AreEqual(1, m_importer.FramesPerTick);

            m_importer.FramesPerTick = -5;
            Assert.AreEqual(1, m_importer.FramesPerTick);

            m_importer.FramesPerTick = 1;
            Assert.AreEqual(1, m_importer.FramesPerTick);
        }

        #endregion

        #region [ LoadFile Tests ]

        [DataTestMethod]
        [DataRow(FileType.Ascii)]
        [DataRow(FileType.Binary)]
        public void LoadAndPublish_SupportedFormat_PublishesOneFrame(FileType fileType)
        {
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "load_publish", analogChannelCount: 2, digitalChannelCount: 0,
                sampleCount: 10, fileType: fileType);
            int publishedMeasurements = 0;
            m_importer.NewMeasurements += (sender, args) => publishedMeasurements += args.Argument.Count;

            InvokeLoadFile(cfgPath);
            Assert.AreEqual(FileImporterState.Replaying, GetState());
            InvokePublishNextFrame();

            Assert.AreEqual(1L, (long)GetField("m_framesPublished"));
            Assert.AreEqual(2, publishedMeasurements, "Both analog channels should be published.");
        }

        [TestMethod]
        public void LoadFile_MissingCfg_TransitionsToError()
        {
            // Arrange
            string missingPath = Path.Combine(m_testDir, "nonexistent.cfg");

            // Act
            InvokeLoadFile(missingPath);

            // Assert
            Assert.AreEqual(FileImporterState.Error, GetState());
        }

        [TestMethod]
        public void LoadFile_MissingDat_TransitionsToError()
        {
            // Arrange
            string cfgPath = Path.Combine(m_testDir, "nodat.cfg");
            File.WriteAllText(cfgPath, "dummy cfg content");

            // Act
            InvokeLoadFile(cfgPath);

            // Assert
            Assert.AreEqual(FileImporterState.Error, GetState());
            string lastError = (string)GetField("m_lastError");
            Assert.IsTrue(lastError.Contains("DAT file not found"),
                $"Expected error about missing DAT file. Got: '{lastError}'");
        }

        [TestMethod]
        public void LoadFile_SetsFrameCount()
        {
            // Arrange
            const int sampleCount = 42;
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "framecount", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: sampleCount, fileType: FileType.Float32);

            // Act
            InvokeLoadFile(cfgPath);

            // Assert
            long totalFrames = (long)GetField("m_totalFrames");
            Assert.AreEqual(sampleCount, totalFrames);
        }

        #endregion

        #region [ PostProcessFile Tests ]

        [TestMethod]
        public void PostProcessFile_Delete_RemovesBothFiles()
        {
            // Arrange
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "delete_test", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 5, fileType: FileType.Float32);
            string datPath = Path.ChangeExtension(cfgPath, ".dat");
            SetField("m_currentFilePath", cfgPath);
            SetField("m_currentFileName", Path.GetFileName(cfgPath));
            m_importer.DeleteAfterProcessing = true;
            m_importer.ProcessedDirectory = null;

            // Act
            InvokePostProcessFile();

            // Assert
            Assert.IsFalse(File.Exists(cfgPath), "CFG file should be deleted");
            Assert.IsFalse(File.Exists(datPath), "DAT file should be deleted");
        }

        [TestMethod]
        public void PostProcessFile_Move_MovesBothFiles()
        {
            // Arrange
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "move_test", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: 5, fileType: FileType.Float32);
            string datPath = Path.ChangeExtension(cfgPath, ".dat");
            string processedDir = Path.Combine(m_testDir, "processed");
            Directory.CreateDirectory(processedDir);
            SetField("m_currentFilePath", cfgPath);
            SetField("m_currentFileName", Path.GetFileName(cfgPath));
            m_importer.ProcessedDirectory = processedDir;
            m_importer.DeleteAfterProcessing = false;

            // Act
            InvokePostProcessFile();

            // Assert
            Assert.IsFalse(File.Exists(cfgPath), "Original CFG file should be moved");
            Assert.IsFalse(File.Exists(datPath), "Original DAT file should be moved");
            Assert.IsTrue(File.Exists(Path.Combine(processedDir, "move_test.cfg")), "CFG should exist in processed dir");
            Assert.IsTrue(File.Exists(Path.Combine(processedDir, "move_test.dat")), "DAT should exist in processed dir");
        }

        #endregion

        #region [ AutoRepeat Tests ]

        [TestMethod]
        public void AutoRepeat_ReloadsFileOnEof()
        {
            // Arrange
            const int sampleCount = 3;
            string cfgPath = ComtradeTestDataHelper.CreateComtradeFiles(
                m_testDir, "repeat_test", analogChannelCount: 1, digitalChannelCount: 0,
                sampleCount: sampleCount, fileType: FileType.Float32);
            InvokeLoadFile(cfgPath);
            m_importer.AutoRepeat = true;

            // Act - exhaust all frames to trigger EOF + AutoRepeat reload
            for (int i = 0; i <= sampleCount; i++)
                InvokePublishNextFrame();

            // Assert - should have reloaded and be back in Replaying state
            Assert.AreEqual(FileImporterState.Replaying, GetState());
            long framesPublished = (long)GetField("m_framesPublished");
            Assert.AreEqual(0L, framesPublished, "Frame counter should reset after reload");
        }

        #endregion

        #region [ Reflection Helpers ]

        private static readonly BindingFlags NonPublicInstance = BindingFlags.NonPublic | BindingFlags.Instance;

        private static FieldInfo GetFieldInfo(string fieldName)
        {
            FieldInfo field = typeof(FileImporter).GetField(fieldName, NonPublicInstance);
            Assert.IsNotNull(field, $"Field '{fieldName}' not found on FileImporter");
            return field;
        }

        private static MethodInfo GetMethodInfo(string methodName)
        {
            MethodInfo method = typeof(FileImporter).GetMethod(methodName, NonPublicInstance);
            Assert.IsNotNull(method, $"Method '{methodName}' not found on FileImporter");
            return method;
        }

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

        private void InvokeLoadFile(string cfgFilePath)
        {
            GetMethodInfo("LoadFile").Invoke(m_importer, new object[] { cfgFilePath });
        }

        private void InvokePublishNextFrame()
        {
            GetMethodInfo("PublishNextFrame").Invoke(m_importer, null);
        }

        private void InvokePostProcessFile()
        {
            GetMethodInfo("PostProcessFile").Invoke(m_importer, null);
        }

        #endregion
    }
}
