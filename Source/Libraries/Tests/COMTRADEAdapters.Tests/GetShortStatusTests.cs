//******************************************************************************************************
//  GetShortStatusTests.cs - Gbtc
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

using System.Reflection;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class GetShortStatusTests
    {
        private FileImporter m_importer;

        [TestInitialize]
        public void Setup()
        {
            m_importer = new FileImporter();
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer?.Dispose();
        }

        [TestMethod]
        public void Completed_WithStatusString_ContainsCompleted()
        {
            // Arrange
            SetState(FileImporterState.Completed);
            SetField("m_completedStatus", "Completed test.cfg - 100 frames published in 00:00:01");

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.IsTrue(result.Contains("Completed"),
                $"Full completed status must still contain 'Completed'. Got: '{result}'");
        }

        [TestMethod]
        public void Completed_NullStatus_FallsBackToCompleted()
        {
            // Arrange
            SetState(FileImporterState.Completed);
            SetField("m_completedStatus", null);

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.AreEqual("Completed", result);
        }

        [TestMethod]
        public void Idle_ReturnsIdle()
        {
            // Arrange
            SetState(FileImporterState.Idle);

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.AreEqual("Idle", result);
        }

        [TestMethod]
        public void Replaying_ContainsFrameCount()
        {
            // Arrange
            SetState(FileImporterState.Replaying);
            SetField("m_currentFileName", "test.cfg");
            SetField("m_framesPublished", 50L);
            SetField("m_totalFrames", 100L);

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.IsTrue(result.Contains("50/100"), $"Expected frame count '50/100' in: '{result}'");
            Assert.IsTrue(result.Contains("50%"), $"Expected percentage '50%' in: '{result}'");
        }

        [TestMethod]
        public void Replaying_ZeroTotalFrames_NoDivisionByZero()
        {
            // Arrange
            SetState(FileImporterState.Replaying);
            SetField("m_currentFileName", "test.cfg");
            SetField("m_framesPublished", 0L);
            SetField("m_totalFrames", 0L);

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.IsTrue(result.Contains("0%"), $"Expected '0%' in: '{result}'");
        }

        [TestMethod]
        public void Error_ContainsErrorMessage()
        {
            // Arrange
            SetState(FileImporterState.Error);
            SetField("m_lastError", "File not found");

            // Act
            string result = m_importer.GetShortStatus(256);

            // Assert
            Assert.IsTrue(result.Contains("File not found"), $"Expected error message in: '{result}'");
        }

        [TestMethod]
        public void NonCompleted_DoesNotContainCompleted()
        {
            // Arrange
            FileImporterState[] nonCompletedStates = {
                FileImporterState.Idle,
                FileImporterState.Loading,
                FileImporterState.Replaying,
                FileImporterState.Watching,
                FileImporterState.Error
            };

            foreach (FileImporterState state in nonCompletedStates)
            {
                SetState(state);
                SetField("m_currentFileName", "test.cfg");
                SetField("m_lastError", "some error");
                m_importer.WatchDirectory = @"C:\Incoming";
                SetField("m_framesPublished", 0L);
                SetField("m_totalFrames", 100L);

                // Act
                string result = m_importer.GetShortStatus(256);

                // Assert
                Assert.IsFalse(result.Contains("Completed"),
                    $"State {state} must not contain 'Completed' to prevent false positives. Got: '{result}'");
            }
        }

        [TestMethod]
        public void MaxLength_Truncates()
        {
            // Arrange
            SetState(FileImporterState.Replaying);
            SetField("m_currentFileName", "very_long_filename_that_should_be_truncated.cfg");
            SetField("m_framesPublished", 50L);
            SetField("m_totalFrames", 100L);

            // Act
            string result = m_importer.GetShortStatus(10);

            // Assert
            Assert.IsTrue(result.Length <= 10,
                $"Result length {result.Length} exceeds maxLength 10: '{result}'");
        }

        private void SetState(FileImporterState state)
        {
            FieldInfo field = typeof(FileImporter).GetField("m_state", BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(field, "Field 'm_state' not found on FileImporter");
            field.SetValue(m_importer, state);
        }

        private void SetField(string fieldName, object value)
        {
            FieldInfo field = typeof(FileImporter).GetField(fieldName, BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.IsNotNull(field, $"Field '{fieldName}' not found on FileImporter");
            field.SetValue(m_importer, value);
        }
    }
}
