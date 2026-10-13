//******************************************************************************************************
//  WriteAheadLogTests.cs - Gbtc
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
using System.IO;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace EventHubOutputAdapter.Tests
{
    /// <summary>
    /// Verifies completed file publication and ownership during retention.
    /// </summary>
    [TestClass]
    public class WriteAheadLogTests
    {
        #region [ Members ]

        private string m_directory;

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Creates a unique WAL directory path for each test.
        /// </summary>
        [TestInitialize]
        public void Initialize()
        {
            m_directory = Path.Combine(Path.GetTempPath(), "EventHubWalTests-" + Guid.NewGuid().ToString("N"));
        }

        /// <summary>
        /// Removes files created by the WAL test.
        /// </summary>
        [TestCleanup]
        public void Cleanup()
        {
            if (Directory.Exists(m_directory))
                Directory.Delete(m_directory, true);
        }

        /// <summary>
        /// Publishes the complete body under a numeric filename while ignoring unfinished files.
        /// </summary>
        [TestMethod]
        public void WritePublishesCompleteBodyWithNumericName()
        {
            WriteAheadLog wal = new WriteAheadLog(m_directory);
            const string Body = "[{\"Body\":\"PMU_Ünïcödé\"}]";
            wal.SetSequencePast(41);

            string path = wal.Write(Body);

            Assert.AreEqual("0000000042.wal", Path.GetFileName(path));
            Assert.AreEqual(Body, wal.Read(path));
            CollectionAssert.AreEqual(Encoding.UTF8.GetBytes(Body), File.ReadAllBytes(path));
            CollectionAssert.AreEqual(new[] { path }, wal.GetPendingFiles());
            CollectionAssert.AreEqual(new[] { path }, Directory.GetFiles(m_directory));

            File.WriteAllText(Path.Combine(m_directory, "0000000043.wal.unfinished.tmp"), "partial body");
            CollectionAssert.AreEqual(new[] { path }, wal.GetPendingFiles());
        }

        /// <summary>
        /// Preserves a completed file and removes the temporary file when publication fails.
        /// </summary>
        [TestMethod]
        public void WriteFailedPublicationPreservesExistingFileAndRemovesTemporaryFile()
        {
            WriteAheadLog first = new WriteAheadLog(m_directory);
            string path = first.Write("existing batch");
            WriteAheadLog restarted = new WriteAheadLog(m_directory);

            // A stale sequence must fail safely instead of replacing a completed batch.
            Assert.ThrowsException<IOException>(() => restarted.Write("replacement batch", acquire: true));

            Assert.AreEqual("existing batch", first.Read(path));
            CollectionAssert.AreEqual(new[] { path }, Directory.GetFiles(m_directory));
            Assert.IsTrue(restarted.TryAcquire(path), "Failed publication must not reserve the existing file.");
        }

        /// <summary>
        /// Protects the active WAL file and removes the oldest eligible files during retention.
        /// </summary>
        [TestMethod]
        public void EnforceSizeLimitPreservesActiveFileAndDeletesOldestAvailableFile()
        {
            WriteAheadLog wal = new WriteAheadLog(m_directory);
            string body = new string('x', 400 * 1024);
            string active = wal.Write(body, acquire: true);
            string oldest = wal.Write(body);
            string newest = wal.Write(body);

            Assert.IsFalse(wal.TryAcquire(active), "Publication must reserve the file for its sender.");
            wal.EnforceSizeLimit(1);

            Assert.IsFalse(File.Exists(oldest));
            Assert.IsTrue(File.Exists(active));
            Assert.IsTrue(File.Exists(newest));
            Assert.AreEqual(1L, wal.DroppedBatches);
            Assert.IsFalse(wal.TryAcquire(oldest));

            wal.Release(active);
            wal.EnforceSizeLimit(0);

            Assert.AreEqual(0, wal.PendingFileCount);
            Assert.AreEqual(3L, wal.DroppedBatches);
        }

        #endregion
    }
}
