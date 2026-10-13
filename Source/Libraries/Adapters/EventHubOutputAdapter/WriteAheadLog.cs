//******************************************************************************************************
//  WriteAheadLog.cs - Gbtc
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
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;

namespace EventHubOutputAdapter
{
    /// <summary>
    /// Self-contained write-ahead log file manager. Handles WAL file creation, reading,
    /// deletion, replay discovery, and size limit enforcement. No GSF dependency.
    /// </summary>
    internal class WriteAheadLog
    {
        #region [ Members ]

        // Fields
        private readonly string m_walPath;
        private readonly object m_fileLock = new object();
        private readonly HashSet<string> m_activeFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        private long m_walSequence;
        private long m_droppedBatches;

        #endregion

        #region [ Constructors ]

        /// <summary>
        /// Creates a new WAL manager for the specified directory, creating it if needed.
        /// </summary>
        /// <param name="walPath">Directory for persisted batch files.</param>
        public WriteAheadLog(string walPath)
        {
            m_walPath = walPath;
            Directory.CreateDirectory(m_walPath);
        }

        #endregion

        #region [ Properties ]

        /// <summary>
        /// Gets the WAL directory path.
        /// </summary>
        public string WalPath => m_walPath;

        /// <summary>
        /// Gets the number of batches dropped due to WAL size limit enforcement.
        /// </summary>
        public long DroppedBatches => Interlocked.Read(ref m_droppedBatches);

        /// <summary>
        /// Gets the current number of pending WAL files on disk, or zero if the directory cannot be read.
        /// </summary>
        public int PendingFileCount
        {
            get
            {
                try
                {
                    if (Directory.Exists(m_walPath))
                        return Directory.GetFiles(m_walPath, "*.wal").Length;
                }
                catch
                {
                    // Status reporting must not fail when the directory cannot be inspected.
                }

                return 0;
            }
        }

        #endregion

        #region [ Methods ]

        /// <summary>
        /// Writes and closes a batch body before publishing a new WAL file.
        /// </summary>
        /// <param name="batchBody">Serialized REST batch body to persist.</param>
        /// <param name="acquire">Whether to reserve the published file for its current sender.</param>
        /// <returns>Path of the completed WAL file.</returns>
        public string Write(string batchBody, bool acquire = false)
        {
            long seq = Interlocked.Increment(ref m_walSequence);
            string filePath = Path.Combine(m_walPath, $"{seq:D10}.wal");
            string temporaryPath = filePath + "." + Guid.NewGuid().ToString("N") + ".tmp";

            try
            {
                using (FileStream stream = new FileStream(temporaryPath, FileMode.CreateNew, FileAccess.Write, FileShare.None))
                using (StreamWriter writer = new StreamWriter(stream, new UTF8Encoding(false, true)))
                    writer.Write(batchBody);

                lock (m_fileLock)
                {
                    // The rename never replaces an existing completed file. Retention cannot run
                    // between publication and ownership assignment for an active send.
                    File.Move(temporaryPath, filePath);

                    if (acquire)
                        m_activeFiles.Add(Path.GetFullPath(filePath));
                }

                return filePath;
            }
            finally
            {
                // A failed write or rename must not leave an unfinished batch for replay.
                // Cleanup must not hide the original storage error.
                try
                {
                    File.Delete(temporaryPath);
                }
                catch (IOException)
                {
                }
                catch (UnauthorizedAccessException)
                {
                }
            }
        }

        /// <summary>
        /// Reserves an existing WAL file for one sender, protecting it from retention.
        /// </summary>
        /// <param name="filePath">Path of the completed WAL file to reserve.</param>
        /// <returns>Whether the file exists and was reserved by this call.</returns>
        public bool TryAcquire(string filePath)
        {
            lock (m_fileLock)
            {
                try
                {
                    if ((File.GetAttributes(filePath) & FileAttributes.Directory) != 0)
                        return false;
                }
                catch (FileNotFoundException)
                {
                    return false;
                }
                catch (DirectoryNotFoundException)
                {
                    return false;
                }

                return m_activeFiles.Add(Path.GetFullPath(filePath));
            }
        }

        /// <summary>
        /// Releases a sender's reservation, allowing the file to be retried or removed by retention.
        /// </summary>
        /// <param name="filePath">Path of the WAL file to release.</param>
        public void Release(string filePath)
        {
            lock (m_fileLock)
                m_activeFiles.Remove(Path.GetFullPath(filePath));
        }

        /// <summary>
        /// Reads the contents of a WAL file.
        /// </summary>
        /// <param name="filePath">Path of the WAL file to read.</param>
        /// <returns>Persisted REST batch body.</returns>
        public string Read(string filePath)
        {
            return File.ReadAllText(filePath, System.Text.Encoding.UTF8);
        }

        /// <summary>
        /// Deletes a WAL file, reporting storage errors to the caller.
        /// </summary>
        /// <param name="filePath">Path of the WAL file to delete.</param>
        public void Delete(string filePath)
        {
            lock (m_fileLock)
                File.Delete(filePath);
        }

        /// <summary>
        /// Finds completed WAL files for replay.
        /// </summary>
        /// <returns>WAL file paths in ordinal order, or an empty array if none exist.</returns>
        public string[] GetPendingFiles()
        {
            if (!Directory.Exists(m_walPath))
                return Array.Empty<string>();

            string[] files = Directory.GetFiles(m_walPath, "*.wal");
            Array.Sort(files, StringComparer.Ordinal);

            return files;
        }

        /// <summary>
        /// Sets the internal sequence counter past the given value, ensuring new WAL files
        /// don't collide with replayed files.
        /// </summary>
        /// <param name="seq">Highest sequence number discovered during replay.</param>
        public void SetSequencePast(long seq)
        {
            // Use Interlocked to safely set only if the new value is higher
            long current;

            do
            {
                current = Interlocked.Read(ref m_walSequence);

                if (seq <= current)
                    return;
            }
            while (Interlocked.CompareExchange(ref m_walSequence, seq, current) != current);
        }

        /// <summary>
        /// Enforces a WAL directory size limit by deleting the oldest files when the total
        /// size exceeds the specified cap. Increments <see cref="DroppedBatches"/> for each
        /// deleted file. Files owned by a sender are retained even when they exceed the cap.
        /// Storage errors are reported to the caller.
        /// </summary>
        /// <param name="maxSizeMB">Maximum retained size in megabytes.</param>
        public void EnforceSizeLimit(long maxSizeMB)
        {
            lock (m_fileLock)
            {
                if (!Directory.Exists(m_walPath))
                    return;

                string[] files = Directory.GetFiles(m_walPath, "*.wal");
                long totalBytes = 0;

                foreach (string filePath in files)
                {
                    try
                    {
                        totalBytes += new FileInfo(filePath).Length;
                    }
                    catch (FileNotFoundException)
                    {
                    }
                    catch (DirectoryNotFoundException)
                    {
                    }
                }

                long limitBytes = maxSizeMB * 1024 * 1024;

                if (totalBytes <= limitBytes)
                    return;

                Array.Sort(files, StringComparer.Ordinal);

                foreach (string filePath in files)
                {
                    if (totalBytes <= limitBytes)
                        break;

                    if (m_activeFiles.Contains(Path.GetFullPath(filePath)))
                        continue;

                    try
                    {
                        long size = new FileInfo(filePath).Length;
                        File.Delete(filePath);
                        totalBytes -= size;
                        Interlocked.Increment(ref m_droppedBatches);
                    }
                    catch (FileNotFoundException)
                    {
                    }
                    catch (DirectoryNotFoundException)
                    {
                    }
                }
            }
        }

        #endregion
    }
}
