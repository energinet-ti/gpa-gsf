//******************************************************************************************************
//  TimestampCompatibilityTests.cs - Gbtc
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
using System.Linq;
using System.Reflection;
using GSF;
using GSF.COMTRADE;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class TimestampCompatibilityTests
    {
        private static readonly long StartTicks = new DateTime(2026, 1, 1, 12, 0, 0, DateTimeKind.Utc).Ticks;
        private FileImporter m_importer;
        private string m_directory;
        private List<IMeasurement> m_published;

        [TestInitialize]
        public void Setup()
        {
            m_directory = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(TimestampCompatibilityTests));
            m_importer = new FileImporter { Name = "Timestamp_" + Guid.NewGuid().ToString("N"), FramesPerTick = 20 };
            m_published = new List<IMeasurement>();
            m_importer.NewMeasurements += (sender, args) => m_published.AddRange(args.Argument);
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_directory);
        }

        [DataTestMethod]
        [DataRow(FileType.Ascii)]
        [DataRow(FileType.Binary)]
        [DataRow(FileType.Binary32)]
        [DataRow(FileType.Float32)]
        public void ExplicitTimestamps_ApplyCfgTimeFactorAndKeepAbsoluteStart(FileType format)
        {
            m_importer.InferTimeFromSampleRates = false;
            string path = WriteRecording(format, new uint[] { 1, 2, 3 }, new uint[] { 0, 100, 250 }, 3);

            Replay(path);

            AssertOffsets(0, 1000000, 2500000);
        }

        [DataTestMethod]
        [DataRow(FileType.Ascii)]
        [DataRow(FileType.Binary)]
        [DataRow(FileType.Binary32)]
        [DataRow(FileType.Float32)]
        public void InferredTimestamps_NormalizeArbitraryFirstIndexAndPreserveGaps(FileType format)
        {
            // First sample exceeds the declared count, and the final record skips indices.
            // Explicit timestamps are deliberately unusable for inferring the sample spacing.
            string path = WriteRecording(format, new uint[] { 50, 51, 55 }, new uint[] { 0, 0, 0 }, 6);

            Replay(path);

            AssertOffsets(0, 1000000, 5000000);
        }

        [DataTestMethod]
        [DataRow(FileType.Ascii)]
        [DataRow(FileType.Binary)]
        [DataRow(FileType.Binary32)]
        [DataRow(FileType.Float32)]
        public void InferredTimestamps_BeyondDeclaredEndRetainGsfExplicitFallback(FileType format)
        {
            string path = WriteRecording(format, new uint[] { 50, 51, 54 }, new uint[] { 0, 100, 700 }, 3);

            Replay(path);

            AssertOffsets(0, 1000000, 7000000);
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void Revision2013_AppliesUtcTimeCodeToAbsoluteTimestamps(bool infer)
        {
            m_importer.InferTimeFromSampleRates = infer;
            TimeOffset offset = new TimeOffset { Hours = -2, Minutes = 30 };
            string path = WriteRecording(FileType.Float32, new uint[] { 50, 51, 52 }, new uint[] { 0, 100, 250 }, 3,
                timeCode: offset);

            Replay(path);

            AssertOffsets(offset.TickOffset, offset.TickOffset + 1000000,
                offset.TickOffset + (infer ? 2000000 : 2500000));
        }

        [DataTestMethod]
        [DataRow(false)]
        [DataRow(true)]
        public void MultipleRates_PreserveGsfRateSelectionAndExplicitFallback(bool infer)
        {
            m_importer.InferTimeFromSampleRates = infer;
            SampleRate[] rates =
            {
                new SampleRate { Rate = 10.0D, EndSample = 1 },
                new SampleRate { Rate = 20.0D, EndSample = 3 }
            };
            string path = WriteRecording(FileType.Ascii, new uint[] { 50, 51, 52, 53, 54 },
                new uint[] { 0, 100, 200, 300, 700 }, 3, rates);

            Replay(path);

            // GSF selects the last qualifying rate rather than accumulating rate segments.
            // The importer does not change that selection.
            AssertOffsets(infer ? new long[] { 0, 500000, 1000000, 1500000, 7000000 }
                : new long[] { 0, 1000000, 2000000, 3000000, 7000000 });
        }

        [TestMethod]
        public void ZeroSampleRate_FallsBackToScaledExplicitTimestamps()
        {
            string path = WriteRecording(FileType.Ascii, new uint[] { 50, 51, 52 }, new uint[] { 0, 100, 250 }, 3,
                new[] { new SampleRate { Rate = 0.0D, EndSample = 3 } });

            Replay(path);

            AssertOffsets(0, 1000000, 2500000);
        }

        [DataTestMethod]
        [DataRow(FileType.Ascii)]
        [DataRow(FileType.Binary)]
        [DataRow(FileType.Binary32)]
        [DataRow(FileType.Float32)]
        public void SegmentedReplay_KeepsSampleNumbersAlignedAndResetsOnReload(FileType format)
        {
            string path = WriteRecording(format, new uint[] { 50, 51, 55 }, new uint[] { 0, 0, 0 }, 6);
            string dat = Path.ChangeExtension(path, ".dat");
            string segment = Path.ChangeExtension(path, ".d00");
            // GSF orders .d00 before .dat and skips a malformed final ASCII line.
            if (format == FileType.Ascii)
            {
                string[] lines = File.ReadAllLines(dat);
                File.WriteAllLines(segment, new[] { lines[0], "" });
                File.WriteAllLines(dat, lines.Skip(1));
            }
            else
            {
                byte[] data = File.ReadAllBytes(dat);
                int length = format == FileType.Binary ? 10 : 12;
                File.WriteAllBytes(segment, data.Take(length).ToArray());
                File.WriteAllBytes(dat, data.Skip(length).ToArray());
            }

            Replay(path);
            AssertOffsets(0, 1000000, 5000000);
            m_published.Clear();
            Replay(path);
            AssertOffsets(0, 1000000, 5000000);
        }

        private string WriteRecording(FileType format, uint[] samples, uint[] timestamps, int endSample,
            SampleRate[] rates = null, TimeOffset timeCode = null)
        {
            ChannelMetadata[] channels =
            {
                new ChannelMetadata { Name = "Station:Voltage", Units = "V", SignalType = GSF.Units.EE.SignalType.ALOG }
            };
            Schema schema = Writer.CreateSchema(channels, "TestStation", "TestDevice", StartTicks, endSample,
                2013, format, 1000.0D, 10.0D, 50.0D, false);
            schema.AnalogChannels[0].Multiplier = 1.0D;
            schema.AnalogChannels[0].Adder = 0.0D;
            if (rates != null)
                schema.SampleRates = rates;
            if (timeCode != null)
                schema.TimeCode = timeCode;
            string path = Path.Combine(m_directory, "timestamps.cfg");
            File.WriteAllText(path, schema.FileImage);
            string dat = Path.ChangeExtension(path, ".dat");
            if (format == FileType.Ascii)
            {
                File.WriteAllLines(dat, samples.Select((sample, i) => string.Format(CultureInfo.InvariantCulture,
                    "{0},{1},{2}", sample, timestamps[i], 100 + i)));
            }
            else
            {
                using (BinaryWriter writer = new BinaryWriter(File.Create(dat)))
                {
                    for (int i = 0; i < samples.Length; i++)
                    {
                        writer.Write(samples[i]);
                        writer.Write(timestamps[i]);
                        if (format == FileType.Binary)
                            writer.Write((short)(100 + i));
                        else if (format == FileType.Binary32)
                            writer.Write(100 + i);
                        else
                            writer.Write((float)(100 + i));
                    }
                }
            }
            return path;
        }

        private void Replay(string path)
        {
            const BindingFlags flags = BindingFlags.Instance | BindingFlags.NonPublic;
            typeof(FileImporter).GetMethod("LoadFile", flags).Invoke(m_importer, new object[] { path });
            typeof(FileImporter).GetMethod("PublishNextFrame", flags).Invoke(m_importer, null);
            StringAssert.Contains(m_importer.GetStatus(), "Completed");
        }

        private void AssertOffsets(params long[] offsets)
        {
            CollectionAssert.AreEqual(offsets.Select(offset => StartTicks + offset).ToArray(),
                m_published.Select(measurement => (long)measurement.Timestamp).ToArray());
            CollectionAssert.AreEqual(Enumerable.Range(100, offsets.Length).Select(value => (double)value).ToArray(),
                m_published.Select(measurement => measurement.Value).ToArray());
        }
    }
}
