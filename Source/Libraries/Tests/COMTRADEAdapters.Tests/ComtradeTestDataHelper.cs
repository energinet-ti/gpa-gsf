//******************************************************************************************************
//  ComtradeTestDataHelper.cs - Gbtc
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

using System;
using System.IO;
using GSF;
using GSF.COMTRADE;

namespace COMTRADEAdapters.Tests
{
    internal static class ComtradeTestDataHelper
    {
        /// <summary>
        /// Default analog value generator: produces unique, predictable values.
        /// Value = (sampleIndex + 1) * 100.0 + channelIndex
        /// </summary>
        internal static readonly Func<int, int, double> DefaultAnalogGenerator =
            (sample, channel) => (sample + 1) * 100.0 + channel;

        /// <summary>
        /// Default digital value generator: alternating true/false per sample.
        /// </summary>
        private static readonly Func<int, int, bool> DefaultDigitalGenerator =
            (sample, channel) => sample % 2 == 0;

        /// <summary>
        /// Creates a COMTRADE .cfg/.dat file pair using GSF.COMTRADE.Writer.
        /// </summary>
        /// <param name="directory">Directory to write files into.</param>
        /// <param name="baseName">Base file name (without extension).</param>
        /// <param name="analogChannelCount">Number of analog channels.</param>
        /// <param name="digitalChannelCount">Number of digital channels.</param>
        /// <param name="sampleCount">Number of samples to write.</param>
        /// <param name="fileType">COMTRADE data format (ASCII, Binary, Binary32, Float32).</param>
        /// <param name="samplingRate">Sampling rate in Hz.</param>
        /// <param name="nominalFrequency">Nominal system frequency in Hz.</param>
        /// <param name="analogValueGenerator">Custom analog value generator (sampleIndex, channelIndex) => value.</param>
        /// <param name="digitalValueGenerator">Custom digital value generator (sampleIndex, channelIndex) => value.</param>
        /// <returns>Full path to the .cfg file.</returns>
        public static string CreateComtradeFiles(
            string directory,
            string baseName,
            int analogChannelCount,
            int digitalChannelCount,
            int sampleCount,
            FileType fileType = FileType.Float32,
            double samplingRate = 30.0,
            double nominalFrequency = 60.0,
            Func<int, int, double> analogValueGenerator = null,
            Func<int, int, bool> digitalValueGenerator = null)
        {
            analogValueGenerator = analogValueGenerator ?? DefaultAnalogGenerator;
            digitalValueGenerator = digitalValueGenerator ?? DefaultDigitalGenerator;

            // Build channel metadata
            ChannelMetadata[] metadata = new ChannelMetadata[analogChannelCount + digitalChannelCount];

            for (int i = 0; i < analogChannelCount; i++)
            {
                metadata[i] = new ChannelMetadata
                {
                    Name = $"Analog_{i}",
                    IsDigital = false,
                    Units = "V",
                    CircuitComponent = null,
                    SignalType = GSF.Units.EE.SignalType.ALOG
                };
            }

            for (int i = 0; i < digitalChannelCount; i++)
            {
                metadata[analogChannelCount + i] = new ChannelMetadata
                {
                    Name = $"Digital_{i}",
                    IsDigital = true,
                    Units = null,
                    CircuitComponent = null,
                    SignalType = GSF.Units.EE.SignalType.DIGI
                };
            }

            Ticks startTime = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).Ticks;

            Schema schema = Writer.CreateSchema(
                metadata,
                "TestStation",
                "TestDevice",
                startTime,
                sampleCount,
                fileType == FileType.Binary32 || fileType == FileType.Float32 ? 2013 : 1999,
                fileType,
                1000.0,
                samplingRate,
                nominalFrequency,
                false); // includeFracSecDefinition: false

            string cfgPath = Path.Combine(directory, baseName + ".cfg");
            string datPath = Path.Combine(directory, baseName + ".dat");

            // Write .cfg
            File.WriteAllText(cfgPath, schema.FileImage);

            // Write .dat samples
            // The Writer's separate analog/digital overloads expect digitalValues sized to the
            // metadata entry count (not schema.TotalDigitalChannels), as the Writer internally
            // expands each bool to a 16-bit digital word.
            long ticksPerSample = samplingRate > 0 ? (long)(Ticks.PerSecond / samplingRate) : 0;

            if (fileType == FileType.Ascii)
            {
                using (StreamWriter writer = new StreamWriter(datPath))
                {
                    for (int s = 0; s < sampleCount; s++)
                    {
                        double[] analogValues = new double[analogChannelCount];
                        bool[] digitalValues = new bool[digitalChannelCount];

                        for (int c = 0; c < analogChannelCount; c++)
                            analogValues[c] = analogValueGenerator(s, c);

                        for (int c = 0; c < digitalChannelCount; c++)
                            digitalValues[c] = digitalValueGenerator(s, c);

                        Ticks timestamp = startTime + s * ticksPerSample;

                        Writer.WriteNextRecordAscii(writer, schema, timestamp, analogValues, digitalValues, (uint)(s + 1));
                    }
                }
            }
            else
            {
                using (FileStream stream = new FileStream(datPath, FileMode.Create, FileAccess.Write))
                {
                    for (int s = 0; s < sampleCount; s++)
                    {
                        double[] analogValues = new double[analogChannelCount];
                        bool[] digitalValues = new bool[digitalChannelCount];

                        for (int c = 0; c < analogChannelCount; c++)
                            analogValues[c] = analogValueGenerator(s, c);

                        for (int c = 0; c < digitalChannelCount; c++)
                            digitalValues[c] = digitalValueGenerator(s, c);

                        Ticks timestamp = startTime + s * ticksPerSample;

                        if (fileType == FileType.Binary)
                            Writer.WriteNextRecordBinary(stream, schema, timestamp, analogValues, digitalValues, (uint)(s + 1));
                        else if (fileType == FileType.Binary32)
                            Writer.WriteNextRecordBinary32(stream, schema, timestamp, analogValues, digitalValues, (uint)(s + 1));
                        else // Float32
                            Writer.WriteNextRecordFloat32(stream, schema, timestamp, analogValues, digitalValues, (uint)(s + 1));
                    }
                }
            }

            return cfgPath;
        }

        /// <summary>
        /// Creates COMTRADE files from raw string content for hand-crafted edge cases.
        /// </summary>
        public static string CreateRawComtradeFiles(
            string directory,
            string baseName,
            string cfgContent,
            string datContent)
        {
            string cfgPath = Path.Combine(directory, baseName + ".cfg");
            string datPath = Path.Combine(directory, baseName + ".dat");

            File.WriteAllText(cfgPath, cfgContent);
            File.WriteAllText(datPath, datContent);

            return cfgPath;
        }

        /// <summary>
        /// Creates a temporary directory for test files.
        /// </summary>
        public static string CreateTempTestDirectory(string testName)
        {
            string path = Path.Combine(Path.GetTempPath(), "COMTRADETests", testName + "_" + Guid.NewGuid().ToString("N").Substring(0, 8));
            Directory.CreateDirectory(path);
            return path;
        }

        /// <summary>
        /// Cleans up a temporary test directory.
        /// </summary>
        public static void CleanupTempDirectory(string path)
        {
            if (!string.IsNullOrEmpty(path) && Directory.Exists(path))
            {
                try
                {
                    Directory.Delete(path, true);
                }
                catch
                {
                    // Best-effort cleanup
                }
            }
        }
    }
}
