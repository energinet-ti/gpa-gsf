//******************************************************************************************************
//  BadValueFilteringTests.cs - Gbtc
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
using GSF.COMTRADE;
using GSF.TimeSeries;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class BadValueFilteringTests
    {
        private FileImporter m_importer;
        private string m_directory;
        private readonly List<IMeasurement> m_published = new List<IMeasurement>();

        [TestInitialize]
        public void Setup()
        {
            m_importer = new FileImporter { Name = "Filtering_" + Guid.NewGuid().ToString("N") };
            m_directory = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(BadValueFilteringTests));
            m_published.Clear();
            m_importer.NewMeasurements += (sender, args) => m_published.AddRange(args.Argument);
        }

        [TestCleanup]
        public void Cleanup()
        {
            m_importer.Dispose();
            ComtradeTestDataHelper.CleanupTempDirectory(m_directory);
        }

        [TestMethod]
        public void Binary_SuppressesScaledInt16SentinelAndPreservesValidSamples()
        {
            Replay(CreateIntegerRecording(FileType.Binary, short.MinValue, -100, 42), 3);
            CollectionAssert.AreEqual(new[] { -190.0D, 94.0D }, Values());
        }

        [TestMethod]
        public void Binary32_Int16OptionSuppressesInt16Sentinel()
        {
            m_importer.Binary32UsesInt16Sentinel = true;
            Replay(CreateIntegerRecording(FileType.Binary32, short.MinValue, -100, 42), 3);
            CollectionAssert.AreEqual(new[] { -190.0D, 94.0D }, Values());
        }

        [TestMethod]
        public void Binary32_DefaultPreservesValidMinus32768AndSuppressesInt32Sentinel()
        {
            Assert.IsFalse(m_importer.Binary32UsesInt16Sentinel);
            Replay(CreateIntegerRecording(FileType.Binary32, short.MinValue, int.MinValue, -100), 3);
            CollectionAssert.AreEqual(new[] { -65526.0D, -190.0D }, Values());
        }

        [TestMethod]
        public void Binary_Int16SentinelStillAppliesWhenBinary32OptionIsDisabled()
        {
            m_importer.Binary32UsesInt16Sentinel = false;
            Replay(CreateIntegerRecording(FileType.Binary, short.MinValue, 42), 2);
            CollectionAssert.AreEqual(new[] { 94.0D }, Values());
        }

        [DataTestMethod]
        [DataRow(FileType.Binary)]
        [DataRow(FileType.Binary32)]
        public void SkipBadValuesFalse_PublishesSentinelAsReturnedByParser(FileType fileType)
        {
            m_importer.SkipBadValues = false;
            Replay(CreateIntegerRecording(fileType, short.MinValue, 42), 2);
            CollectionAssert.AreEqual(new[] { -65526.0D, 94.0D }, Values());
        }

        [TestMethod]
        public void Float32_DefaultSuppressesNaNAndInfinityWithoutApplyingIntegerSentinels()
        {
            Replay(CreateFloatRecording(float.NaN, float.PositiveInfinity, float.NegativeInfinity, -32768.0F, 42.0F), 5);
            CollectionAssert.AreEqual(new[] { -32768.0D, 42.0D }, Values());
        }

        [TestMethod]
        public void SkipBadValuesFalse_PreservesNaNAndInfinity()
        {
            m_importer.SkipBadValues = false;
            Replay(CreateFloatRecording(float.NaN, float.PositiveInfinity, float.NegativeInfinity, 42.0F), 4);
            Assert.AreEqual(4, m_published.Count);
            Assert.IsTrue(double.IsNaN(m_published[0].Value));
            Assert.IsTrue(double.IsPositiveInfinity(m_published[1].Value));
            Assert.IsTrue(double.IsNegativeInfinity(m_published[2].Value));
            Assert.AreEqual(42.0D, m_published[3].Value);
        }

        [TestMethod]
        public void Binary32Policy_ParsesExplicitConnectionStringAndRetainsDefault()
        {
            m_importer.ConnectionString = "Binary32UsesInt16Sentinel=True";
            m_importer.Initialize();
            Assert.IsTrue(m_importer.Binary32UsesInt16Sentinel);
            m_importer.ConnectionString = "";
            m_importer.Initialize();
            Assert.IsFalse(m_importer.Binary32UsesInt16Sentinel);
        }

        private string CreateIntegerRecording(FileType fileType, params int[] values)
        {
            string path = ComtradeTestDataHelper.CreateComtradeFiles(m_directory, "integer", 1, 0, values.Length, fileType);
            Schema schema = new Schema(path);
            schema.AnalogChannels[0].Multiplier = 2.0D;
            schema.AnalogChannels[0].Adder = 10.0D;
            File.WriteAllText(path, schema.FileImage);
            using (BinaryWriter writer = new BinaryWriter(File.Create(Path.ChangeExtension(path, ".dat"))))
            {
                for (int i = 0; i < values.Length; i++)
                {
                    writer.Write((uint)(i + 1));
                    writer.Write((uint)(i * 33333));
                    if (fileType == FileType.Binary)
                        writer.Write((short)values[i]);
                    else
                        writer.Write(values[i]);
                }
            }
            return path;
        }

        private string CreateFloatRecording(params float[] values)
        {
            return ComtradeTestDataHelper.CreateComtradeFiles(m_directory, "floating", 1, 0, values.Length,
                FileType.Float32, analogValueGenerator: (sample, channel) => values[sample]);
        }

        private void Replay(string path, int count)
        {
            typeof(FileImporter).GetMethod("LoadFile", BindingFlags.Instance | BindingFlags.NonPublic).Invoke(m_importer, new object[] { path });
            for (int i = 0; i <= count; i++)
                typeof(FileImporter).GetMethod("PublishNextFrame", BindingFlags.Instance | BindingFlags.NonPublic).Invoke(m_importer, null);
            StringAssert.Contains(m_importer.GetShortStatus(256), "Completed");
        }

        private double[] Values()
        {
            return m_published.Select(measurement => measurement.Value).ToArray();
        }
    }
}
