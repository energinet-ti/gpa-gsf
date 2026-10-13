//******************************************************************************************************
//  MeasurementMappingTests.cs - Gbtc
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
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using GSF.COMTRADE;
using GSF.TimeSeries;
using GSF.TimeSeries.Adapters;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class MeasurementMappingTests
    {
        private FileImporter m_importer;
        private string m_directory;
        private readonly List<IMeasurement> m_published = new List<IMeasurement>();

        [TestInitialize]
        public void Setup()
        {
            m_importer = new FileImporter { Name = "Mapping_" + Guid.NewGuid().ToString("N") };
            m_directory = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(MeasurementMappingTests));
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
        public void SuccessiveRecordings_ReuseChannelKeysAndPreserveValidTags()
        {
            string first = CreateRecording("first", 2, schema =>
            {
                schema.AnalogChannels[0].Name = "Plant:Voltage";
                schema.AnalogChannels[1].Name = "Plant:Current";
            });
            string second = CreateRecording("second", 2, schema =>
            {
                schema.AnalogChannels[0].Name = "Plant:Voltage";
                schema.AnalogChannels[1].Name = "Plant:Current";
            });

            LoadAndPublish(first);
            IMeasurement[] initial = m_published.ToArray();
            LoadAndPublish(second);

            Assert.AreEqual(4, m_published.Count);
            Assert.AreEqual("TestDevice_Voltage", initial[0].TagName);
            Assert.AreEqual("TestDevice_Current", initial[1].TagName);
            for (int i = 0; i < 2; i++)
            {
                Assert.AreEqual(m_importer.Name, initial[i].Key.Source);
                Assert.AreEqual((ulong)(i + 1), initial[i].Key.ID);
                Assert.AreEqual(initial[i].Key, m_published[i + 2].Key);
                Assert.AreEqual(initial[i].ID, m_published[i + 2].ID);
                Assert.AreEqual(initial[i].Value, m_published[i + 2].Value);
                Assert.AreEqual(initial[i].Timestamp, m_published[i + 2].Timestamp);
            }
        }

        [TestMethod]
        public void ReorderedChannels_KeepTheirOriginalSignalAssociations()
        {
            string first = CreateRecording("ordered", 2, schema =>
            {
                schema.AnalogChannels[0].Name = "Plant:Voltage";
                schema.AnalogChannels[1].Name = "Plant:Current";
            });
            string second = CreateRecording("reordered", 2, schema =>
            {
                schema.AnalogChannels[0].Name = "Plant:Current";
                schema.AnalogChannels[1].Name = "Plant:Voltage";
            });

            LoadAndPublish(first);
            LoadAndPublish(second);

            Assert.AreEqual(m_published[0].ID, m_published[3].ID);
            Assert.AreEqual(m_published[1].ID, m_published[2].ID);
            Assert.AreEqual(m_published[0].TagName, m_published[3].TagName);
            Assert.AreEqual(m_published[1].TagName, m_published[2].TagName);
        }

        [TestMethod]
        public void ReconstructedAdapter_ReusesRuntimeIdentityWithoutGrowingKeys()
        {
            string path = CreateRecording("reconstructed", 1);
            LoadAndPublish(path);
            MeasurementKey original = m_published.Single().Key;
            string adapterName = m_importer.Name;
            m_importer.Dispose();
            m_importer = new FileImporter { Name = adapterName };
            m_importer.NewMeasurements += (sender, args) => m_published.AddRange(args.Argument);

            LoadAndPublish(path);

            Assert.AreEqual(2, m_published.Count);
            Assert.AreEqual(original, m_published[1].Key);
            Assert.AreEqual(original.SignalID, m_published[1].ID);
        }

        [TestMethod]
        public void UnchangedMapping_NotifiesHostRoutingOnce()
        {
            int updates = 0;
            string path = CreateRecording("routing", 1);
            m_importer.OutputMeasurementsUpdated += (sender, args) => updates++;

            LoadAndPublish(path);
            LoadAndPublish(path);

            Assert.AreEqual(1, updates);
            Assert.AreEqual(2, m_published.Count);
        }

        [DataTestMethod]
        [DataRow(false, false)]
        [DataRow(false, true)]
        [DataRow(true, false)]
        [DataRow(true, true)]
        public void ReentrantReplacement_WaitsForPendingHostRoutingBeforeStartingTimer(bool replaceFromRouting, bool differentMapping)
        {
            string first = CreateRecording("routing_first", 1);
            string second = CreateRecording("routing_second", 1, schema =>
            {
                if (differentMapping)
                    schema.AnalogChannels[0].Name = "Plant:OtherVoltage";
            });
            m_importer.ProcessingInterval = 60000;
            bool replaced = false;
            bool startedBeforeRoutingCompleted = false;
            int routingNotifications = 0;
            Action replace = () =>
            {
                if (replaced)
                    return;

                replaced = true;
                m_importer.ReplayFile(second);
                System.Timers.Timer timer = (System.Timers.Timer)typeof(FileImporter)
                    .GetField("m_publishTimer", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(m_importer);
                startedBeforeRoutingCompleted = timer.Enabled;
            };
            m_importer.StatusMessage += (sender, args) =>
            {
                if (!replaceFromRouting)
                    replace();
            };
            m_importer.OutputMeasurementsUpdated += (sender, args) =>
            {
                routingNotifications++;
                if (replaceFromRouting)
                    replace();
            };

            m_importer.ReplayFile(first);

            Assert.IsTrue(replaced);
            Assert.IsFalse(startedBeforeRoutingCompleted, "Replacement publication must wait for every pending routing callback.");
            Assert.AreEqual(differentMapping ? 2 : 1, routingNotifications);
            Assert.AreEqual(second, m_importer.FileName);
            System.Timers.Timer activeTimer = (System.Timers.Timer)typeof(FileImporter)
                .GetField("m_publishTimer", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(m_importer);
            Assert.IsTrue(activeTimer.Enabled, "Draining the older routing callback must start the latest replay.");
            Invoke("PublishNextFrame");
            Assert.AreEqual(1, m_published.Count);
            Assert.AreEqual(differentMapping ? "TestDevice_OtherVoltage" : "TestDevice_Analog_0", m_published[0].TagName);
        }

        [TestMethod]
        public void AutoRepeat_RetainsSignalIdentity()
        {
            string path = CreateRecording("repeat", 1);
            m_importer.AutoRepeat = true;
            LoadAndPublish(path);
            MeasurementKey original = m_published[0].Key;
            Invoke("PublishNextFrame"); // EOF reloads the one-frame recording.
            Invoke("PublishNextFrame");

            Assert.AreEqual(2, m_published.Count);
            Assert.AreEqual(original, m_published[1].Key);
        }

        [TestMethod]
        public void DifferentDevice_DoesNotOverwritePreviousChannelIdentity()
        {
            string first = CreateRecording("device_one", 1);
            string second = CreateRecording("device_two", 1, schema => schema.DeviceID = "OtherDevice");
            LoadAndPublish(first);
            LoadAndPublish(second);
            LoadAndPublish(first);

            Assert.AreNotEqual(m_published[0].ID, m_published[1].ID);
            Assert.AreNotEqual(m_published[0].Key, m_published[1].Key);
            Assert.AreEqual(m_published[0].Key, m_published[2].Key);
            Assert.AreEqual(m_published[0].Key, MeasurementKey.LookUpBySignalID(m_published[0].ID));
        }

        [TestMethod]
        public void PlainChannelIdentifier_IsUsedWhenParsedChannelNameIsEmpty()
        {
            string path = CreateRecording("plain", 2);
            LoadAndPublish(path);

            CollectionAssert.AreEqual(new[] { "TestDevice_Analog_0", "TestDevice_Analog_1" },
                m_published.Select(measurement => measurement.TagName).ToArray());
        }

        [TestMethod]
        public void CollidingAnalogAndDigitalTags_AreUniqueWithoutRenamingValidTags()
        {
            string path = ComtradeTestDataHelper.CreateComtradeFiles(m_directory, "collisions", 3, 1, 1);
            Schema schema = new Schema(path);
            schema.AnalogChannels[0].Name = "Plant:Shared";
            schema.AnalogChannels[1].Name = "Plant:Shared";
            schema.AnalogChannels[2].Name = "Plant:Shared_A2";
            foreach (DigitalChannel channel in schema.DigitalChannels)
                channel.Name = "Plant:Shared";
            File.WriteAllText(path, schema.FileImage);

            LoadAndPublish(path);
            string[] tags = m_published.Select(measurement => measurement.TagName).ToArray();
            Assert.AreEqual(tags.Length, tags.Distinct(StringComparer.OrdinalIgnoreCase).Count());
            Assert.AreEqual("TestDevice_Shared", tags[0]);
            Assert.AreEqual("TestDevice_Shared_A2", tags[2], "A collision suffix must not steal a valid original tag.");
            Assert.AreEqual(m_published.Count, m_published.Select(measurement => measurement.ID).Distinct().Count());

            Guid[] signalIDs = m_published.Select(measurement => measurement.ID).ToArray();
            m_published.Clear();
            LoadAndPublish(path);
            CollectionAssert.AreEqual(tags, m_published.Select(measurement => measurement.TagName).ToArray());
            CollectionAssert.AreEqual(signalIDs, m_published.Select(measurement => measurement.ID).ToArray());
        }

        [TestMethod]
        public void ExistingHostMetadata_RoutesByHostDeviceAndKeepsRegisteredIdentity()
        {
            string path = CreateRecording("registered", 1, schema =>
            {
                schema.DeviceID = "1001";
                schema.AnalogChannels[0].Name = "Plant:Voltage";
            });
            Guid signalID = Guid.NewGuid();
            string keyText = "HostArchive_" + Guid.NewGuid().ToString("N") + ":72";
            DataSet source = CreateHostMetadata(signalID, keyText, "1001_Voltage", "HOST_DEVICE");
            m_importer.DataSource = source;
            MeasurementKey registered = MeasurementKey.LookUpOrCreate(signalID, keyText);
            registered.SetMeasurementMetadata("1001_Voltage", 7.0D, 2.0D, Measurement.AverageValueFilter);
            MeasurementKey[] downstreamKeys = AdapterBase.ParseInputMeasurementKeys(source, false,
                "FILTER ActiveMeasurements WHERE Device = 'HOST_DEVICE'");
            bool metadataReadyBeforeData = false;
            m_importer.OutputMeasurementsUpdated += (sender, args) =>
                metadataReadyBeforeData = m_importer.OutputMeasurements.Single().ID == signalID;
            bool dataBeforeMetadata = false;
            m_importer.NewMeasurements += (sender, args) => dataBeforeMetadata |= !metadataReadyBeforeData;

            LoadAndPublish(path);
            LoadAndPublish(path);

            Assert.AreEqual(2, m_published.Count);
            Assert.IsFalse(dataBeforeMetadata, "The host must learn the output mapping before receiving values.");
            Assert.IsTrue(m_published.All(measurement => measurement.ID == signalID));
            Assert.IsTrue(m_published.All(measurement => downstreamKeys.Contains(measurement.Key)));
            Assert.AreEqual(keyText, m_published[0].Key.ToString());
            Assert.AreEqual(207.0D, m_published[0].AdjustedValue);
            Assert.AreEqual(registered.Metadata.MeasurementValueFilter, m_published[0].Metadata.MeasurementValueFilter);
            Assert.AreEqual("HOST_DEVICE", source.Tables["ActiveMeasurements"].Rows[0]["Device"]);
            Assert.AreEqual(1, source.Tables["ActiveMeasurements"].Rows.Count, "Replay must not insert guessed device or measurement records.");
        }

        [TestMethod]
        public void AmbiguousHostTags_FailWithoutPublishingToAnArbitrarySignal()
        {
            string path = CreateRecording("ambiguous", 1);
            DataSet source = CreateHostMetadata(Guid.NewGuid(), "Host:401", "TestDevice_Analog_0", "FIRST");
            source.Tables["ActiveMeasurements"].Rows.Add(Guid.NewGuid(), "Host:402", "TestDevice_Analog_0", "SECOND");
            m_importer.DataSource = source;

            Invoke("LoadFile", path);
            Invoke("PublishNextFrame");

            Assert.AreEqual(0, m_published.Count);
            StringAssert.Contains(m_importer.GetShortStatus(256), "Error");
            StringAssert.Contains(m_importer.Status, "multiple measurements");
        }

        [TestMethod]
        public void RegisteredRuntimeKey_IsNotReplacedByANewSignalID()
        {
            string path = CreateRecording("key_exists", 1);
            Guid signalID = Guid.NewGuid();
            MeasurementKey registered = MeasurementKey.CreateOrUpdate(signalID, m_importer.Name, 1);
            registered.SetMeasurementMetadata("TestDevice_Analog_0", 0.0D, 1.0D, null);

            LoadAndPublish(path);

            Assert.AreEqual(registered, m_published.Single().Key);
            Assert.AreEqual(signalID, m_published.Single().ID);
        }

        [TestMethod]
        public void ConfiguredMapping_PreservesSuppliedMetadataAndPositionalMismatchBehavior()
        {
            string path = CreateRecording("configured", 2);
            MeasurementKey key = MeasurementKey.CreateOrUpdate(Guid.NewGuid(), "Configured_" + Guid.NewGuid().ToString("N"), 52);
            MeasurementMetadata metadata = key.Metadata.ChangeTagName("ConfiguredTag").ChangeAdderMultiplier(7.0D, 2.0D);
            IMeasurement[] configured = { new Measurement { Metadata = metadata } };
            m_importer.AutoCreateMeasurements = false;
            m_importer.OutputMeasurements = configured;

            LoadAndPublish(path);

            Assert.AreEqual(1, m_published.Count);
            Assert.AreSame(configured, m_importer.OutputMeasurements);
            Assert.AreSame(metadata, m_published[0].Metadata);
            Assert.AreEqual(100.0D, m_published[0].Value);
            Assert.AreEqual(207.0D, m_published[0].AdjustedValue);
        }

        private string CreateRecording(string name, int count, Action<Schema> customize = null)
        {
            string path = ComtradeTestDataHelper.CreateComtradeFiles(m_directory, name, count, 0, 1);
            if (customize != null)
            {
                Schema schema = new Schema(path);
                customize(schema);
                File.WriteAllText(path, schema.FileImage);
            }
            return path;
        }

        private static DataSet CreateHostMetadata(Guid signalID, string key, string tag, string device)
        {
            DataTable measurements = new DataTable("ActiveMeasurements");
            measurements.Columns.Add("SignalID", typeof(Guid));
            measurements.Columns.Add("ID", typeof(string));
            measurements.Columns.Add("PointTag", typeof(string));
            measurements.Columns.Add("Device", typeof(string));
            measurements.Rows.Add(signalID, key, tag, device);
            DataSet source = new DataSet();
            source.Tables.Add(measurements);
            return source;
        }

        private void LoadAndPublish(string path)
        {
            Invoke("LoadFile", path);
            Invoke("PublishNextFrame");
        }

        private void Invoke(string method, params object[] args)
        {
            typeof(FileImporter).GetMethod(method, BindingFlags.Instance | BindingFlags.NonPublic).Invoke(m_importer, args);
        }
    }
}
