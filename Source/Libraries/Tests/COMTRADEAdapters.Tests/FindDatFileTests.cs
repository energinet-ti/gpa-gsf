//******************************************************************************************************
//  FindDatFileTests.cs - Gbtc
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
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace COMTRADEAdapters.Tests
{
    [TestClass]
    public class FindDatFileTests
    {
        private string m_testDir;

        [TestInitialize]
        public void Setup()
        {
            m_testDir = ComtradeTestDataHelper.CreateTempTestDirectory(nameof(FindDatFileTests));
        }

        [TestCleanup]
        public void Cleanup()
        {
            ComtradeTestDataHelper.CleanupTempDirectory(m_testDir);
        }

        [DataTestMethod]
        [DataRow(".dat")]
        [DataRow(".DAT")]
        [DataRow(".Dat")]
        public void ExistingExtension_ReturnsPath(string extension)
        {
            string basePath = Path.Combine(m_testDir, "test");
            File.WriteAllText(basePath + extension, "");

            string result = FileImporter.FindDatFile(basePath);

            Assert.IsNotNull(result);
            Assert.IsTrue(File.Exists(result));
            // Case-insensitive filesystems may return a different casing from the original path.
            Assert.IsTrue(result.EndsWith(".dat", StringComparison.OrdinalIgnoreCase));
        }

        [TestMethod]
        public void NoFile_ReturnsNull()
        {
            // Arrange
            string basePath = Path.Combine(m_testDir, "nonexistent");

            // Act
            string result = FileImporter.FindDatFile(basePath);

            // Assert
            Assert.IsNull(result);
        }
    }
}
