using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

// General Information about an assembly is controlled through the following 
// set of attributes. Change these attribute values to modify the information
// associated with an assembly.
[assembly: AssemblyTitle("Event Hub Output Adapter for GSF")]
[assembly: AssemblyDescription("Device-framed Azure Event Hub output adapter for the Grid Solutions Framework")]
[assembly: AssemblyCompany("Grid Protection Alliance")]
[assembly: AssemblyProduct("Grid Solutions Framework")]
[assembly: AssemblyCopyright("Copyright ©  2026")]
[assembly: AssemblyDefaultAlias("EventHubOutputAdapter")]
[assembly: AssemblyTrademark("")]
[assembly: AssemblyCulture("")]

// Assembly manifest attributes.
#if DEBUG
[assembly: AssemblyConfiguration("Debug Build")]
#else
[assembly: AssemblyConfiguration("Release Build")]
#endif

// Setting ComVisible to false makes the types in this assembly not visible 
// to COM components.  If you need to access a type in this assembly from 
// COM, set the ComVisible attribute to true on that type.
[assembly: ComVisible(false)]

// The following GUID is for the ID of the typelib if this project is exposed to COM
[assembly: Guid("687EBA12-1237-43D0-9C8B-E0516A049E1B")]

// Version information for an assembly consists of the following four values:
//
//      Major Version
//      Minor Version 
//      Build Number
//      Revision
//
// You can specify all the values or you can default the Build and Revision Numbers 
// by using the '*' as shown below:
// [assembly: AssemblyVersion("2.4.321.0")]
[assembly: AssemblyVersion("2.4.321.0")]
[assembly: AssemblyFileVersion("2.4.321.0")]
[assembly: AssemblyInformationalVersion("2.4.321-beta")]

[assembly: InternalsVisibleTo("EventHubOutputAdapter.Tests")]
