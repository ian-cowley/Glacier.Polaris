using System;
using System.IO;

namespace Glacier.Polaris.IO
{
    internal static class IpcWriter
    {
        public static void Write(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            df.ToArrowIpc(fs);
        }

        public static void WriteStream(DataFrame df, string filePath)
        {
            using var fs = new FileStream(filePath, FileMode.Create, FileAccess.Write);
            df.ToArrowIpc(fs);
        }
    }
}
