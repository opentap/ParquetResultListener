using Parquet;
using Parquet.Data;
using System.Diagnostics;

namespace OldVersion
{
    internal class Program
    {
        static void Main(string[] args)
        {
            int rowGroups = 10_000;
            int rowsPerGroup = 1_000;
            DataField[] fields = new DataField[3];
            for (int i = 0; i < fields.Length; i++)
            {
                fields[i] = new DataField($"Column{i}", typeof(int));
            }
            DataColumn[] columns = new DataColumn[fields.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                Array data = Array.CreateInstance(typeof(int), rowsPerGroup);
                for (int j = 0; j < rowsPerGroup; j++)
                {
                    data.SetValue(i * j, j);
                }
                columns[i] = new DataColumn(fields[i], data);
            }

            Stopwatch sw = Stopwatch.StartNew();

            Schema schema = new Schema(fields);
            var stream = File.Open(DateTime.Now.ToString("T") + ".parquet", FileMode.CreateNew);
            var writer = new ParquetWriter(schema, stream);
            for (int i = 0; i < rowGroups; i++)
            {
                var rowGroupWriter = writer.CreateRowGroup();

                for (int j = 0;j < columns.Length; j++)
                {
                    rowGroupWriter.WriteColumn(columns[j]);
                }
            }
            writer.Dispose();
            stream.Flush();
            stream.Dispose();

            sw.Stop();
            Console.WriteLine($"{rowGroups} rowgroups of {rowsPerGroup} rows");
            Console.WriteLine($"{sw.Elapsed.ToStringFormatted()}");
        }
    }
}
internal static class TimespanExtensions
{
    internal static string ToStringFormatted(this TimeSpan timeSpan)
    {
        if (timeSpan.TotalNanoseconds < 1000)
        {
            return $"{timeSpan.Nanoseconds:0}ns";
        }
        if (timeSpan.TotalMicroseconds < 1000)
        {
            return $"{timeSpan.Microseconds:0}.{timeSpan.Nanoseconds:000}μs";
        }
        if (timeSpan.TotalMilliseconds < 1000)
        {
            return $"{timeSpan.Milliseconds:0}.{timeSpan.Microseconds:000}ms";
        }
        if (timeSpan.TotalSeconds < 1000)
        {
            return $"{timeSpan.Seconds:0}.{timeSpan.Milliseconds:000}s";
        }

        return timeSpan.ToString("g");
    }
}