using Parquet;
using Parquet.Data;
using Parquet.Rows;
using Parquet.Schema;
using System.Diagnostics;

namespace OldVersion
{
    internal class Program
    {
        static StreamWriter csv;

        static int file = 0;
        static void Main(string[] args)
        {
            MainAsync(args);
            Task.Delay(1000).Wait();
            Console.ReadKey();
        }

        static async void MainAsync(string[] args)
        {
            // Warmup
            var fs = File.OpenWrite(DateTime.Now.ToString("T") + ".results.csv");
            csv = new StreamWriter(fs);
            for (int i = 0; i < 10; i++)
            {
                await RunBenchmark<int>(1_000, 1_000, new DataField[10], false, (i, j) => i * j);
            }
            csv.Dispose();
            fs.Dispose();
            fs = File.OpenWrite(DateTime.Now.ToString("T") + ".results.csv");
            csv = new StreamWriter(fs);

            // Benchmarks
            await RunBenchmark<int>(100, 1_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 1_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(10_000, 1_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 100, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 1_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 10_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 1_000, new DataField[1], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 1_000, new DataField[10], false, (i, j) => i * j);
            await RunBenchmark<int>(1_000, 1_000, new DataField[100], false, (i, j) => i * j);
            await RunBenchmark<string>(1_000, 1_000, new DataField[10], false, (i, j) => (i * j).ToString());

            csv.Dispose();
            fs.Dispose();
            await Console.Out.WriteLineAsync("Done");
        }

        private static async Task RunBenchmark<T>(int rowGroups, int rowsPerGroup, DataField[] fields, bool useTable, Func<int, int, T> createFunc)
        {
            Console.WriteLine("Generating Data");
            for (int i = 0; i < fields.Length; i++)
            {
                fields[i] = new DataField($"Column{i}", typeof(T));
            }
            ParquetSchema schema = new ParquetSchema(fields);
            Table table = CreateTable(rowsPerGroup, fields, createFunc);
            DataColumn[] columns = CreateColumns(rowsPerGroup, fields, createFunc);

            Console.WriteLine("Starting writing");
            Stopwatch sw = Stopwatch.StartNew();

            string path = DateTime.Now.ToString("T") + "." + file++ + ".parquet";
            var stream = File.Open(path, FileMode.CreateNew);
            var writer = await ParquetWriter.CreateAsync(schema, stream);
            for (int i = 0; i < rowGroups; i++)
            {
                if (useTable)
                {
                    await writer.WriteAsync(table);
                }
                else
                {
                    var rowGroupWriter = writer.CreateRowGroup();

                    for (int j = 0; j < columns.Length; j++)
                    {
                        await rowGroupWriter.WriteColumnAsync(columns[j]);
                    }
                    rowGroupWriter.Dispose();
                }
            }
            writer.Dispose();
            stream.Flush();
            stream.Dispose();

            sw.Stop();
            var fi = new FileInfo(path);
            Console.WriteLine($"{rowGroups} rowgroups of {rowsPerGroup} rows");
            Console.WriteLine($"{sw.Elapsed.ToStringFormatted()}");
            csv.WriteLine($"{rowGroups}\t{rowsPerGroup}\t{fields.Length}\t{fi.Length}\t{typeof(T).Name}\t{sw.Elapsed.ToStringFormatted()}\t{sw.Elapsed.TotalNanoseconds}");
        }

        private static DataColumn[] CreateColumns<T>(int rowsPerGroup, DataField[] fields, Func<int, int, T> createFunc)
        {
            DataColumn[] columns = new DataColumn[fields.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                Array data = Array.CreateInstance(typeof(T), rowsPerGroup);
                for (int j = 0; j < rowsPerGroup; j++)
                {
                    data.SetValue(createFunc(i + 1, j + 1), j);
                }
                columns[i] = new DataColumn(fields[i], data);
            }

            return columns;
        }

        private static Table CreateTable<T>(int rowsPerGroup, DataField[] fields, Func<int, int, T> createFunc)
        {
            Table table = new Table(fields);
            for (int i = 0; i < rowsPerGroup; i++)
            {
                object[] data = new object[fields.Length];
                for (int j = 0; j < fields.Length; j++)
                {
                    data.SetValue(createFunc(i + 1, j + 1), j);
                }
                table.Add(data);
            }
            return table;
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
            return $"{timeSpan.TotalSeconds:0}.{timeSpan.Milliseconds:000}s";
        }

        return timeSpan.ToString("g");
    }
}