﻿using NUnit.Framework;
using OpenTap;
using OpenTap.Plugins.Parquet;
using Parquet.Extensions;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Parquet.Tests
{
    internal class ParquetFileTests
    {
        // TODO: Consider adding tests for writing results, step parameters and plan parameters. For now these tests should cover the tasks most prone to failing.

        [Test]
        public void CreatingFileTest()
        {
            var builder = new SchemaBuilder();
            var schema = builder.ToSchema();

            using var stream = new MemoryStream();
            var stepGuid = Guid.NewGuid();
            var parentGuid = Guid.NewGuid();
            using (var file = new ParquetFile(schema, stream, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(null, null, null, "ResultName", stepGuid, parentGuid);
            }

            using (var reader = new ParquetReader(stream))
            {
                Assert.That(reader.RowGroupCount, Is.EqualTo(1));
                Assert.That(reader.Schema, Is.EqualTo(schema));
                var columns = reader.ReadEntireRowGroup(0);

                Assert.That(columns.First().Data.GetValue(0), Is.EqualTo("ResultName"));
                Assert.That(columns.Skip(1).First().Data.GetValue(0)?.ToString(), Is.EqualTo(stepGuid.ToString()));
                Assert.That(columns.Skip(2).First().Data.GetValue(0)?.ToString(), Is.EqualTo(parentGuid.ToString()));
            }
        }

        [Test]
        public void MergingFilesSameSizeTest()
        {
            var builder = new SchemaBuilder();
            var schema = builder.ToSchema();

            using var stream1 = new MemoryStream();
            var stepGuid1 = Guid.NewGuid();
            var parentGuid1 = Guid.NewGuid();
            using (var file = new ParquetFile(schema, stream1, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(null, null, null, "ResultName", stepGuid1, parentGuid1);
            }

            using var stream2 = new MemoryStream();
            var stepGuid2 = Guid.NewGuid();
            var parentGuid2 = Guid.NewGuid();
            using (var file = new ParquetFile(schema, stream2, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(stream1);
                file.AddRows(null, null, null, "ResultName2", stepGuid2, parentGuid2);
            }

            using (var reader = new ParquetReader(stream2))
            {
                Assert.That(reader.RowGroupCount, Is.EqualTo(2));
                Assert.That(reader.Schema, Is.EqualTo(schema));
                var columns1 = reader.ReadEntireRowGroup(0);
                var columns2 = reader.ReadEntireRowGroup(1);

                Assert.That(columns1.First().Data.GetValue(0), Is.EqualTo("ResultName"));
                Assert.That(columns2.First().Data.GetValue(0), Is.EqualTo("ResultName2"));
                Assert.That(columns1.Skip(1).First().Data.GetValue(0)?.ToString(), Is.EqualTo(stepGuid1.ToString()));
                Assert.That(columns2.Skip(1).First().Data.GetValue(0)?.ToString(), Is.EqualTo(stepGuid2.ToString()));
                Assert.That(columns1.Skip(2).First().Data.GetValue(0)?.ToString(), Is.EqualTo(parentGuid1.ToString()));
                Assert.That(columns2.Skip(2).First().Data.GetValue(0)?.ToString(), Is.EqualTo(parentGuid2.ToString()));
            }
        }

        [Test]
        public void MergingFilesTest()
        {
            var builder = new SchemaBuilder();

            var schema1 = builder.ToSchema();
            using var stream1 = new MemoryStream();
            var stepGuid1 = Guid.NewGuid();
            var parentGuid1 = Guid.NewGuid();
            using (var file = new ParquetFile(schema1, stream1, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(null, null, null, "ResultName", stepGuid1, parentGuid1);
            }

            var table = new ResultTable(
                "ResultName2",
                new ResultColumn[]
                {
                    new ResultColumn("Hello", new []{"Test"}),
                }
            );
            builder.AddResults(table);
            var schema2 = builder.ToSchema();
            using var stream2 = new MemoryStream();
            var stepGuid2 = Guid.NewGuid();
            var parentGuid2 = Guid.NewGuid();
            using (var file = new ParquetFile(schema2, stream2, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(stream1);
                file.AddRows(null, null, table.GetResults(), "ResultName2", stepGuid2, parentGuid2);
            }

            using (var reader = new ParquetReader(stream2))
            {
                Assert.That(reader.RowGroupCount, Is.EqualTo(2));
                Assert.That(reader.Schema, Is.EqualTo(schema2));
                var columns1 = reader.ReadEntireRowGroup(0);
                var columns2 = reader.ReadEntireRowGroup(1);

                Assert.That(columns1.First().Data.GetValue(0), Is.EqualTo("ResultName"));
                Assert.That(columns2.First().Data.GetValue(0), Is.EqualTo("ResultName2"));
                Assert.That(columns1.Skip(1).First().Data.GetValue(0)?.ToString(), Is.EqualTo(stepGuid1.ToString()));
                Assert.That(columns2.Skip(1).First().Data.GetValue(0)?.ToString(), Is.EqualTo(stepGuid2.ToString()));
                Assert.That(columns1.Skip(2).First().Data.GetValue(0)?.ToString(), Is.EqualTo(parentGuid1.ToString()));
                Assert.That(columns2.Skip(2).First().Data.GetValue(0)?.ToString(), Is.EqualTo(parentGuid2.ToString()));
                Assert.That(columns1.Skip(3).First().Data.GetValue(0)?.ToString(), Is.EqualTo(null));
                Assert.That(columns2.Skip(3).First().Data.GetValue(0)?.ToString(), Is.EqualTo("Test"));
            }
        }

        [Test]
        public void CreateMetadata()
        {
            var builder = new SchemaBuilder();
            var schema = builder.ToSchema();

            using var stream = new MemoryStream();
            var stepGuid = Guid.NewGuid();
            var parentGuid = Guid.NewGuid();
            using (var file = new ParquetFile(schema, stream, new ParquetFileOptions() { CloseStream = false }))
            {
                file.AddRows(null, null, null, "ResultName", stepGuid, parentGuid);
                file.AddMetadata("Custom", "Metadata");
            }

            using (var reader = new ParquetReader(stream))
            {
                Assert.That(reader.CustomMetadata["SchemaVersion"], Is.EqualTo("1.0.0.0"));
                Assert.That(reader.CustomMetadata["Custom"], Is.EqualTo("Metadata"));
                Assert.That(DateTime.TryParse(reader.CustomMetadata["Time"], out _), Is.True);
                Assert.That(reader.CustomMetadata["ToolVersion"], Is.EqualTo("1.0.0.0"));
            }
        }
    }
}
