﻿using Parquet;
using Parquet.Data;
using Parquet.Data.Rows;
using Parquet.Extensions;
using System;
using System.Collections.Generic;
using System.IO;
using System.Security.Cryptography;

namespace OpenTap.Plugins.Parquet
{
    [Display("Parquet", "Save results in a Parquet file", "Database")]
    public sealed class ParquetResultListener : ResultListener
    {
        private readonly Dictionary<string, ParquetFile> _parquetFiles = new Dictionary<string, ParquetFile>();
        private readonly Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new Dictionary<Guid, TestPlanRun>();
        private readonly Dictionary<Guid, TestStepRun> _guidToStepRuns = new Dictionary<Guid, TestStepRun>();
        private readonly HashSet<Guid> _hasWrittenParameters = new HashSet<Guid>();

        [Display("File path1", "The file path of the parquet file(s). Can use <ResultType> to have one file per result type.")]
        [FilePath(FilePathAttribute.BehaviorChoice.Save)]
        public MacroString FilePath { get; set; } = new MacroString() { Text = "Results/<TestPlanName>.<Date>/<ResultType>.parquet" };

        public static CompressionMethod CompressionMethod = CompressionMethod.None;
        [Display(nameof(CompressionMethod))]
        public CompressionMethod compressionMethod { get => CompressionMethod; set => CompressionMethod = value; }
        public static int CompressionLevel = -1;
        [Display(nameof(compressionLevel))]
        public int compressionLevel { get => CompressionLevel; set => CompressionLevel = value; }

        public ParquetResultListener()
        {
            Name = "Parquet";
        }

        public override void Open()
        {
            base.Open();
        }

        public override void Close()
        {
            base.Close();
            foreach (ParquetFile file in _parquetFiles.Values)
            {
                file.Dispose();
            }
            _parquetFiles.Clear();

            _guidToPlanRuns.Clear();
            _guidToStepRuns.Clear();
        }

        public override void OnTestPlanRunStart(TestPlanRun planRun)
        {
            base.OnTestPlanRunStart(planRun);

            _guidToPlanRuns[planRun.Id] = planRun;
        }

        public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
        {
            base.OnTestPlanRunCompleted(planRun, logStream);

            if (!_hasWrittenParameters.Contains(planRun.Id))
            {
                string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
                {
                    { "ResultType", "Plan" }
                });
                SchemaBuilder builder = new SchemaBuilder();
                builder.AddParameters(FieldType.Plan, planRun);
                ParquetFile file = GetOrCreateParquetFile(builder, path);
                file.AddRows(planRun.GetParameters(), null, null, null, planRun.Id, null);
                _hasWrittenParameters.Add(planRun.Id);
            }

            foreach (ParquetFile file in _parquetFiles.Values)
            {
                file.Dispose();
                planRun.PublishArtifact(file.Path);
            }
            _parquetFiles.Clear();
        }

        public override void OnTestStepRunStart(TestStepRun stepRun)
        {
            base.OnTestStepRunStart(stepRun);
            _guidToStepRuns[stepRun.Id] = stepRun;
        }

        public override void OnTestStepRunCompleted(TestStepRun stepRun)
        {
            base.OnTestStepRunCompleted(stepRun);

            if (!_hasWrittenParameters.Contains(stepRun.Id))
            {
                TestPlanRun planRun = GetPlanRun(stepRun);
                string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
                {
                    { "ResultType", "Plan" }
                });
                SchemaBuilder builder = new SchemaBuilder();
                builder.AddParameters(FieldType.Step, stepRun);
                ParquetFile file = GetOrCreateParquetFile(builder, path);
                file.AddRows(null, stepRun.GetParameters(), null, null, stepRun.Id, stepRun.Parent);
                _hasWrittenParameters.Add(stepRun.Id);
            }
        }

        public override void OnResultPublished(Guid stepRunId, ResultTable result)
        {
            base.OnResultPublished(stepRunId, result);
            TestStepRun stepRun = _guidToStepRuns[stepRunId];
            TestPlanRun planRun = GetPlanRun(stepRun);

            string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
            {
                { "ResultType", result.Name }
            });
            SchemaBuilder builder = new SchemaBuilder();
            builder.AddParameters(FieldType.Step, stepRun);
            builder.AddResults(result);
            ParquetFile file = GetOrCreateParquetFile(builder, path);
            file.AddRows(null, stepRun.GetParameters(), result.GetResults(), result.Name, stepRun.Id, stepRun.Parent);

            _hasWrittenParameters.Add(stepRunId);
        }

        private ParquetFile GetOrCreateParquetFile(SchemaBuilder builder, string path)
        {
            if (!_parquetFiles.TryGetValue(path, out ParquetFile? file))
            {
                string dirPath = Path.GetDirectoryName(path);
                if (!Directory.Exists(dirPath))
                {
                    Directory.CreateDirectory(dirPath);
                }

                file = new ParquetFile(builder.ToSchema(), path);
                _parquetFiles[path] = file;
            }
            
            if (!file.CanContain(builder.ToSchema()))
            {
                builder.Union(file.Schema);
                file.Dispose();
                string tmpPath = path + ".tmp";
                File.Move(path, tmpPath);

                file = new ParquetFile(builder.ToSchema(), path);
                _parquetFiles[path] = file;

                file.AddRows(tmpPath);
                File.Delete(tmpPath);
            }

            return file;
        }

        private TestPlanRun GetPlanRun(TestStepRun run)
        {
            TestPlanRun? planRun;
            while (!_guidToPlanRuns.TryGetValue(run.Parent, out planRun))
            {
                run = _guidToStepRuns[run.Parent];
            }
            return planRun;
        }
    }
}
