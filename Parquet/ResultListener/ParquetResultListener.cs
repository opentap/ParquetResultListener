﻿using Parquet;
using Parquet.Data;
using Parquet.Schema;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace OpenTap.Plugins.Parquet
{
    [Display("Parquet", "Save results in a Parquet file", "Database")]
    public sealed class ParquetResultListener : ResultListener
    {
        internal static new TraceSource Log { get; } = OpenTap.Log.CreateSource("Parquet");

        private readonly Dictionary<Guid, TestPlanRun> _guidToPlanRuns = new Dictionary<Guid, TestPlanRun>();
        private readonly Dictionary<Guid, TestStepRun> _guidToStepRuns = new Dictionary<Guid, TestStepRun>();
        private readonly HashSet<Guid> _hasWrittenParameters = new HashSet<Guid>();
        private readonly Dictionary<string, ParquetResult> _results = new Dictionary<string, ParquetResult>();

        [Display("File path", "The file path of the parquet file(s). Can use <ResultType> to have one file per result type.")]
        [FilePath(FilePathAttribute.BehaviorChoice.Save)]
        public MacroString FilePath { get; set; } = new MacroString() { Text = "Results/<TestPlanName>.<Date>/<ResultType>.parquet" };

        [Display("Delete on publish", "If true the files will be removed when published as artifacts.")]
        public bool DeleteOnPublish { get; set; } = false;

        [Display("Method")]
        public CompressionMethod CompressionMethod { get; set; }

        [Display("Level")]
        public CompressionLevel CompressionLevel { get; set; }

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
        }

        public override void OnTestPlanRunStart(TestPlanRun planRun)
        {
            base.OnTestPlanRunStart(planRun);

            _guidToPlanRuns[planRun.Id] = planRun;
            GetFile(planRun).AddPlanRow(planRun);
        }

        public override void OnTestPlanRunCompleted(TestPlanRun planRun, Stream logStream)
        {
            base.OnTestPlanRunCompleted(planRun, logStream);
            
            foreach (KeyValuePair<string,ParquetResult> parquetResult in _results)
            {
                parquetResult.Value.Dispose();
            }
            _results.Clear();

            _guidToPlanRuns.Clear();
            _guidToStepRuns.Clear();
        }

        public override void OnTestStepRunStart(TestStepRun stepRun)
        {
            _guidToStepRuns[stepRun.Id] = stepRun;
            base.OnTestStepRunStart(stepRun);
        }

        public override void OnTestStepRunCompleted(TestStepRun stepRun)
        {
            base.OnTestStepRunCompleted(stepRun);

            if (!_hasWrittenParameters.Contains(stepRun.Id))
            {
                TestPlanRun planRun = GetPlanRun(stepRun);
                
                GetFile(planRun).AddStepRow(stepRun);
                _hasWrittenParameters.Add(stepRun.Id);
            }
        }

        public override void OnResultPublished(Guid stepRunId, ResultTable result)
        {
            base.OnResultPublished(stepRunId, result);
            TestStepRun stepRun = _guidToStepRuns[stepRunId];
            TestPlanRun planRun = GetPlanRun(stepRun);

            GetFile(planRun, result.Name).AddResultRow(stepRun, result);

            _hasWrittenParameters.Add(stepRunId);
        }

        private ParquetResult GetFile(TestPlanRun planRun, string resultType = "Plan")
        {
            string path = FilePath.Expand(planRun, planRun.StartTime, "./", new Dictionary<string, object>
            {
                { "ResultType", resultType }
            });

            if (!_results.TryGetValue(path, out ParquetResult? result))
            {
                result = new ParquetResult(path);
                _results.Add(path, result);
            }

            return result;
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
