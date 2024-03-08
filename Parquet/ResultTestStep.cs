using OpenTap;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Parquet;

public class Column
{
    public string Name { get; set; } = "Column";
    public ColumnType Type { get; set; } = ColumnType.StaticInt;
}

public enum ColumnType
{
    Disabled, StaticInt, StaticString, Linear, Sine, Random
}


public class ResultStep : TestStep
{
    public string ResultName { get; set; } = "TestResults";
    public int Count1 { get; set; } = 1000;
    public List<Column> Columns { get; set; }
    public int ColumnCount => Columns.Count;

    public override void Run()
    {
        ResultColumn[] columns = Columns.Where(c => c.Type != ColumnType.Disabled).Select(c => new ResultColumn(c.Name, GetResultData(c.Type).ToArray())).ToArray();
        Results.Publish(new ResultTable(ResultName, columns));
    }

    private IEnumerable<IConvertible> GetResultData(ColumnType columnType)
    {
        for (int i = 0; i < Count1; i++)
        {
            switch (columnType)
            {
                case ColumnType.Disabled:
                    yield break;
                case ColumnType.StaticInt:
                    yield return 0;
                    break;
                case ColumnType.StaticString:
                    yield return ResultName;
                    break;
                case ColumnType.Linear:
                    yield return i;
                    break;
                case ColumnType.Sine:
                    yield return (float)Math.Sin(i / (float)Count1);
                    break;
                case ColumnType.Random:
                    yield return new Random(i).Next();
                    break;
            }
        }
    }
}
