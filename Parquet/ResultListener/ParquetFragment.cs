﻿using Parquet.Schema;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Parquet.Data;
using Parquet.Extensions;
using Parquet;
using ColumnKey = (string Name, System.Type Type);
using ColumnData = (object?[] Data, Parquet.Schema.DataField Field);

namespace OpenTap.Plugins.Parquet;

internal sealed class ParquetFragment : IDisposable
{
    private static readonly int RowGroupSize = 10_000;
    private readonly string _path;
    private readonly Stream _stream;
    private ParquetWriter? _writer;
    private ParquetSchema? _schema;
    private int _cacheSize;
    private readonly List<DataField> _fields;
    private readonly Dictionary<string, ColumnData> _cache;

    public ParquetFragment(string path)
    {
        _path = path;
        string? dirPath = Path.GetDirectoryName(path);
        if (!string.IsNullOrWhiteSpace(dirPath) && !Directory.Exists(dirPath))
        {
            Directory.CreateDirectory(dirPath);
        }
        _stream = System.IO.File.Open(_path, FileMode.Create, FileAccess.Write);
        _fields = new();
        _cache = new();
    }

    public ParquetFragment(ParquetFragment fragment)
    {
        _path = fragment._path + ".tmp";
        _stream = System.IO.File.Open(_path, FileMode.Create, FileAccess.Write);
        _cacheSize = fragment._cacheSize;
        _fields = fragment._fields;
        _cache = fragment._cache;
    }

    public bool AddRows(string? resultName, Guid? guid, Guid? parentId, Guid? stepId,
        Dictionary<string, IConvertible>? plan,
        Dictionary<string, IConvertible>? step,
        Dictionary<string, Array>? results)
    {
        int resultCount = results?.Values.Max(d => d.Length) ?? 1;
        bool fitsInCache = true;
        while (resultCount > 0)
        {
            int count = Math.Min(RowGroupSize - _cacheSize, resultCount);

            AddToCache("ResultName", typeof(string), Enumerable.Repeat<object?>(resultName, count).ToArray());
            AddToCache("Guid", typeof(Guid), Enumerable.Repeat<object?>(guid, count).ToArray());
            AddToCache("Parent", typeof(Guid), Enumerable.Repeat<object?>(parentId, count).ToArray());
            AddToCache("StepId", typeof(Guid), Enumerable.Repeat<object?>(stepId, count).ToArray());
            
            if (plan is not null)
                foreach (var item in plan)
                {
                    AddToCache("Plan/" + item.Key, item.Value.GetType(), Enumerable.Repeat<object?>(item.Value, count).ToArray());
                }
            if (step is not null)
                foreach (var item in step)
                {
                    AddToCache("Step/" + item.Key, item.Value.GetType(), Enumerable.Repeat<object?>(item.Value, count).ToArray());
                }
            if (results is not null)
                foreach(var item in results)
                {
                    AddToCache("Results/" + item.Key, item.Value.GetValue(0).GetType(), item.Value.Cast<object?>().Take(count).ToArray());
                }

            _cacheSize += count;
            resultCount -= count;
            foreach (var item in _cache)
            {
                if (item.Value.Data.Length < _cacheSize)
                {
                    AddToCache(item.Key, item.Value.Field.ClrType, Enumerable.Repeat<object?>(null, count).ToArray());
                }
            }

            if (!fitsInCache && _writer is not null)
            {
                return false;
            }
            
            if (_cacheSize >= RowGroupSize)
            {
                WriteCache();
            }
        }
        return true;
        

        void AddToCache(string name, Type type, object?[] values)
        {
            Type parquetType = GetParquetType(type);
            if (!_cache.TryGetValue(name, out ColumnData data))
            {
                fitsInCache = false;
                DataField field = new DataField(name, parquetType, true);
                data = (new object?[RowGroupSize], field);
                _cache.Add(name, data);
                _fields.Add(data.Field);
            }

            Array.Copy(values, 0, data.Data, _cacheSize, values.Length);
        }
    }

    public void WriteCache()
    {
        if (_writer is null || _schema is null)
        {
            _schema = new ParquetSchema(_fields);
            _writer = ParquetWriter.CreateAsync(_schema, _stream).Result;
        }
        ParquetRowGroupWriter rowGroupWriter = _writer.CreateRowGroup();
        for (var i = 0; i < _schema.DataFields.Length; i++)
        {
            ColumnData data = _cache[_schema.DataFields[i].Name];
            Array arr;
            if (data.Field.ClrType == typeof(string))
            {
                arr = data.Data.Take(_cacheSize).Select(o => o?.ToString()).ToArray();
            }
            else
            {
                arr = Array.CreateInstance(_schema.DataFields[i].ClrType.AsNullable(), _cacheSize);
                Array.Copy(data.Data, arr, _cacheSize);
            }
            DataColumn column = new DataColumn(_schema.DataFields[i], arr);
            rowGroupWriter.WriteColumnAsync(column).Wait();
        }
        _cacheSize = 0;
        rowGroupWriter.Dispose();
    }

    public void Dispose()
    {
        _writer?.Dispose();
        _stream.Flush();
        _stream.Dispose();
    }
    
    private static Type GetParquetType(Type type)
    {
        if (type.IsEnum)
        {
            return typeof(string);
        }

        return type;
    }
}
