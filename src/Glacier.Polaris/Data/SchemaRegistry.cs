using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Glacier.Polaris.Data
{
    /// <summary>
    /// Uses FrozenCollections and contiguous arrays for fast schema lookups.
    /// </summary>
    public static class SchemaRegistry
    {
        private static FrozenDictionary<string, Type> _globalSchemaCache;
        private static readonly object _lock = new object();

        private static Type[]? _fastTypeLookupTable;

        static SchemaRegistry()
        {
            _globalSchemaCache = new Dictionary<string, Type>().ToFrozenDictionary();
        }

        public static void RegisterSchema(IEnumerable<KeyValuePair<string, Type>> schema)
        {
            lock (_lock)
            {
                var dict = new Dictionary<string, Type>(_globalSchemaCache);
                foreach (var kvp in schema)
                {
                    dict[kvp.Key] = kvp.Value;
                }

                _globalSchemaCache = dict.ToFrozenDictionary();
                _fastTypeLookupTable = new Type[_globalSchemaCache.Count];

                int i = 0;
                foreach (var type in _globalSchemaCache.Values)
                {
                    _fastTypeLookupTable[i++] = type;
                }
            }
        }

        public static Type? GetType(string columnName)
        {
            if (_globalSchemaCache.TryGetValue(columnName, out var type))
            {
                return type;
            }
            return null;
        }

        public static ReadOnlySpan<Type> GetFastLookupTable()
        {
            return _fastTypeLookupTable;
        }
    }
}
