using System;
using System.Collections.Frozen;
using System.Collections.Generic;

namespace Glacier.Polaris.Data
{
    /// <summary>
    /// Demonstrates using FrozenCollections and Non-GC heap arrays for zero-allocation schema lookups.
    /// This prevents GC pressure during high-throughput query planning.
    /// </summary>
    public static class SchemaRegistry
    {
        private static FrozenDictionary<string, Type> _globalSchemaCache;
        private static readonly object _lock = new object();

        // An array allocated on the Non-GC heap. 
        // It won't be scanned by the GC generation phases.
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

                // Allocate a lookup array directly on the POH (Pinned Object Heap)
                // In .NET 8/9/10 GC.AllocateArray with Pinned bypasses traditional GC scans for static data
                _fastTypeLookupTable = GC.AllocateArray<Type>(_globalSchemaCache.Count, pinned: true);
                
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
