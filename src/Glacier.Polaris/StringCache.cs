using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace Glacier.Polaris
{
    public static class StringCache
    {
        private static bool _isEnabled = false;
        private static readonly ConcurrentDictionary<string, uint> _stringToCode = new();
        private static readonly ConcurrentList<string> _codeToString = new();

        public static void Enable() => _isEnabled = true;
        public static void Disable()
        {
            _isEnabled = false;
            _stringToCode.Clear();
            _codeToString.Clear();
        }

        public static bool IsEnabled => _isEnabled;

        public static uint GetOrCreate(string s)
        {
            if (s == null) return uint.MaxValue;
            if (_stringToCode.TryGetValue(s, out uint code)) return code;

            lock (_codeToString)
            {
                if (_stringToCode.TryGetValue(s, out code)) return code;
                code = (uint)_codeToString.Count;
                _stringToCode[s] = code;
                _codeToString.Add(s);
                return code;
            }
        }

        public static string? GetString(uint code)
        {
            if (code == uint.MaxValue) return null;
            return _codeToString[(int)code];
        }

        public static string[] GetRevMap()
        {
            lock (_codeToString)
            {
                return _codeToString.ToArray();
            }
        }

        // Simple concurrent list helper
        private class ConcurrentList<T>
        {
            private readonly List<T> _list = new();
            public void Add(T item) => _list.Add(item);
            public void Clear() => _list.Clear();
            public int Count => _list.Count;
            public T this[int index] => _list[index];
            public T[] ToArray() => _list.ToArray();
        }
    }
}
