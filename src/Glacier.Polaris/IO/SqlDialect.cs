using System;
using System.Data.Common;
using System.Globalization;

namespace Glacier.Polaris.IO
{
    /// <summary>
    /// Identifies the SQL dialect / provider used when constructing a <see cref="SqlReader"/>.
    /// </summary>
    public enum SqlDialect
    {
        /// <summary>
        /// Microsoft SQL Server via <c>Microsoft.Data.SqlClient</c>.
        /// </summary>
        SqlServer,

        /// <summary>
        /// PostgreSQL via <c>Npgsql</c>.
        /// </summary>
        PostgreSql,

        /// <summary>
        /// Any ADO.NET-compatible provider supplied directly as a <see cref="DbConnection"/>.
        /// Use this for SQLite, Oracle, MySQL, etc.
        /// </summary>
        Generic
    }

    internal static class SqlDialectFactory
    {
        /// <summary>
        /// Creates an open <see cref="DbConnection"/> for the given dialect and connection string.
        /// Returns <c>null</c> for <see cref="SqlDialect.Generic"/> — the caller must supply its own connection.
        /// </summary>
        public static DbConnection Create(SqlDialect dialect, string connectionString) => dialect switch
        {
            SqlDialect.SqlServer  => CreateSqlServer(connectionString),
            SqlDialect.PostgreSql => CreatePostgres(connectionString),
            _ => throw new NotSupportedException(
                    $"Use the DbConnection overload for dialect '{dialect}'.")
        };

        private static DbConnection CreateSqlServer(string cs)
        {
            var conn = new Microsoft.Data.SqlClient.SqlConnection(cs);
            return conn;
        }

        private static DbConnection CreatePostgres(string cs)
        {
            var conn = new Npgsql.NpgsqlConnection(cs);
            return conn;
        }

        internal static string EscapeIdentifier(SqlDialect dialect, string identifier)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(identifier);
            if (identifier.Contains('\0'))
                throw new ArgumentException("SQL identifier cannot contain NUL characters.", nameof(identifier));

            return dialect switch
            {
                SqlDialect.SqlServer => $"[{identifier.Replace("]", "]]")}]",
                _ => $"\"{identifier.Replace("\"", "\"\"")}\""
            };
        }

        /// <summary>
        /// Wraps a query with a column projection clause appropriate for the dialect.
        /// When <paramref name="columns"/> is null/empty the original query is returned unchanged.
        /// </summary>
        public static string ApplyProjection(SqlDialect dialect, string query, string[]? columns)
        {
            if (columns == null || columns.Length == 0) return query;
            string cols = string.Join(", ", Array.ConvertAll(columns, c => EscapeIdentifier(dialect, c)));

            if (dialect == SqlDialect.SqlServer)
            {
                var clauses = AnalyzeTopLevelClauses(query);
                if (clauses.HasTopLevelOrderBy && !clauses.HasTopLevelTop && !clauses.HasTopLevelOffset && !clauses.HasTopLevelFor)
                {
                    return $"SELECT {cols} FROM ({query}\nOFFSET 0 ROWS) __ag_sub";
                }
            }

            return $"SELECT {cols} FROM ({query}\n) __ag_sub";
        }

        /// <summary>
        /// Wraps a query with a TOP/LIMIT clause appropriate for the dialect.
        /// </summary>
        public static string ApplyLimit(SqlDialect dialect, string query, int? limit)
        {
            if (limit == null) return query;
            ArgumentOutOfRangeException.ThrowIfNegative(limit.Value);

            string limitStr = limit.Value.ToString(CultureInfo.InvariantCulture);
            var clauses = AnalyzeTopLevelClauses(query);

            if (dialect == SqlDialect.SqlServer)
            {
                if (clauses.HasTopLevelOrderBy && !clauses.HasTopLevelTop && !clauses.HasTopLevelOffset && !clauses.HasTopLevelFor)
                {
                    return $"{query}\nOFFSET 0 ROWS FETCH NEXT {limitStr} ROWS ONLY";
                }
                return $"SELECT TOP ({limitStr}) * FROM ({query}\n) __ag_sub";
            }

            if (clauses.HasTopLevelLimit || clauses.HasTopLevelFetch)
            {
                return $"SELECT * FROM ({query}\n) __ag_sub LIMIT {limitStr}";
            }

            return $"{query}\nLIMIT {limitStr}";
        }

        internal static string StripTrailingSemicolons(string sql)
        {
            if (string.IsNullOrEmpty(sql))
                return string.Empty;

            int firstTrailingSemi = -1;
            System.Collections.Generic.List<int>? extraTrailingSemis = null;

            int i = 0;
            int len = sql.Length;
            while (i < len)
            {
                char c = sql[i];

                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }

                // Line comment: -- ...
                if (c == '-' && i + 1 < len && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n') i++;
                    continue;
                }

                // Block comment: /* ... */
                if (c == '/' && i + 1 < len && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i = Math.Min(len, i + 2);
                    continue;
                }

                if (c == ';')
                {
                    if (firstTrailingSemi < 0)
                    {
                        firstTrailingSemi = i;
                    }
                    else
                    {
                        extraTrailingSemis ??= new System.Collections.Generic.List<int>();
                        extraTrailingSemis.Add(i);
                    }
                    i++;
                    continue;
                }

                // Any non-trivia, non-semicolon token clears previously seen semicolons.
                firstTrailingSemi = -1;
                extraTrailingSemis?.Clear();

                if (c == '\'')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '\'')
                        {
                            i++;
                            if (i < len && sql[i] == '\'') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                if (c == '"')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '"')
                        {
                            i++;
                            if (i < len && sql[i] == '"') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                if (c == '[')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == ']')
                        {
                            i++;
                            if (i < len && sql[i] == ']') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    continue;
                }

                i++;
            }

            if (firstTrailingSemi < 0)
                return sql.TrimEnd();

            var sb = new System.Text.StringBuilder(len - 1);
            int lastCopy = 0;
            sb.Append(sql, lastCopy, firstTrailingSemi - lastCopy);
            lastCopy = firstTrailingSemi + 1;

            if (extraTrailingSemis != null)
            {
                foreach (int idx in extraTrailingSemis)
                {
                    sb.Append(sql, lastCopy, idx - lastCopy);
                    lastCopy = idx + 1;
                }
            }

            if (lastCopy < len)
                sb.Append(sql, lastCopy, len - lastCopy);

            return sb.ToString().TrimEnd();
        }

        private readonly struct TopLevelClauseInfo
        {
            public bool HasTopLevelOrderBy { get; init; }
            public bool HasTopLevelTop { get; init; }
            public bool HasTopLevelOffset { get; init; }
            public bool HasTopLevelFor { get; init; }
            public bool HasTopLevelLimit { get; init; }
            public bool HasTopLevelFetch { get; init; }
        }

        private static TopLevelClauseInfo AnalyzeTopLevelClauses(string sql)
        {
            bool hasOrderBy = false;
            bool hasTop = false;
            bool hasOffset = false;
            bool hasFor = false;
            bool hasLimit = false;
            bool hasFetch = false;
            bool inSelectProjection = false;

            int parenDepth = 0;
            bool prevWasDot = false;
            string? prevTopLevelWord = null;
            int i = 0;
            int len = sql.Length;

            while (i < len)
            {
                char c = sql[i];

                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }

                // Line comment: -- ...
                if (c == '-' && i + 1 < len && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n') i++;
                    continue;
                }

                // Block comment: /* ... */
                if (c == '/' && i + 1 < len && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i = Math.Min(len, i + 2);
                    continue;
                }

                // Single-quoted string literal
                if (c == '\'')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '\'')
                        {
                            i++;
                            if (i < len && sql[i] == '\'') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    continue;
                }

                // Double-quoted identifier
                if (c == '"')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == '"')
                        {
                            i++;
                            if (i < len && sql[i] == '"') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    continue;
                }

                // T-SQL bracketed identifier [...]
                if (c == '[')
                {
                    i++;
                    while (i < len)
                    {
                        if (sql[i] == ']')
                        {
                            i++;
                            if (i < len && sql[i] == ']') { i++; continue; }
                            break;
                        }
                        i++;
                    }
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    continue;
                }

                if (c == '(')
                {
                    parenDepth++;
                    prevWasDot = false;
                    if (parenDepth == 1) prevTopLevelWord = null;
                    i++;
                    continue;
                }

                if (c == ')')
                {
                    if (parenDepth > 0) parenDepth--;
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    i++;
                    continue;
                }

                // Parameter sigils (@param, $param, :param) - treat as a single token so @offset / :limit aren't keywords
                if (c == '@' || c == '$' || (c == ':' && i + 1 < len && (char.IsAsciiLetter(sql[i + 1]) || sql[i + 1] == '_')))
                {
                    i++;
                    while (i < len && (char.IsAsciiLetterOrDigit(sql[i]) || sql[i] == '_')) i++;
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    continue;
                }

                if (char.IsAsciiLetter(c) || c == '_')
                {
                    int start = i;
                    i++;
                    while (i < len && (char.IsAsciiLetterOrDigit(sql[i]) || sql[i] == '_')) i++;

                    if (parenDepth == 0 && !prevWasDot)
                    {
                        ReadOnlySpan<char> word = sql.AsSpan(start, i - start);
                        bool prevWasAs = string.Equals(prevTopLevelWord, "AS", StringComparison.Ordinal);

                        if (prevWasAs)
                        {
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("AS", StringComparison.OrdinalIgnoreCase))
                        {
                            prevTopLevelWord = "AS";
                        }
                        else if (word.Equals("SELECT", StringComparison.OrdinalIgnoreCase))
                        {
                            inSelectProjection = true;
                            prevTopLevelWord = "SELECT";
                        }
                        else if (word.Equals("DISTINCT", StringComparison.OrdinalIgnoreCase))
                        {
                            prevTopLevelWord = prevTopLevelWord == "SELECT" ? "DISTINCT" : null;
                        }
                        else if (word.Equals("ALL", StringComparison.OrdinalIgnoreCase))
                        {
                            prevTopLevelWord = prevTopLevelWord == "SELECT" ? "ALL" : null;
                        }
                        else if (word.Equals("FROM", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("WHERE", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("GROUP", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("HAVING", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("WINDOW", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("QUALIFY", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("UNION", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("INTERSECT", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("EXCEPT", StringComparison.OrdinalIgnoreCase) ||
                                 word.Equals("INTO", StringComparison.OrdinalIgnoreCase))
                        {
                            inSelectProjection = false;
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("ORDER", StringComparison.OrdinalIgnoreCase))
                        {
                            prevTopLevelWord = "ORDER";
                        }
                        else if (word.Equals("BY", StringComparison.OrdinalIgnoreCase) && prevTopLevelWord == "ORDER")
                        {
                            hasOrderBy = true;
                            inSelectProjection = false;
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("FOR", StringComparison.OrdinalIgnoreCase))
                        {
                            prevTopLevelWord = "FOR";
                        }
                        else if ((word.Equals("XML", StringComparison.OrdinalIgnoreCase) ||
                                  word.Equals("JSON", StringComparison.OrdinalIgnoreCase) ||
                                  word.Equals("BROWSE", StringComparison.OrdinalIgnoreCase)) && prevTopLevelWord == "FOR")
                        {
                            hasFor = true;
                            inSelectProjection = false;
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("TOP", StringComparison.OrdinalIgnoreCase))
                        {
                            if (prevTopLevelWord is "SELECT" or "DISTINCT" or "ALL")
                            {
                                int nextPos = SkipWhitespaceAndComments(sql, i);
                                if (IsTopCountStart(sql, nextPos))
                                    hasTop = true;
                            }
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("OFFSET", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!inSelectProjection && (hasOrderBy || hasLimit))
                            {
                                int nextPos = SkipWhitespaceAndComments(sql, i);
                                if (IsOffsetOrLimitValueStart(sql, nextPos))
                                    hasOffset = true;
                            }
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("LIMIT", StringComparison.OrdinalIgnoreCase))
                        {
                            int nextPos = SkipWhitespaceAndComments(sql, i);
                            bool isValueStart = IsOffsetOrLimitValueStart(sql, nextPos);
                            bool isLimitAll = IsNextWord(sql, nextPos, "ALL") &&
                                              (!inSelectProjection || IsEndOrClauseAfterWord(sql, nextPos));
                            if (isValueStart || isLimitAll)
                            {
                                hasLimit = true;
                                inSelectProjection = false;
                            }
                            prevTopLevelWord = null;
                        }
                        else if (word.Equals("FETCH", StringComparison.OrdinalIgnoreCase))
                        {
                            int nextPos = SkipWhitespaceAndComments(sql, i);
                            if (IsNextWord(sql, nextPos, "FIRST") || IsNextWord(sql, nextPos, "NEXT"))
                            {
                                int afterKeyword = AdvanceWordAndTrivia(sql, nextPos);
                                bool hasFetchTarget = IsOffsetOrLimitValueStart(sql, afterKeyword) ||
                                                      IsNextWord(sql, afterKeyword, "ROW") ||
                                                      IsNextWord(sql, afterKeyword, "ROWS");
                                if (hasFetchTarget)
                                {
                                    hasFetch = true;
                                    inSelectProjection = false;
                                }
                            }
                            prevTopLevelWord = null;
                        }
                        else
                        {
                            prevTopLevelWord = null;
                        }
                    }

                    prevWasDot = false;
                    continue;
                }

                if (char.IsAsciiDigit(c))
                {
                    while (i < len && (char.IsAsciiLetterOrDigit(sql[i]) || sql[i] == '_')) i++;
                    prevWasDot = false;
                    if (parenDepth == 0) prevTopLevelWord = null;
                    continue;
                }

                prevWasDot = c == '.';
                if (parenDepth == 0) prevTopLevelWord = null;
                i++;
            }

            return new TopLevelClauseInfo
            {
                HasTopLevelOrderBy = hasOrderBy,
                HasTopLevelTop = hasTop,
                HasTopLevelOffset = hasOffset,
                HasTopLevelFor = hasFor,
                HasTopLevelLimit = hasLimit,
                HasTopLevelFetch = hasFetch
            };
        }

        private static int AdvanceWordAndTrivia(string sql, int wordStart)
        {
            int pos = wordStart;
            while (pos < sql.Length && (char.IsAsciiLetterOrDigit(sql[pos]) || sql[pos] == '_'))
                pos++;
            return SkipWhitespaceAndComments(sql, pos);
        }

        private static bool IsEndOrClauseAfterWord(string sql, int wordStart)
        {
            int after = AdvanceWordAndTrivia(sql, wordStart);
            if (after >= sql.Length || sql[after] == ';')
                return true;
            return IsNextWord(sql, after, "OFFSET") ||
                   IsNextWord(sql, after, "FOR") ||
                   IsNextWord(sql, after, "UNION") ||
                   IsNextWord(sql, after, "INTERSECT") ||
                   IsNextWord(sql, after, "EXCEPT");
        }

        private static int SkipWhitespaceAndComments(string sql, int i)
        {
            int len = sql.Length;
            while (i < len)
            {
                char c = sql[i];
                if (char.IsWhiteSpace(c))
                {
                    i++;
                    continue;
                }
                if (c == '-' && i + 1 < len && sql[i + 1] == '-')
                {
                    i += 2;
                    while (i < len && sql[i] != '\n') i++;
                    continue;
                }
                if (c == '/' && i + 1 < len && sql[i + 1] == '*')
                {
                    i += 2;
                    while (i + 1 < len && !(sql[i] == '*' && sql[i + 1] == '/')) i++;
                    i = Math.Min(len, i + 2);
                    continue;
                }
                break;
            }
            return i;
        }

        private static bool IsTopCountStart(string sql, int pos)
        {
            if (pos >= sql.Length) return false;
            char c = sql[pos];
            return c == '(' || char.IsAsciiDigit(c) || c == '@' || c == '?' || c == ':';
        }

        private static bool IsOffsetOrLimitValueStart(string sql, int pos)
        {
            if (pos >= sql.Length) return false;
            char c = sql[pos];
            return char.IsAsciiDigit(c) || c == '(' || c == '@' || c == '?' || c == ':' || c == '$';
        }

        private static bool IsNextWord(string sql, int pos, string expected)
        {
            if (pos >= sql.Length) return false;
            int start = pos;
            if (!char.IsAsciiLetter(sql[pos]) && sql[pos] != '_') return false;
            pos++;
            while (pos < sql.Length && (char.IsAsciiLetterOrDigit(sql[pos]) || sql[pos] == '_')) pos++;
            return sql.AsSpan(start, pos - start).Equals(expected, StringComparison.OrdinalIgnoreCase);
        }
    }
}
