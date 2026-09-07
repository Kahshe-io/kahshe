-- NOT ClickBench queries. Run after the standard 43 by clickbench-run.py --extra, and
-- recorded separately: the token form is a DIFFERENT predicate from LIKE '%google%'
-- (googleusercontent, 26google and hellgoogle are substrings, not the token), so the
-- row counts differ and neither number stands in for the other. X0 is the shape the
-- lab overlay pushes to kahshe's term tier (docs/ARCHITECTURE.md §8); X1/X2 are a needle
-- token picked from the data, in both forms, for the pruning the term tier is built for.
SELECT COUNT(*) FROM {table} WHERE regexp_like(lower(URL), '(^|[^a-z0-9])google([^a-z0-9]|$)');
SELECT COUNT(*) FROM {table} WHERE regexp_like(lower(URL), '(^|[^a-z0-9])offilialog([^a-z0-9]|$)');
SELECT COUNT(*) FROM {table} WHERE URL LIKE '%offilialog%';
