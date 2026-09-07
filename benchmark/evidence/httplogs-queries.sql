-- Rally http_logs read operations, translated to SQL over the PARSED table (ts, clientip,
-- request, status, size). Rally's bodies are Elasticsearch requests (operations/default.json);
-- each line names the operation it stands for. `range` is relative to Rally's clock; here it
-- is the corpus's own last ten weeks. ES returns 10 hits by default, hence LIMIT 10.
SELECT COUNT(*) FROM {table};                                                                                                      -- default (match_all)
SELECT COUNT(*) FROM {table} WHERE request = 'GET / HTTP/1.0';                                                                     -- term (request.raw)
SELECT COUNT(*) FROM {table} WHERE ts >= TIMESTAMP '1998-05-15 00:00:00' AND ts < TIMESTAMP '1998-07-27 00:00:00';                 -- range (@timestamp)
SELECT COUNT(*) FROM {table} WHERE ts >= TIMESTAMP '1998-06-15 00:00:00' AND ts < TIMESTAMP '1998-06-16 00:00:00' AND status = 200; -- 200s-in-range
SELECT COUNT(*) FROM {table} WHERE ts >= TIMESTAMP '1998-06-15 00:00:00' AND ts < TIMESTAMP '1998-06-16 00:00:00' AND status = 400; -- 400s-in-range
SELECT date_trunc('hour', ts) AS h, COUNT(*) FROM {table} GROUP BY 1 ORDER BY 1;                                                    -- hourly_agg (date_histogram)
SELECT * FROM {table} ORDER BY ts DESC LIMIT 10;                                                                                    -- desc_sort_timestamp
SELECT * FROM {table} ORDER BY ts ASC LIMIT 10;                                                                                     -- asc_sort_timestamp
SELECT * FROM {table} ORDER BY size DESC LIMIT 10;                                                                                  -- sort_size_desc
SELECT * FROM {table} ORDER BY status DESC LIMIT 10;                                                                                -- sort_status_desc
SELECT DISTINCT request FROM {table} WHERE request LIKE 'GET image%' LIMIT 10;                                                      -- terms_enum (prefix "GET image")
