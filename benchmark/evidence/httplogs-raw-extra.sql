-- NOT Rally operations. The same needles inside the raw log line.
SELECT COUNT(*) FROM {table} WHERE message LIKE '%71.162.18.0 - -%';                                      -- Y0 the rare IP inside the line: LIKE, unpruned by rule
SELECT COUNT(*) FROM {table} WHERE message LIKE '%guinness%';                                         -- Y1 the rare path segment inside the line: LIKE, unpruned
SELECT COUNT(*) FROM {table} WHERE regexp_like(lower(message), '(^|[^a-z0-9])guinness([^a-z0-9]|$)');  -- Y2 its token form: term tier
SELECT COUNT(*) FROM {table} WHERE regexp_like(lower(message), '(^|[^a-z0-9])zzqxabsent([^a-z0-9]|$)'); -- Y3 control: absent token
SELECT COUNT(*) FROM {table};                                                                        -- Y4 control: count
