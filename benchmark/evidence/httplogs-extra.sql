-- NOT Rally operations. The needles, ours, in every form the engine path can carry, so the
-- record shows which form reaches which tier. Values were picked from the data by count.
SELECT COUNT(*) FROM {table} WHERE request = 'GET /nlquery.fcg?ho=typhoon&po=5004&qr=guinness&si=&cb=0&cc=for+-New+York+Stock+Exchange&cl=1&clsub_1_46=5753&cn_1=New+York+Stock+Exchange&us=025 HTTP/1.0';                                              -- X0 equality on a rare request: Iceberg EQ, gram tier exact via kahshe, min/max on stock
SELECT COUNT(*) FROM {table} WHERE request LIKE '%guinness%';                                        -- X1 LIKE on its rare path segment: unpruned by rule
SELECT COUNT(*) FROM {table} WHERE regexp_like(lower(request), '(^|[^a-z0-9])guinness([^a-z0-9]|$)'); -- X2 token form: term tier via the overlay
SELECT COUNT(*) FROM {table} WHERE clientip = '71.162.18.0';                                              -- X3 equality on a rare client IP (a whole value in its own column)
SELECT COUNT(*) FROM {table} WHERE request = 'GET /images/hm_bg.jpg HTTP/1.0';                       -- X4 control: equality on a COMMON request
SELECT COUNT(*) FROM {table} WHERE request = 'GET /zzqx-absent-zzqx HTTP/1.0';                       -- X5 control: equality on an absent request
