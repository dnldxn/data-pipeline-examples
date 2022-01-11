
CREATE TABLE features
AS
SELECT 
    cust_id,
    call_dttm,
    COUNT(*) AS calls_1m,
    EARLIEST_BY_OFFSET(call_dttm) AS earliest_call,
    LATEST_BY_OFFSET(call_dttm) AS latest_call
FROM IVR
GROUP BY cust_id, call_dttm
EMIT CHANGES
;