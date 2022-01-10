
/*
Purchase amount by Year
*/
SELECT
    TO_CHAR(order_dttm, 'YYYY') AS year,
    SUM(total_owed)
FROM purchase
GROUP BY 1
ORDER BY year ASC
;

