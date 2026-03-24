/* Simple ETL - load and filter customer data */
LIBNAME mylib '/data/warehouse';

DATA work.customers_filtered;
    SET mylib.customers;
    WHERE age >= 18 AND status = 'ACTIVE';
    KEEP customer_id name age status region;
RUN;

PROC SORT DATA=work.customers_filtered;
    BY region customer_id;
RUN;

PROC SQL;
    CREATE TABLE work.region_summary AS
    SELECT region,
           COUNT(*) AS customer_count,
           AVG(age) AS avg_age
    FROM work.customers_filtered
    GROUP BY region;
QUIT;
