/* Database-sourced ETL — reads from DB2 and MSSQL, imports Excel from S3 */
LIBNAME dwh DB2 DATABASE=warehouse SCHEMA=dbo;
LIBNAME crm ODBC DSN=crm_mssql SCHEMA=sales;

DATA work.combined_customers;
    SET dwh.customers;
    WHERE active_flag = 1;
    KEEP customer_id name region segment;
RUN;

PROC SQL;
    CONNECT TO DB2 (DATABASE=warehouse);
    CREATE TABLE work.db2_orders AS
    SELECT * FROM CONNECTION TO DB2 (
        SELECT order_id, customer_id, order_date, total
        FROM dbo.orders
        WHERE order_date >= '2024-01-01'
    );
    DISCONNECT FROM DB2;
QUIT;

PROC IMPORT DATAFILE="/shared/data/products.xlsx"
    OUT=work.products
    DBMS=XLSX
    REPLACE;
    SHEET="Sheet1";
    GETNAMES=YES;
RUN;

PROC SQL;
    CREATE TABLE work.enriched AS
    SELECT a.*, b.order_date, b.total, c.product_name
    FROM work.combined_customers a
    LEFT JOIN work.db2_orders b ON a.customer_id = b.customer_id
    LEFT JOIN work.products c ON b.order_id = c.order_id;
QUIT;
