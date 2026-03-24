/* Medium complexity - merge with retain and conditional logic */
LIBNAME sales '/data/sales';
LIBNAME ref '/data/reference';

%INCLUDE '/macros/date_utils.sas';

%LET report_date = %SYSFUNC(today());

DATA work.orders_enriched;
    MERGE sales.orders (IN=a)
          ref.products (IN=b);
    BY product_id;
    IF a;

    IF unit_price > 1000 THEN price_tier = 'PREMIUM';
    ELSE IF unit_price > 100 THEN price_tier = 'STANDARD';
    ELSE price_tier = 'BASIC';

    total_amount = quantity * unit_price;
    RENAME customer_name = cust_name;
    DROP temp_var1 temp_var2;
    FORMAT order_date DATE9.;
RUN;

PROC SORT DATA=work.orders_enriched;
    BY customer_id order_date;
RUN;

DATA work.customer_running_total;
    SET work.orders_enriched;
    BY customer_id;
    RETAIN running_total 0;
    IF FIRST.customer_id THEN running_total = 0;
    running_total + total_amount;
    IF LAST.customer_id THEN OUTPUT;
RUN;

PROC MEANS DATA=work.orders_enriched NOPRINT;
    CLASS price_tier;
    VAR total_amount quantity;
    OUTPUT OUT=work.tier_stats
        MEAN(total_amount) = avg_amount
        SUM(quantity) = total_qty
        N(total_amount) = order_count;
RUN;
