/* Downstream report - merges customer region data with order stats */
DATA work.regional_orders;
    MERGE work.region_summary (IN=a)
          work.tier_stats (IN=b);
    BY region;
    IF a;
RUN;

DATA work.customer_order_summary;
    SET work.customer_running_total;
    WHERE running_total > 500;
RUN;
