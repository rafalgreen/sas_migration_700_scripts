/* Complex script with macros, arrays, and nested logic */
LIBNAME raw '/data/raw';
LIBNAME stage '/data/staging';
LIBNAME mart '/data/mart';

%INCLUDE '/macros/date_utils.sas';
%INCLUDE '/macros/validation.sas';

%MACRO process_monthly(year=, month=);
    %LET start_dt = %SYSFUNC(MDY(&month, 1, &year));
    %LET end_dt = %SYSFUNC(INTNX(MONTH, &start_dt, 0, E));

    DATA work.monthly_extract;
        SET raw.transactions;
        WHERE txn_date BETWEEN &start_dt AND &end_dt;
        KEEP txn_id customer_id txn_date amount category store_id;
    RUN;

    PROC SQL;
        CREATE TABLE work.customer_txns AS
        SELECT a.*, b.segment, b.region, b.acquisition_date
        FROM work.monthly_extract a
        LEFT JOIN raw.customer_dim b
            ON a.customer_id = b.customer_id;
    QUIT;

    DATA work.enriched_txns;
        SET work.customer_txns;
        BY customer_id txn_date;

        ARRAY cat_amounts{5} cat1-cat5;
        ARRAY cat_names{5} $20 _TEMPORARY_
            ('FOOD' 'ELECTRONICS' 'CLOTHING' 'HOME' 'OTHER');

        RETAIN cat1-cat5 0 prev_amount;

        IF FIRST.customer_id THEN DO;
            DO i = 1 TO 5;
                cat_amounts{i} = 0;
            END;
            prev_amount = .;
        END;

        DO i = 1 TO 5;
            IF UPCASE(category) = cat_names{i} THEN
                cat_amounts{i} + amount;
        END;

        IF prev_amount NE . THEN
            amount_change = amount - prev_amount;
        ELSE
            amount_change = 0;

        days_since_acq = txn_date - acquisition_date;

        IF days_since_acq <= 90 THEN tenure_group = 'NEW';
        ELSE IF days_since_acq <= 365 THEN tenure_group = 'DEVELOPING';
        ELSE IF days_since_acq <= 730 THEN tenure_group = 'MATURE';
        ELSE tenure_group = 'VETERAN';

        prev_amount = amount;
        DROP i;

        IF LAST.customer_id THEN OUTPUT;
    RUN;

    PROC TRANSPOSE DATA=work.enriched_txns
        OUT=work.txns_long (RENAME=(_NAME_=metric COL1=value));
        BY customer_id;
        VAR cat1-cat5;
    RUN;
%MEND process_monthly;

%MACRO run_all_months(year=);
    %DO m = 1 %TO 12;
        %process_monthly(year=&year, month=&m);

        PROC APPEND BASE=stage.yearly_txns DATA=work.enriched_txns;
        RUN;
    %END;
%MEND run_all_months;

%run_all_months(year=2025);

PROC SQL;
    CREATE TABLE mart.customer_summary AS
    SELECT customer_id,
           segment,
           region,
           tenure_group,
           SUM(amount) AS total_spend,
           COUNT(*) AS txn_count,
           MAX(txn_date) AS last_txn_date FORMAT=DATE9.
    FROM stage.yearly_txns
    GROUP BY customer_id, segment, region, tenure_group;
QUIT;
