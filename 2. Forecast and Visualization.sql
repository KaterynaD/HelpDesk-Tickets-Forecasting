-- Build Simple Forecasting model
CREATE OR REPLACE forecast mytest_db.tickets.tickets_cnt_daily_simple_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_training'),
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt'
);

-- Show models to confirm training has completed
use schema mytest_db.tickets;
SHOW forecast;

-- Creating and Visualizing Predictions

-- Create predictions, and save results to a table:  
CALL mytest_db.tickets.tickets_cnt_daily_simple_forecast!FORECAST(FORECASTING_PERIODS => 30, CONFIG_OBJECT => {'prediction_interval': .9});
CREATE OR REPLACE TABLE mytest_db.tickets.simple_forecast_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);


-- Visualize the results, overlaid on top of one another: 
SELECT
    CreatedDate,
    TicketsCnt AS Training_TicketsCnt,
    NULL AS Testing_TicketsCnt,
    NULL AS forecast
FROM
    mytest_db.tickets.TicketsDaily_training
WHERE
    CreatedDate > '2022-12-01'
UNION
SELECT
    coalesce(ts, CreatedDate) AS CreatedDate,
    NULL AS Training_TicketsCnt,
    TicketsCnt AS Testing_TicketsCnt,
    forecast AS forecast
FROM
    mytest_db.tickets.TicketsDaily_testing t
    full outer join  mytest_db.tickets.simple_forecast_predictions f
    on t.Createddate=f.ts
WHERE
    t.CreatedDate between '2023-01-01' and '2023-01-31'   
ORDER BY
    CreatedDate asc;

-- get Feature Importance
CALL mytest_db.tickets.tickets_cnt_daily_simple_forecast!explain_feature_importance();

-- Evaluate model performance:
CALL mytest_db.tickets.tickets_cnt_daily_simple_forecast!show_evaluation_metrics();

/*------------------------------------ Forecasting with Holidays -----------------------------------------------------------------------*/

--Forecasting with company holidays
CREATE OR REPLACE forecast mytest_db.tickets.tickets_cnt_daily_cal_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_training'),
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt'
);


-- Creating and Visualizing Predictions


-- Create predictions, and save results to a table:  
CALL mytest_db.tickets.tickets_cnt_daily_cal_forecast!FORECAST( 
INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_forecasting'),
TIMESTAMP_COLNAME => 'CreatedDate');
CREATE OR REPLACE TABLE mytest_db.tickets.cal_forecast_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);

-- Visualize the results, overlaid on top of one another: 
SELECT
    CreatedDate,
    TicketsCnt AS Training_TicketsCnt,
    NULL AS Testing_TicketsCnt,
    NULL AS simple_forecast,
    NULL AS cal_forecast
FROM
    mytest_db.tickets.TicketsDaily_cal_training
WHERE
    CreatedDate > '2022-12-01'
UNION
SELECT
    coalesce(sf.ts, cf.ts, CreatedDate) AS CreatedDate,
    NULL AS Training_TicketsCnt,
    t.TicketsCnt AS Testing_TicketsCnt,
    sf.forecast AS simple_forecast,
    cf.forecast AS cal_forecast,
FROM
    mytest_db.tickets.TicketsDaily_cal_testing t
    full outer join  mytest_db.tickets.simple_forecast_predictions sf
    on t.Createddate=sf.ts
    full outer join  mytest_db.tickets.cal_forecast_predictions cf
    on t.Createddate=cf.ts
WHERE
    t.CreatedDate between '2023-01-01' and '2023-01-31'   
ORDER BY
    CreatedDate asc;

-- get Feature Importance
CALL mytest_db.tickets.tickets_cnt_daily_cal_forecast!explain_feature_importance();

-- Evaluate model performance:
CALL mytest_db.tickets.tickets_cnt_daily_cal_forecast!show_evaluation_metrics();

/*------------------------------------ Forecasting by Ticket Group  -----------------------------------------------------------------------*/


-- Train Model;
CREATE OR REPLACE forecast mytest_db.tickets.tickets_cnt_daily_group_cal_forecast (
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_grp_training'),
    SERIES_COLNAME => 'TicketGroup',
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt'
);

-- Creating and Visualizing Predictions


-- Create predictions, and save results to a table:  
CALL mytest_db.tickets.tickets_cnt_daily_group_cal_forecast!FORECAST( 
INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_grp_forecasting'),
 SERIES_COLNAME => 'TicketGroup',
TIMESTAMP_COLNAME => 'CreatedDate');
CREATE OR REPLACE TABLE mytest_db.tickets.cal_grp_forecast_predictions AS (
    SELECT
        *
    FROM
        TABLE(RESULT_SCAN(-1))
);

select *
from mytest_db.tickets.cal_grp_forecast_predictions
limit 100

-- Visualize the results, overlaid on top of one another: 
SELECT
    CreatedDate,
    TicketsCnt AS Training_TicketsCnt,
    NULL AS Testing_TicketsCnt,
    NULL AS forecast
FROM
    mytest_db.tickets.TicketsDaily_cal_grp_training
WHERE
    CreatedDate > '2022-12-01'
    and TicketGroup='Other'
UNION
SELECT
    coalesce(cf.ts, CreatedDate) AS CreatedDate,
    NULL AS Training_TicketsCnt,
    t.TicketsCnt AS Testing_TicketsCnt,
    cf.forecast AS forecast
FROM
    mytest_db.tickets.TicketsDaily_cal_grp_testing t
    full outer join  mytest_db.tickets.cal_grp_forecast_predictions cf
    on t.Createddate=cf.ts
    and t.TicketGroup =cf.Series
WHERE
    t.CreatedDate between '2023-01-01' and '2023-01-31'  
    and t.TicketGroup='Other'
ORDER BY
    CreatedDate asc;

    -- get Feature Importance
CALL mytest_db.tickets.tickets_cnt_daily_group_cal_forecast!explain_feature_importance();
SELECT * FROM TABLE(RESULT_SCAN(-1))
WHERE Rank<3
ORDER BY Series, Rank


-- Evaluate model performance:
CALL mytest_db.tickets.tickets_cnt_daily_group_cal_forecast!show_evaluation_metrics();
SELECT * FROM TABLE(RESULT_SCAN(-1))
WHERE Series='Other'