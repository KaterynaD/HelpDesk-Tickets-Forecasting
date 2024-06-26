-- Create the model: UNSUPERVISED method
CREATE OR REPLACE snowflake.ml.anomaly_detection mytest_db.tickets.tickets_cnt_daily_group_anomaly(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_grp_training'),
    SERIES_COLNAME => 'TicketGroup',
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt',
    LABEL_COLNAME => ''
); 

-- Call the model and store the results into table; this could take ~10-20 secs; please be patient
CALL mytest_db.tickets.tickets_cnt_daily_group_anomaly!DETECT_ANOMALIES(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_cal_grp_testing'),
    SERIES_COLNAME => 'TicketGroup',
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt',
    CONFIG_OBJECT => {'prediction_interval': 0.95}
);

-- Create a table from the results
CREATE OR REPLACE TABLE mytest_db.tickets.tickets_cnt_daily_group_anomaly AS (
    SELECT *
    FROM TABLE(RESULT_SCAN(-1))
);

-- Review the results
SELECT * FROM mytest_db.tickets.tickets_cnt_daily_group_anomaly
where is_anomaly=TRUE
and series='Other'


-- Query to identify trends
SELECT series, is_anomaly, count(is_anomaly) AS num_records
FROM mytest_db.tickets.tickets_cnt_daily_group_anomaly
WHERE is_anomaly =1
GROUP BY ALL
ORDER BY num_records DESC


SELECT 
t.Createddate, 
t.TicketsCnt,
a.Forecast,
case when a.Is_Anomaly=True then 200 else 0 end Is_Anomaly
FROM mytest_db.tickets.TicketsDaily_cal_grp_testing t
join mytest_db.tickets.tickets_cnt_daily_group_anomaly a
on t.CreatedDate=a.ts
and t.TicketGroup=a.Series
where series='Customer Service Queue 2';

-- get Feature Importance
CALL mytest_db.tickets.tickets_cnt_daily_group_anomaly!explain_feature_importance();
SELECT * FROM TABLE(RESULT_SCAN(-1))
WHERE Rank<3
ORDER BY Series, Rank;


-- Evaluate model performance:
CALL mytest_db.tickets.tickets_cnt_daily_group_anomaly!show_evaluation_metrics();
SELECT * FROM TABLE(RESULT_SCAN(-1))
WHERE Series='Customer Service Queue 1'