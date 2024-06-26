--Assuming data are loaded in the source table by an other process

--Materializad view with aggregation
CREATE or REPLACE MATERIALIZED VIEW mytest_db.tickets.TicketsDailyByGroup as
select 
case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end TicketGroup,
cast(to_char(CreatedTime,'yyyy-mm-dd') as date) CreatedDate, 
count(TicketId) TicketsCnt
from  mytest_db.tickets.tickets
group by case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end, cast(to_char(CreatedTime,'yyyy-mm-dd') as date);

--View for training with data till the current day
create or replace view mytest_db.tickets.TicketsDaily_grp_training as
select
TicketGroup,
cast(CreatedDate as TIMESTAMP_NTZ) AS CreatedDate,
TicketsCnt
from mytest_db.tickets.TicketsDailyByGroup
where CreatedDate <= CURRENT_DATE()
order by TicketGroup,CreatedDate;

--Table for prediction
create or replace TRANSIENT TABLE MYTEST_DB.TICKETS.GRP_FORECAST_PREDICTIONS (    
	SERIES VARIANT,
	TS TIMESTAMP_NTZ(9),
	FORECAST FLOAT,
	LOWER_BOUND FLOAT,
	UPPER_BOUND FLOAT,
    CREATEDDATE TIMESTAMP_NTZ(9),
    MODEL VARCHAR(1000)
);


--Model
CREATE OR REPLACE forecast mytest_db.tickets.tickets_cnt_daily_group_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_grp_training'),
    SERIES_COLNAME => 'TicketGroup',
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt'
);


--Forecast and data review
CALL  mytest_db.tickets.tickets_cnt_daily_group_forecast!FORECAST( 
    FORECASTING_PERIODS => 30, 
    CONFIG_OBJECT => {'prediction_interval': .9}
    );
INSERT INTO mytest_db.tickets.grp_forecast_predictions  (
    SELECT
        *,
        Current_Timestamp() as CreatedDate,
        'mytest_db.tickets.tickets_cnt_daily_group_forecast' as Model
    FROM
        TABLE(RESULT_SCAN(-1))
    );


    

    SELECT series, cast(sum(forecast) as int)  as Next_Month_Tickets_Cnt 
    FROM mytest_db.tickets.grp_forecast_predictions
    WHERE CREATEDDATE=(select max(CREATEDDATE) from mytest_db.tickets.grp_forecast_predictions)
    and MODEL='mytest_db.tickets.tickets_cnt_daily_group_forecast'
    GROUP BY series
    ORDER BY series;


-- Create a task to run every month to retrain the forecasting model: 
CREATE OR REPLACE TASK mytest_db.tickets.tickets_next_month_forecast_training_task
    WAREHOUSE = compute_wh
    SCHEDULE = 'USING CRON 0 0 1 * * America/Los_Angeles' -- Runs once a month
AS
CREATE OR REPLACE forecast mytest_db.tickets.tickets_cnt_daily_group_forecast(
    INPUT_DATA => SYSTEM$REFERENCE('VIEW', 'mytest_db.tickets.TicketsDaily_grp_training'),
    SERIES_COLNAME => 'TicketGroup',
    TIMESTAMP_COLNAME => 'CreatedDate',
    TARGET_COLNAME => 'TicketsCnt'
);



-- Creates a Stored Procedure to extract predictions from our freshly trained model for teh next 30 days
CREATE OR REPLACE PROCEDURE mytest_db.tickets.extract_predictions()
RETURNS TABLE ()
LANGUAGE sql 
AS
BEGIN

CALL  mytest_db.tickets.tickets_cnt_daily_group_forecast!FORECAST( 
    FORECASTING_PERIODS => 30, 
    CONFIG_OBJECT => {'prediction_interval': .9}
    );

INSERT INTO mytest_db.tickets.grp_forecast_predictions  (
    SELECT
        *,
        Current_Timestamp() as CreatedDate,
        'mytest_db.tickets.tickets_cnt_daily_group_forecast' as Model
    FROM
        TABLE(RESULT_SCAN(-1))
    );
    
DECLARE res RESULTSET DEFAULT (
    SELECT series, cast(sum(forecast) as int)  as Next_Month_Tickets_Cnt 
    FROM mytest_db.tickets.grp_forecast_predictions
    WHERE CREATEDDATE=(select max(CREATEDDATE) from mytest_db.tickets.grp_forecast_predictions)
    and MODEL='mytest_db.tickets.tickets_cnt_daily_group_forecast'
    GROUP BY series
    ORDER BY series
    );
BEGIN 
    RETURN table(res);
END;
END;

-- Create an email integration: 
USE mytest_db.tickets;
CREATE OR REPLACE NOTIFICATION INTEGRATION my_email_int
TYPE = EMAIL
ENABLED = TRUE
ALLOWED_RECIPIENTS = ('drogaieva@gmail.com');  

-- Create Snowpark Python Stored Procedure to format email and send it
CREATE OR REPLACE PROCEDURE mytest_db.tickets.send_tickets_forecast()
RETURNS string
LANGUAGE python
runtime_version = 3.9
packages = ('snowflake-snowpark-python')
handler = 'send_email'
AS
$$
def send_email(session):
    session.call('mytest_db.tickets.extract_predictions').collect()


    html_table = session.sql("select * from table(result_scan(last_query_id(-1)))").to_pandas().to_html()
    # https://codepen.io/labnol/pen/poyPejO?editors=1000
    html_table = html_table.replace('class="dataframe"', 'style="border: solid 2px #DDEEEE; border-collapse: collapse; border-spacing: 0; font: normal 14px Roboto, sans-serif;"')
    html_table = html_table.replace('<th>', '<th style="background-color: #DDEFEF; border: solid 1px #DDEEEE; color: #336B6B; padding: 10px; text-align: left; text-shadow: 1px 1px 1px #fff;">')
    html_table = html_table.replace('<td>', '<td style="    border: solid 1px #DDEEEE; color: #333; padding: 10px; text-shadow: 1px 1px 1px #fff;">')
      
    session.call('system$send_email',
        'my_email_int',
        'drogaieva@gmail.com',
        'Email Alert: Next Month Tickets Prediction',
        html_table,
        'text/html')
$$;


call mytest_db.tickets.send_tickets_forecast();

-- Orchestrating the Tasks: 
CREATE OR REPLACE TASK mytest_db.tickets.send_tickets_forecast_task
    warehouse = compute_wh
    AFTER mytest_db.tickets.tickets_next_month_forecast_training_task
    AS CALL mytest_db.tickets.send_tickets_forecast();

-- Steps to resume and then immediately execute the task DAG:  
ALTER TASK mytest_db.tickets.send_tickets_forecast_task RESUME;
ALTER TASK mytest_db.tickets.tickets_next_month_forecast_training_task RESUME;
EXECUTE TASK mytest_db.tickets.tickets_next_month_forecast_training_task;

ALTER TASK mytest_db.tickets.tickets_next_month_forecast_training_task SUSPEND;
ALTER TASK mytest_db.tickets.send_tickets_forecast_task SUSPEND;


