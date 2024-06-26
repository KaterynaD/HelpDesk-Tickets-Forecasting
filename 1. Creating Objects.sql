create or replace stage control_db.external_stages.ticket_stage 
STORAGE_INTEGRATION = aws_kd_projects_stg_int
url='s3://kd-projects/other/tickets/';

create or replace schema mytest_db.tickets;

CREATE OR REPLACE EXTERNAL TABLE mytest_db.tickets.tickets
(
TicketID	INTEGER		 as (value:c1::INTEGER)	,
Subject	VARCHAR		 as (value:c2::STRING)	,
Status	VARCHAR		 as (value:c3::STRING)	,
Priority	VARCHAR		 as (value:c4::STRING)	,
Source	VARCHAR		 as (value:c5::STRING)	,
Type	VARCHAR		 as (value:c6::STRING)	,
Agent	VARCHAR		 as (value:c7::STRING)	,
TicketGroup	VARCHAR		 as (value:c8::STRING)	,
Createdtime	TIMESTAMP_NTZ(9)		 as (value:c9::TIMESTAMP_NTZ(9))	,
DuebyTime	TIMESTAMP_NTZ(9)		 as (value:c10::TIMESTAMP_NTZ(9))	,
Resolvedtime	TIMESTAMP_NTZ(9)		 as (value:c11::TIMESTAMP_NTZ(9))	,
Closedtime	TIMESTAMP_NTZ(9)		 as (value:c12::TIMESTAMP_NTZ(9))	,
Lastupdatetime	TIMESTAMP_NTZ(9)		 as (value:c13::TIMESTAMP_NTZ(9))	,
Initialresponsetime	TIMESTAMP_NTZ(9)		 as (value:c14::TIMESTAMP_NTZ(9))	,
Timetracked	VARCHAR		 as (value:c15::STRING)	,
Firstresponsetime_in_hrs	VARCHAR		 as (value:c16::STRING)	,
Resolutiontime_in_hrs	VARCHAR		 as (value:c17::STRING)	,
Agentinteractions	INTEGER		 as (value:c18::INTEGER)	,
Customerinteractions	INTEGER		 as (value:c19::INTEGER)	,
Resolutionstatus	VARCHAR		 as (value:c20::STRING)	,
Firstresponsestatus	VARCHAR		 as (value:c21::STRING)	,
TicketTags	VARCHAR		 as (value:c22::STRING)	,
Surveyresults	VARCHAR		 as (value:c23::STRING)	,
Associationtype	VARCHAR		 as (value:c24::STRING)	,
Product	VARCHAR		 as (value:c25::STRING)	,
UWCategory	VARCHAR		 as (value:c26::STRING)	,
Everyresponsestatus	VARCHAR		 as (value:c27::STRING)	,
UWSubcategory	VARCHAR		 as (value:c28::STRING)	,
EscalationGroup	VARCHAR		 as (value:c29::STRING)	,
PrimaryRootCause	VARCHAR		 as (value:c30::STRING)	,
PrimaryRootCauseDetail	VARCHAR		 as (value:c31::STRING)	,
SecondaryRootCauseGroup	VARCHAR		 as (value:c32::STRING)	,
SecondaryRootCause	VARCHAR		 as (value:c33::STRING)	,
SecondaryRootCauseDetail	VARCHAR		 as (value:c34::STRING)	,
Category	VARCHAR		 as (value:c35::STRING)	,
SubCategory	VARCHAR		 as (value:c36::STRING)	,
Detail	VARCHAR		 as (value:c37::STRING)	,
TechniciansDecisionClassification	VARCHAR		 as (value:c38::STRING)	,
RallyNumber	VARCHAR		 as (value:c39::STRING)	,
ChangeManagementNumber	VARCHAR		 as (value:c40::STRING)	,
RequesterType	VARCHAR		 as (value:c41::STRING)	,
InquiryCategory	VARCHAR		 as (value:c42::STRING)	,
InquirySubCategory	VARCHAR		 as (value:c43::STRING)	,
Voicemail	VARCHAR		 as (value:c44::STRING)	,
ContactID	VARCHAR		 as (value:c45::STRING)	,
CallTransferredTo	VARCHAR		 as (value:c46::STRING)	,
CSEProductRelated	VARCHAR		 as (value:c47::STRING)	,
State	VARCHAR		 as (value:c48::STRING)	,
Product2	VARCHAR		 as (value:c49::STRING)	,
SentimentAnalysis	VARCHAR		 as (value:c50::STRING)	,
JIRANUMBER	VARCHAR		 as (value:c51::STRING)	,
Fullname	VARCHAR		 as (value:c52::STRING)	,
ContactID2	VARCHAR		 as (value:c53::STRING)	
)
		 WITH LOCATION = @control_db.external_stages.ticket_stage
		 FILE_FORMAT = (FORMAT_NAME='control_db.file_formats.csv_format' skip_header = 1 error_on_column_count_mismatch=false );    


         
CREATE or REPLACE MATERIALIZED VIEW mytest_db.tickets.TicketsDaily as
select 
cast(to_char(CreatedTime,'yyyy-mm-dd') as date) CreatedDate, 
count(TicketId) TicketsCnt
from  mytest_db.tickets.tickets
group by cast(to_char(CreatedTime,'yyyy-mm-dd') as date);

--explore
select 
cast(to_char(CreatedTime,'yyyy-mm-dd') as date) CreatedDate, 
count(TicketId) TicketsCnt
from  mytest_db.tickets.tickets
group by cast(to_char(CreatedTime,'yyyy-mm-dd') as date)
limit 10

--There are more tickets before 2021 when we only started working from home and less after June 2023 when company annonced closing
create or replace view mytest_db.tickets.TicketsDaily_in_working_mode as
select
CreatedDate,
TicketsCnt
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2021-01-01' and '2023-06-01'
order by CreatedDate;

--Training data
create or replace view mytest_db.tickets.TicketsDaily_training as
select
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
TicketsCnt
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2021-01-01' and '2023-01-01'
order by CreatedDate;

--Testing data
create or replace view mytest_db.tickets.TicketsDaily_testing as
select
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
TicketsCnt
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2023-01-01' and '2023-06-01'
order by CreatedDate;


--Training data with calendar - good example of using DIM_TIME with Company Holidays
create or replace view mytest_db.tickets.TicketsDaily_cal_training as
select
cast(CreatedDate as TIMESTAMP_NTZ) AS CreatedDate,
TicketsCnt,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2021-01-01' and '2022-12-31'
order by CreatedDate;


--Testing data
create or replace view mytest_db.tickets.TicketsDaily_cal_testing as
select
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
TicketsCnt,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2023-01-01' and '2023-06-01'
order by CreatedDate;


--base view with holiday flag for forecasting
create or replace view mytest_db.tickets.TicketsDaily_cal_forecasting as
select
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDaily
where CreatedDate between '2023-01-01' and '2023-06-01'
order by CreatedDate;

--Forecasting By Group
--Explore
select 
case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end TicketGroup,
count(ticketID) cnt
from mytest_db.tickets.tickets
group by case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end
order by count(*) desc;

CREATE or REPLACE MATERIALIZED VIEW mytest_db.tickets.TicketsDailyByGroup as
select 
case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end TicketGroup,
cast(to_char(CreatedTime,'yyyy-mm-dd') as date) CreatedDate, 
count(TicketId) TicketsCnt
from  mytest_db.tickets.tickets
group by case when TicketGroup in ('Customer Service Queue 1','Customer Service Queue 2') then TicketGroup else 'Other' end, cast(to_char(CreatedTime,'yyyy-mm-dd') as date);

--Explore
with data as (
select 
CreatedDate,
case when TicketGroup='Customer Service Queue 1' then TicketsCnt else 0 end Q1,
case when TicketGroup='Customer Service Queue 2' then TicketsCnt else 0 end Q2,
case when TicketGroup='Other' then TicketsCnt else 0 end Other
from mytest_db.tickets.TicketsDailyByGroup
)
select
CreatedDate,
max(Q1) Q1,
max(Q2) Q2,
max(Other) Other
from data
group by CreatedDate
order by CreatedDate;

--Training data with group calendar - good example of using DIM_TIME with Company Holidays
create or replace view mytest_db.tickets.TicketsDaily_cal_grp_training as
select
TicketGroup,
cast(CreatedDate as TIMESTAMP_NTZ) AS CreatedDate,
TicketsCnt,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDailyByGroup
where CreatedDate between '2021-01-01' and '2022-12-31'
order by TicketGroup,CreatedDate;


--Testing data
create or replace view mytest_db.tickets.TicketsDaily_cal_grp_testing as
select
TicketGroup,
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
TicketsCnt,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDailyByGroup
where CreatedDate between '2023-01-01' and '2023-04-01'
order by TicketGroup,CreatedDate;


--base view with holiday flag for forecasting
create or replace view mytest_db.tickets.TicketsDaily_cal_grp_forecasting as
select
TicketGroup,
cast(CreatedDate as TIMESTAMP_NTZ) CreatedDate,
case when CreatedDate in ( '2021-01-01','2021-01-18','2021-02-15','2021-05-31','2021-07-05','2021-09-06','2021-11-25','2021-11-26','2021-12-24','2022-01-17','2022-02-21','2022-05-30','2022-07-04','2022-09-05','2022-11-24','2022-11-25','2022-12-26','2023-01-02','2023-01-16','2023-02-20','2023-05-29','2023-07-04','2023-09-04','2023-11-23','2023-11-24','2023-12-25'
) then 1 else 0 end AS IsHoliday
from mytest_db.tickets.TicketsDailyByGroup
where CreatedDate between '2023-01-01' and '2023-04-01'
order by TicketGroup,CreatedDate;






select count(*) from mytest_db.tickets.TicketsDaily_cal_training
where IsHoliday=1;

select count(*) from mytest_db.tickets.TicketsDaily_cal_testing
where IsHoliday=1;


select * from mytest_db.tickets.TicketsDaily_training
limit 10