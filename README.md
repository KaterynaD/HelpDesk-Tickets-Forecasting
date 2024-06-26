The source files are downloaded from Freshdesk once a month and saved in an S3 bucket.

Snowflake objects:
- External table for the source data
- Materialize view for daily tickets count
- Views for training, testing and forecast data
- Timeseries model to predict number of tickets for the next 30 days by customer support queues
- Task to re-train the model monthly
- SQL stored procedure to extract prediction
- Email integration
- Python stored procedure to send nice formatted email with the prediction

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/b313b15d-fd65-4a74-b2be-91d636da1432)

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/cdc68996-3b0e-498a-bed7-049b1f51db0a)

I did some research to add external features like public holidays but it does not impact the model accuracy. The overall results are not very impressive.

Some results:

Customer Service Queue 1

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/dd49bd29-99af-4c6d-9bab-b410262caeb2)

Customer Service Queuep 2

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/556481ce-d156-4408-bd19-1a352b164ed6)

All other queues

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/78cae2aa-4708-4e89-b16a-0eee7ea1fc39)

Feature Importance

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/03f20166-407b-434b-bf42-674cd62c751f)

Metrics

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/cddd9c25-117c-4c5d-86f7-1abfac356a44)

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/c65cb718-e72d-4d45-a679-2a8f8cf019a1)

![image](https://github.com/KaterynaD/HelpDesk-Tickets-Forecasting/assets/16999229/4699bb93-e761-4a84-9025-fe5047cd39f9)




