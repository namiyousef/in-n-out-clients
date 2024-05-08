# in-n-out-clients

A package to universalise reading and writing from different data sources. This forms the backend for [in-n-out](https://github.com/namiyousef/in-n-out), a fastapi app for easy reading and writing from different databases.

## Currenlty Supported Datasources

| Data Source      | Support |
| ----------- | ----------- |
| Postgres      | Full read-write support including table/row conflict resolution [[article]](https://towardsdatascience.com/how-to-read-write-dataframes-from-to-sql-over-http-with-fastapi-e48ab91e6a83)       |
| Google Calendar   | Full read-create support including conflict resolution [[article]](https://namiyousef96.medium.com/writing-to-google-calendar-with-support-for-conflict-resolution-be27c1600e7e)       |

