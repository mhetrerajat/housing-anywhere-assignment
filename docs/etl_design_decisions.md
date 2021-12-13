# ETL Design Decisions

## Data Model

The data modeling process follows the Kimball Methodology.

### Grain

In fact table, each row represents an event or interaction user done with the product via browser

### Facts

There is a fact table named `events`.

### Dimensions

There are 4 dimension tables in `analytics` database. Each table answer one question related to the facts table. Following table shows the name of these dimension tables along with the how they are related to facts table.

| Table Name     | Description                    |
| -------------- | ------------------------------ |
| device_details | `Who` did the event            |
| users          | `Who` did the event            |
| locations      | `Where` did the event happened |
| event_date     | `When` event happened          |
