# HousingAnywhere Assignment

Description goes here

## Tasks

- Create an REST API "fetch events" endpoint. The "fetch events" endpoint returns the events based on `event_id` or events between time period.
- Add script to insert `events_data.json` in SQL database as raw data
- Implement an ETL to fetch these events from database periodically and store in properly structured format by preprocessing them.
- DDL queries to store the retrieved events
- Implement sample report to fetch data from analytics database

## Usage

### Setup Instructions

Project requires `Python 3.7.8`

- Create a virtualenv specifically for this project. This can be created using `pyenv` and [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) packages. This can be installed using `brew`.

```bash
brew install pyenv
brew install pyenv-virtualenv
# Go to the directory where this code is kept.
# Right way to check whether you are in correct directory or not is to ensure README.md is at the root of it
# Assuming code is kept in `/codes/` directory
cd /codes/housing-anywhere-assignment/

# Create virtualenv
pyenv virtualenv 3.7.8 housinganywhere
pyenv local housinganywhere
```

NOTE: All subsequent steps mentioned in the document assumes that virtualenv is activated

- Install requirements

```bash
pip install -r requirements.txt
```

### Project Structure

The project is divided into two main components, which are `api` and `etl`. The `api` module is Flask based REST API which returns events from the DB. It supports fetching events happened between certain timeperiod. The `etl` module implements all the stages of ETL pipeline.

![readme__project_structure](./docs/images/project_structure.png)
