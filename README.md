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

- Run the following command to initialised the API's SQLite DB

```bash
FLASK_APP=api FLASK_ENV=development flask initdb
```

- Run the following command to run the Flask based API

```bash
FLASK_APP=api FLASK_ENV=development flask run
```

- Open up a separate terminal window, activate the virtualenv and execute following command. The `cli.py` script in the
  root directory acts as an interface to trigger various ETL stages. The following command will show all possible subcommands.

```bash
python cli.py --help
```

<details markdown="1">
<summary>Screenshot</summary>

![etl__cli_help](./docs/images/etl_cli_help.png)

</details>

NOTE: The entire ETL pipeline can be triggered at once using a single command. Please refer to [Trigger ETL Pipeline](#trigger-etl-pipeline) section for more details.

### Project Structure

The project is divided into two main components, which are `api` and `etl`. The `api` module is Flask based REST API which returns events from the DB. It supports fetching events happened between certain timeperiod. The `etl` module implements all the stages of ETL pipeline.

![readme__project_structure](./docs/images/project_structure.png)


### Trigger ETL Pipeline

![ETL](./docs/images/drake.png)

Install pre-requisites using following commands

```bash
brew install drake graphviz
```

- Trigger the pipeline

```bash
drake +...
```

It shows the order in which stages will run and their interdependency. Type `y` in the prompt to execute it

<details markdown="1">
<summary>Screenshot</summary>

![drake_etl](./docs/images/drake_etl.png)

</details>
