DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events (
    event VARCHAR NOT NULL,
    time DATETIME NOT NULL,
    unique_visitor_id VARCHAR NOT NULL,
    ha_user_id VARCHAR,
    browser VARCHAR,
    os VARCHAR,
    country_code VARCHAR,
    PRIMARY KEY (event, time, unique_visitor_id)
);