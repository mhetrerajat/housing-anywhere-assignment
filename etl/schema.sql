DROP TABLE IF EXISTS events;

CREATE TABLE events (
    event VARCHAR NOT NULL,
    time DATETIME NOT NULL,
    unique_visitor_id VARCHAR NOT NULL,
    ha_user_id VARCHAR,
    browser VARCHAR,
    os VARCHAR,
    country VARCHAR,
    device_type VARCHAR,
    PRIMARY KEY (event, time, unique_visitor_id)
);