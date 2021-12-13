DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS device_details;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS event_date;

-- Facts table
CREATE TABLE events (
    event VARCHAR NOT NULL,
    event_date_key INTEGER NOT NULL,
    unique_visitor_id VARCHAR NOT NULL,
    ha_user_key INTEGER,
    location_key INTEGER NOT NULL,
    device_key INTEGER NOT NULL,
    PRIMARY KEY (event, event_date_key, unique_visitor_id),
    FOREIGN KEY (device_key) REFERENCES device_details(id),
    FOREIGN KEY (location_key) REFERENCES locations(id),
    FOREIGN KEY (ha_user_key) REFERENCES users(id),
    FOREIGN KEY (event_date_key) REFERENCES event_date(id)
);

-- Dimension Tables
CREATE TABLE device_details (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    browser VARCHAR NOT NULL,
    os VARCHAR NOT NULL,
    device_type VARCHAR NOT NULL
);

CREATE TABLE locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    country VARCHAR NOT NULL,
    continent VARCHAR NOT NULL,
    official_country_name VARCHAR NOT NULL
);

CREATE TABLE users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ha_user_id VARCHAR NOT NULL
);

CREATE TABLE event_date (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time DATETIME NOT NULL,
    date DATE NOT NULL,
    month VARCHAR NOT NULL,
    is_holiday BOOLEAN NOT NULL,
    year INT NOT NULL,
    quarter VARCHAR NOT NULL,
    day VARCHAR NOT NULL
);