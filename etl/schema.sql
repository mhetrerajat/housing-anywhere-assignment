DROP TABLE IF EXISTS events;
DROP TABLE IF EXISTS device_details;
DROP TABLE IF EXISTS locations;
DROP TABLE IF EXISTS event_date;
DROP TABLE IF EXISTS users;

-- Facts table
CREATE TABLE events ( 
	event                varchar(36) NOT NULL    ,
	event_date_key       integer NOT NULL    ,
	unique_visitor_id    varchar(64) NOT NULL    ,
	ha_user_key          integer     ,
	location_key         integer NOT NULL    ,
	device_key           integer     ,
	CONSTRAINT PrimaryKey PRIMARY KEY ( event, event_date_key, unique_visitor_id ),
	FOREIGN KEY ( device_key ) REFERENCES device_details( id )  ,
	FOREIGN KEY ( location_key ) REFERENCES locations( id )  ,
	FOREIGN KEY ( ha_user_key ) REFERENCES users( id )  ,
	FOREIGN KEY ( event_date_key ) REFERENCES event_date( id )  
 );


-- Dimension Tables

CREATE TABLE device_details ( 
	id                   integer NOT NULL  PRIMARY KEY  AUTOINCREMENT   ,
	browser              varchar(128) NOT NULL    ,
	os                   varchar(64) NOT NULL    ,
	device_type          varchar(64) NOT NULL    ,
	CONSTRAINT Pk_device_details UNIQUE ( id ) 
 );



CREATE TABLE locations ( 
	id                   integer NOT NULL  PRIMARY KEY  AUTOINCREMENT   ,
	country              varchar(256) NOT NULL    ,
	continent            varchar(256) NOT NULL    ,
	official_country_name varchar(256) NOT NULL    ,
	CONSTRAINT Pk_locations UNIQUE ( id ) 
    CONSTRAINT Pk_locations_countries UNIQUE ( country ) 
 );


CREATE TABLE users ( 
	id                   integer NOT NULL  PRIMARY KEY  AUTOINCREMENT   ,
	ha_user_id           varchar(36) NOT NULL    ,
	CONSTRAINT Pk_users UNIQUE ( id )
    CONSTRAINT Pk_users_ha_user_id UNIQUE ( ha_user_id ) 
 );


CREATE TABLE event_date ( 
	id                   integer NOT NULL  PRIMARY KEY  AUTOINCREMENT   ,
	time                 datetime NOT NULL    ,
	date                 date NOT NULL    ,
	month                varchar(36) NOT NULL    ,
	is_holiday           boolean NOT NULL    ,
	year                 integer NOT NULL    ,
	quarter              varchar(36) NOT NULL    ,
	day                  varchar(36) NOT NULL    ,
	CONSTRAINT Pk_event_date UNIQUE ( id ) 
 );
