DROP TABLE IF EXISTS raw_events;

CREATE TABLE raw_events ( 
	event                varchar NOT NULL    ,
	time                 datetime NOT NULL    ,
	unique_visitor_id    varchar NOT NULL    ,
	ha_user_id           varchar(36)     ,
	browser              varchar(128)     ,
	os                   varchar(64)     ,
	country_code         varchar(256)     ,
	CONSTRAINT PrimaryKey PRIMARY KEY ( event, time, unique_visitor_id )
 );
