create database tvdemo DEFAULT CHARACTER SET utf8;

CREATE TABLE episodes (
	id mediumint unsigned NOT NULL,
        airdate date NOT NULL,
	airstamp datetime NOT NULL,
	airtime time default NULL,
	name varchar(128) NOT NULL,
	ep_num decimal(6,1),
	runtime decimal(5,1),
	season tinyint unsigned not null,
	ep_type varchar(32) NOT NULL,
        show_id mediumint unsigned NOT NULL,
	src_url varchar(255) NOT NULL,
        PRIMARY KEY (id),
	key airdate (airdate)
      )
ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE shows (
        id mediumint unsigned NOT NULL,
        name varchar(128) NOT NULL,
        show_type varchar(32) NOT NULL,
	language varchar(32) NOT NULL,
	runtime decimal(5,1),
	premiered date,
	ended date,
	genres varchar(128) NOT NULL,
	src_url varchar(255) NOT NULL,
	PRIMARY KEY (id)
	)
ENGINE=InnoDB DEFAULT CHARSET=utf8;

