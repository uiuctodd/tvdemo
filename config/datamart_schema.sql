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
        show_type int(11) DEFAULT NULL,
	language varchar(32) NOT NULL,
	runtime decimal(5,1),
	premiered date,
	ended date,
	genres varchar(128) NOT NULL,
	src_url varchar(255) NOT NULL,
	PRIMARY KEY (id)
	)
ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `show_types` (
    `id` int(11) NOT NULL AUTO_INCREMENT,
    `code` varchar(32) NOT NULL,
    PRIMARY KEY (`id`),
    UNIQUE KEY `code` (`code`)
  ) 
ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE daily_views (
      id int(11) NOT NULL AUTO_INCREMENT,
      viewdate date NOT NULL,
      episode mediumint unsigned NOT NULL,
      show_id mediumint unsigned NOT NULL,
      country char(2) not null,
      views mediumint unsigned NOT NULL,
      hours_watched mediumint unsigned NOT NULL,
      PRIMARY KEY (`id`),
      UNIQUE KEY `dayshow` (viewdate, episode, country)
  )
ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE show_viewers (
     id int(11) NOT NULL AUTO_INCREMENT,
     week_start date NOT NULL,
     show_id mediumint unsigned NOT NULL,
     country char(2) not null,
     viwer_count int(11) unsigned NOT NULL,
     MHPV decimal(4,1),                      /* mean hours watched per viewer */
     VCH int(11) unsigned NOT NULL,          /* count of viwers who watch many hours of this show*/
     VCL int(11) unsigned NOT NULL,          /* count of viwers who watch few hours of this show  */
     VCML int(11) unsigned NOT NULL,         /* count of viwers who are multilingual */
     MNSW decimal(4,1),                      /* mean number of other shows watched   */
     VSS mediumint unsigned NOT NULL,        /* number of viewrs who watched only this one show */
     VMS mediumint unsigned NOT NULL,        /* number of viewrs who watched several other shows */
     PRIMARY KEY (`id`),
     UNIQUE KEY `dayshow` (week_start, show_id, country)
  )
ENGINE=InnoDB DEFAULT CHARSET=utf8;

