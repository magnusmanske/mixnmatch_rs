/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*M!100616 SET @OLD_NOTE_VERBOSITY=@@NOTE_VERBOSITY, NOTE_VERBOSITY=0 */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `ai` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `property` int(11) NOT NULL,
  `value` varchar(128) NOT NULL,
  `value_type` enum('Qid','string','time','plaintext') NOT NULL,
  `rationale` varchar(128) NOT NULL,
  `note` varchar(64) NOT NULL,
  `model` enum('Claude_Sonnet_3_5') NOT NULL,
  `usable` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `aliases` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `language` varchar(8) NOT NULL DEFAULT '',
  `label` varchar(128) NOT NULL DEFAULT '',
  `added_by_user` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `language` (`language`,`label`,`entry_id`),
  KEY `entry_id` (`entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `auth_control_gender` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `property` int(11) NOT NULL,
  `total` int(11) DEFAULT NULL,
  `male` int(11) DEFAULT NULL,
  `female` int(11) DEFAULT NULL,
  `other` int(11) DEFAULT NULL,
  `unknown` int(11) DEFAULT NULL,
  `p_male` double(22,0) GENERATED ALWAYS AS (`male` / `total`) STORED,
  `p_female` double(22,0) GENERATED ALWAYS AS (`female` / `total`) STORED,
  `p_unknown` double(22,0) GENERATED ALWAYS AS (`unknown` / `total`) STORED,
  `number_of_records` int(11) DEFAULT NULL,
  `p_completed` double(22,0) GENERATED ALWAYS AS (`total` / `number_of_records`) STORED,
  `query_failed` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `property` (`property`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `autoscrape` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog` int(11) NOT NULL,
  `json` mediumtext NOT NULL,
  `last_run_min` int(11) DEFAULT NULL,
  `last_run_urls` int(11) DEFAULT NULL,
  `status` varchar(255) NOT NULL DEFAULT '',
  `owner` int(11) NOT NULL DEFAULT 2,
  `notes` mediumtext NOT NULL,
  `do_auto_update` tinyint(1) NOT NULL DEFAULT 0,
  `last_update` varchar(14) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `catalog` (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci COMMENT='select ext_name,regexp_replace(ext_name,"^(.+), (.+)$","\\\\2 \\\\1") from entry\n#;update entry set ext_name=regexp_replace(ext_name,"^(.+), (.+)$","\\\\2 \\\\1") \nwhere catalog=1352 AND ext_name like "%, %"';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `aux_candidates` (
  `aux_name` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `aux_p` int(10) unsigned NOT NULL,
  `cnt` int(11) unsigned NOT NULL,
  `matched` int(11) unsigned NOT NULL,
  `entry_ids` varchar(255) NOT NULL DEFAULT '',
  KEY `aux_p` (`aux_p`,`cnt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `aux_matched` (
  `entry_id` int(11) unsigned NOT NULL,
  `property` int(10) unsigned NOT NULL,
  `prop_value` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `q` int(10) unsigned NOT NULL,
  `catalog` int(10) unsigned NOT NULL,
  PRIMARY KEY (`entry_id`),
  KEY `property` (`property`,`prop_value`),
  KEY `catalog` (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `auxiliary` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) unsigned NOT NULL,
  `aux_p` int(10) unsigned NOT NULL,
  `aux_name` varchar(255) NOT NULL DEFAULT '',
  `in_wikidata` tinyint(1) NOT NULL DEFAULT 0,
  `entry_is_matched` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id_3` (`entry_id`,`aux_p`,`aux_name`),
  KEY `entry_id_2` (`entry_id`,`in_wikidata`),
  KEY `entry_id` (`entry_id`,`aux_p`),
  KEY `aux_p` (`aux_p`,`aux_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `auxiliary_broken` (
  `id` int(11) unsigned NOT NULL DEFAULT 0,
  `entry_id` int(11) unsigned NOT NULL,
  `aux_p` int(10) unsigned NOT NULL,
  `aux_name` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `in_wikidata` tinyint(1) NOT NULL DEFAULT 0,
  `entry_is_matched` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `aux_p` (`aux_p`,`aux_name`),
  KEY `aux_name` (`aux_name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `auxiliary_fix` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `aux_p` int(10) unsigned NOT NULL,
  `aux_name` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `label` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `aux_p` (`aux_p`,`label`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `auxiliary_props` (
  `p` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `type` varchar(16) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  PRIMARY KEY (`p`),
  KEY `type` (`type`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) DEFAULT NULL,
  `url` varchar(128) DEFAULT NULL,
  `desc` varchar(255) NOT NULL,
  `type` varchar(64) NOT NULL DEFAULT '',
  `wd_prop` int(11) DEFAULT NULL,
  `wd_qual` int(11) DEFAULT NULL,
  `search_wp` varchar(16) NOT NULL DEFAULT 'en',
  `active` tinyint(1) NOT NULL DEFAULT 1,
  `owner` int(11) NOT NULL DEFAULT 2,
  `note` varchar(255) NOT NULL DEFAULT '',
  `source_item` int(11) DEFAULT NULL,
  `has_person_date` varchar(16) NOT NULL DEFAULT '',
  `taxon_run` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `wd_prop` (`wd_prop`,`wd_qual`,`active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `catalog_default_statement` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog` int(11) unsigned NOT NULL,
  `property` int(10) unsigned NOT NULL,
  `value` varchar(32) NOT NULL DEFAULT '',
  `type` int(11) unsigned NOT NULL DEFAULT 0 COMMENT '0 unless specific type required (QID)',
  `property_type` set('item','text') NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `catalog` (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci COMMENT='This table stores statements that apply to entries of a catalog, either all or those of a specified type.';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `cersei` (
  `cersei_scraper_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog_id` int(11) NOT NULL,
  `last_sync` varchar(14) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL,
  PRIMARY KEY (`cersei_scraper_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `code_fragments` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `function` varchar(16) NOT NULL DEFAULT '',
  `catalog` int(11) NOT NULL,
  `php` mediumtext NOT NULL,
  `json` mediumtext NOT NULL,
  `is_active` tinyint(4) NOT NULL DEFAULT 1,
  `note` mediumtext DEFAULT NULL,
  `last_run` timestamp NULL DEFAULT NULL,
  `lua` mediumtext DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `function` (`function`,`catalog`,`is_active`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_aux` (
  `aux_p` int(10) unsigned NOT NULL,
  `aux_name` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL,
  `entry_ids` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL,
  `cnt` bigint(21) NOT NULL,
  `unmatched` decimal(23,0) DEFAULT NULL,
  `fully_matched_qs` mediumtext CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names` (
  `name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci PAGE_CHECKSUM=1;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_artwork` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0,
  `entry_ids` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `name` (`name`),
  KEY `cnt` (`cnt`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_birth_year` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0,
  `entry_ids` mediumtext NOT NULL,
  `dates` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`,`dates`),
  KEY `cnt` (`cnt`,`name`,`dates`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_birth_year_tmp` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0,
  `entry_ids` mediumtext NOT NULL,
  `dates` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`,`dates`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_dates` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0,
  `entry_ids` varchar(255) NOT NULL DEFAULT '',
  `dates` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_human` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `cnt` bigint(21) NOT NULL DEFAULT 0,
  `entry_ids` varchar(255) NOT NULL DEFAULT '',
  `dates` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `name` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `common_names_taxon` (
  `name` varchar(128) NOT NULL DEFAULT '',
  `cnt` int(11) NOT NULL DEFAULT 0,
  `total` int(11) NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `description_aux` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `rx` varchar(255) NOT NULL,
  `property` int(11) NOT NULL,
  `value` varchar(32) NOT NULL,
  `type_constraint` varchar(16) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `rx` (`rx`,`property`,`type_constraint`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `descriptions` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `language` varchar(8) NOT NULL DEFAULT '',
  `label` varchar(128) NOT NULL DEFAULT '',
  `added_by_user` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `language` (`language`,`entry_id`),
  KEY `entry_id` (`entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `entry` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog` int(10) unsigned NOT NULL,
  `ext_id` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL DEFAULT '',
  `ext_url` varchar(255) NOT NULL DEFAULT '',
  `ext_name` varchar(128) NOT NULL DEFAULT '',
  `ext_desc` varchar(255) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL DEFAULT '',
  `q` int(11) DEFAULT NULL,
  `user` int(10) unsigned DEFAULT NULL,
  `timestamp` varchar(16) DEFAULT NULL,
  `random` float DEFAULT NULL,
  `type` varchar(16) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `ext_id` (`ext_id`,`catalog`),
  UNIQUE KEY `catalog` (`catalog`,`ext_id`,`q`),
  KEY `q` (`q`,`user`),
  KEY `timestamp` (`timestamp`),
  KEY `random` (`random`),
  KEY `ext_name` (`ext_name`),
  KEY `type` (`type`),
  KEY `random_2` (`random`,`user`,`q`,`catalog`),
  KEY `catalog_user` (`catalog`,`user`),
  KEY `user` (`user`),
  KEY `ext_name_2` (`ext_name`,`type`),
  KEY `ext_name_catalog` (`ext_name`,`catalog`),
  KEY `catalog_2` (`catalog`,`type`),
  KEY `type_2` (`type`,`q`,`catalog`),
  KEY `catalog_only` (`catalog`),
  KEY `catalog_q_random` (`catalog`,`q`,`random`),
  FULLTEXT KEY `ft_ext_name` (`ext_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `entry2artist` (
  `entry_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `born` int(11) DEFAULT NULL,
  `died` int(11) DEFAULT NULL,
  `q` int(11) DEFAULT NULL,
  PRIMARY KEY (`entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `entry2given_name` (
  `entry_id` int(11) unsigned NOT NULL,
  `random` float NOT NULL DEFAULT 0,
  `given_name_id` int(11) NOT NULL,
  PRIMARY KEY (`entry_id`),
  KEY `random` (`random`),
  KEY `given_name_id` (`given_name_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `entry_creation` (
  `entry_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `timestamp` varchar(20) NOT NULL DEFAULT '',
  PRIMARY KEY (`entry_id`),
  CONSTRAINT `entry_creation_ibfk_1` FOREIGN KEY (`entry_id`) REFERENCES `entry` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `fast_external` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry` int(11) NOT NULL,
  `external_id` varchar(64) NOT NULL DEFAULT '',
  `type` varchar(16) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `type` (`type`),
  KEY `entry` (`entry`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci COMMENT='I have no recollection of how this got here…';
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `frs` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) DEFAULT NULL,
  `rsid` varchar(64) DEFAULT NULL,
  `formal_name` varchar(255) DEFAULT NULL,
  `type` varchar(64) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `rsid` (`rsid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `given_name` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(32) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin NOT NULL DEFAULT '',
  `gender` enum('unknown','male','female','ambiguous') NOT NULL DEFAULT 'unknown',
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`name`),
  KEY `gender` (`gender`),
  KEY `id` (`id`,`gender`),
  KEY `gender_2` (`gender`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `human_dates_tmp` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `years` varchar(9) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  `name` varchar(127) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `has_fully_matched` tinyint(11) NOT NULL DEFAULT 0,
  `has_auto_matched` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `years` (`years`,`name`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `import_file` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `uuid` varchar(36) NOT NULL DEFAULT '',
  `user` int(11) NOT NULL,
  `timestamp` varchar(14) NOT NULL DEFAULT '',
  `type` varchar(8) NOT NULL DEFAULT 'tsv',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uuid` (`uuid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `inaturalist` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL,
  `parent_taxon` int(11) NOT NULL DEFAULT 0,
  `common_name` varchar(255) NOT NULL DEFAULT '',
  `q` int(11) NOT NULL DEFAULT 0,
  `rank` varchar(32) NOT NULL DEFAULT '',
  `extinct` tinyint(1) NOT NULL DEFAULT 0,
  `alternate_names` mediumtext NOT NULL,
  PRIMARY KEY (`id`),
  KEY `name` (`name`),
  KEY `q` (`q`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `isni` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `isni` varchar(32) NOT NULL DEFAULT '',
  `name` tinytext NOT NULL,
  `alt_names` tinytext NOT NULL,
  `locality` tinytext NOT NULL,
  `admin_area_level_1_short` tinytext NOT NULL,
  `post_code` tinytext NOT NULL,
  `country_code` tinytext NOT NULL,
  `urls` tinytext NOT NULL,
  `q` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `isni` (`isni`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `issues` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `type` enum('WD_DUPLICATE','MISMATCH','ITEM_DELETED','MISMATCH_DATES','MULTIPLE') NOT NULL DEFAULT 'WD_DUPLICATE',
  `json` mediumtext NOT NULL,
  `status` set('OPEN','DONE','INACTIVE_CATALOG','RESOLVED_ON_WIKIDATA','JAN01') NOT NULL DEFAULT 'OPEN',
  `user_id` int(11) DEFAULT NULL,
  `resolved_ts` varchar(14) DEFAULT NULL,
  `random` float NOT NULL,
  `catalog` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id` (`entry_id`,`type`),
  KEY `status` (`status`,`random`),
  KEY `status_2` (`status`,`catalog`),
  KEY `status_3` (`status`,`random`,`catalog`),
  KEY `type` (`type`,`status`,`random`,`catalog`),
  KEY `status_4` (`status`,`type`,`random`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `job_sizes` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `action` varchar(128) NOT NULL,
  `size` enum('tiny','small','medium','large','ginormous') NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `action` (`action`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `jobs` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `action` varchar(48) NOT NULL DEFAULT '',
  `catalog` int(11) NOT NULL,
  `json` mediumtext DEFAULT NULL,
  `depends_on` int(11) DEFAULT NULL,
  `status` set('TODO','RUNNING','DONE','FAILED','PAUSED','LOW_PRIORITY','HIGH_PRIORITY','BLOCKED','DEACTIVATED') NOT NULL DEFAULT 'TODO',
  `last_ts` varchar(14) NOT NULL DEFAULT '',
  `note` varchar(255) DEFAULT NULL,
  `repeat_after_sec` int(11) DEFAULT NULL,
  `next_ts` varchar(14) NOT NULL DEFAULT '',
  `user_id` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `action` (`action`,`catalog`),
  KEY `status` (`status`,`depends_on`,`last_ts`),
  KEY `status_2` (`status`,`next_ts`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `journals` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `JournalTitle` varchar(255) DEFAULT NULL,
  `MedAbbr` varchar(255) DEFAULT NULL,
  `ISSN_print` varchar(255) DEFAULT NULL,
  `ISSN_online` varchar(255) DEFAULT NULL,
  `IsoAbbr` varchar(255) DEFAULT NULL,
  `NlmId` varchar(255) DEFAULT NULL,
  `q` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `NlmId` (`NlmId`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `kv` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `kv_key` varchar(255) NOT NULL DEFAULT '',
  `kv_value` mediumtext NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `kv_key` (`kv_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `kv_catalog` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog_id` int(11) unsigned NOT NULL,
  `kv_key` varchar(128) NOT NULL DEFAULT '',
  `kv_value` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  UNIQUE KEY `catalog_id_2` (`catalog_id`,`kv_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `kv_entry` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) unsigned NOT NULL,
  `kv_key` varchar(128) NOT NULL DEFAULT '',
  `kv_value` varchar(255) NOT NULL DEFAULT '',
  `done` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id` (`entry_id`,`kv_key`,`kv_value`),
  KEY `done` (`done`),
  KEY `kv_key` (`kv_key`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `location` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `lat` double NOT NULL,
  `lon` double NOT NULL,
  `precision` double DEFAULT NULL COMMENT 'arc-seconds, like on Wikidata',
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry` (`entry_id`),
  KEY `lat` (`lat`,`lon`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `action` varchar(16) NOT NULL DEFAULT '',
  `entry_id` int(10) unsigned NOT NULL,
  `user` int(10) unsigned NOT NULL,
  `timestamp` varchar(16) NOT NULL DEFAULT '',
  `q` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `timestamp` (`timestamp`),
  KEY `entry` (`entry_id`),
  KEY `user` (`user`,`timestamp`),
  KEY `q` (`q`),
  KEY `entry_2` (`entry_id`,`q`),
  KEY `action` (`action`,`entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `log_aution_houses` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `property` int(11) NOT NULL,
  `ext_id` varchar(64) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `property` (`property`,`ext_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `mnm_relation` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) NOT NULL,
  `property` int(11) NOT NULL,
  `target_entry_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id_2` (`entry_id`,`property`,`target_entry_id`),
  KEY `entry_id` (`entry_id`),
  KEY `target_entry_id` (`target_entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `multi_match` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) unsigned NOT NULL,
  `catalog` int(11) NOT NULL,
  `candidates` tinytext NOT NULL,
  `candidate_count` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id` (`entry_id`),
  KEY `catalog` (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `overview` (
  `catalog` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `total` int(11) NOT NULL DEFAULT 0,
  `noq` int(11) NOT NULL DEFAULT 0,
  `autoq` int(11) NOT NULL DEFAULT 0,
  `na` int(11) NOT NULL DEFAULT 0,
  `manual` int(11) NOT NULL DEFAULT 0,
  `nowd` int(11) NOT NULL DEFAULT 0,
  `multi_match` int(11) NOT NULL DEFAULT 0,
  `types` mediumtext NOT NULL,
  PRIMARY KEY (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `person_dates` (
  `entry_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `born` varchar(10) NOT NULL DEFAULT '',
  `died` varchar(10) NOT NULL DEFAULT '',
  `in_wikidata` tinyint(4) NOT NULL DEFAULT 0,
  `is_matched` tinyint(5) NOT NULL DEFAULT 0,
  `year_born` varchar(5) GENERATED ALWAYS AS (regexp_replace(`born`,'^(-*\\d+).*$','\\1')) STORED,
  `year_died` varchar(5) GENERATED ALWAYS AS (regexp_replace(`died`,'^(-*\\d+).*$','\\1')) STORED,
  PRIMARY KEY (`entry_id`),
  KEY `born` (`born`,`died`),
  KEY `in_wikidata` (`in_wikidata`,`is_matched`),
  KEY `year_born` (`year_born`,`year_died`),
  KEY `is_matched` (`is_matched`,`year_born`,`year_died`),
  KEY `year_born_entry_id` (`entry_id`,`year_born`),
  KEY `yead_died_entry_id` (`entry_id`,`year_died`),
  CONSTRAINT `person_dates_ibfk_1` FOREIGN KEY (`entry_id`) REFERENCES `entry` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `property_cache` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `prop_group` int(11) NOT NULL,
  `property` int(11) NOT NULL,
  `item` int(11) NOT NULL,
  `label` varchar(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`id`),
  KEY `property` (`property`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `props_todo` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `property_num` int(11) NOT NULL,
  `property_name` varchar(255) NOT NULL DEFAULT '',
  `default_type` varchar(16) NOT NULL DEFAULT '',
  `status` enum('NO_CATALOG','HAS_CATALOG','NOT_SUITABLE','DIFFICULT','BROKEN') NOT NULL DEFAULT 'NO_CATALOG',
  `note` varchar(255) NOT NULL DEFAULT '',
  `user_id` int(11) NOT NULL DEFAULT 0,
  `items_using` int(11) DEFAULT NULL,
  `number_of_records` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `property_num` (`property_num`),
  KEY `status` (`status`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `q_p31` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `q` int(11) NOT NULL,
  `p31` int(11) NOT NULL,
  `timestamp` timestamp NOT NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  KEY `q` (`q`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `q_things` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `q` int(11) NOT NULL,
  `label` varchar(255) NOT NULL,
  `section` varchar(16) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `name` (`label`,`section`),
  UNIQUE KEY `q` (`q`,`section`),
  KEY `section` (`section`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `reference_fixer` (
  `q` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `done` tinyint(4) NOT NULL DEFAULT 0,
  PRIMARY KEY (`q`),
  KEY `done` (`done`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `statement_text` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `entry_id` int(11) unsigned NOT NULL,
  `property` int(11) unsigned NOT NULL,
  `text` varchar(255) NOT NULL DEFAULT '',
  `in_wikidata` tinyint(1) unsigned NOT NULL,
  `entry_is_matched` tinyint(1) unsigned NOT NULL,
  `q` int(11) unsigned DEFAULT NULL,
  `user_id` int(10) unsigned DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entry_id` (`entry_id`,`property`,`text`),
  KEY `idx_q` (`q`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `tmp_dates` (
  `entry_id` int(11) unsigned NOT NULL,
  `desc` varchar(250) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `tmp_nbd` (
  `ext_name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT '',
  `catalog` int(10) unsigned NOT NULL,
  `matched` int(1) DEFAULT NULL,
  `nbd` varchar(140) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `entry_id` int(11) unsigned NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `tmp_p214` (
  `entry_id` int(11) unsigned NOT NULL,
  `prop_value` varchar(32) CHARACTER SET ascii COLLATE ascii_general_ci NOT NULL DEFAULT '',
  `q` bigint(11) DEFAULT NULL,
  `catalog` int(10) unsigned NOT NULL,
  PRIMARY KEY (`entry_id`,`prop_value`),
  KEY `P214` (`prop_value`),
  KEY `catalog` (`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `top_missing_groups` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT '',
  `catalogs` varchar(255) NOT NULL DEFAULT '',
  `user` int(11) NOT NULL,
  `timestamp` varchar(14) NOT NULL DEFAULT '',
  `current` int(11) NOT NULL,
  `based_on` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `current` (`current`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `update_info` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `catalog` int(11) NOT NULL,
  `json` mediumtext NOT NULL,
  `note` varchar(64) NOT NULL DEFAULT '',
  `user_id` int(11) NOT NULL,
  `is_current` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `catalog` (`catalog`,`is_current`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `name` varchar(255) NOT NULL DEFAULT '',
  `last_block_check` int(11) NOT NULL DEFAULT 1517914619,
  `is_catalog_admin` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  UNIQUE KEY `tusc_username` (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_aliases` AS SELECT
 1 AS `language`,
  1 AS `label`,
  1 AS `entry_id`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `catalog`,
  1 AS `user`,
  1 AS `q` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_artist_artwork` AS SELECT
 1 AS `artist_entry_id`,
  1 AS `artwork_entry_id`,
  1 AS `artist_name`,
  1 AS `artwork_name`,
  1 AS `artist_desc`,
  1 AS `artwork_desc`,
  1 AS `artist_year_born`,
  1 AS `artist_year_died`,
  1 AS `artwork_year_inception` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_artwork` AS SELECT
 1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type`,
  1 AS `creator`,
  1 AS `inception` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_aux` AS SELECT
 1 AS `aux_p`,
  1 AS `aux_name`,
  1 AS `in_wikidata`,
  1 AS `aux_id`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_catalogs2sandra` AS SELECT
 1 AS `id`,
  1 AS `mnm_url`,
  1 AS `name`,
  1 AS `desc`,
  1 AS `wd_prop`,
  1 AS `wd_qual`,
  1 AS `type`,
  1 AS `source_q` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_catalogs_for_quick_compare` AS SELECT
 1 AS `id`,
  1 AS `name`,
  1 AS `url`,
  1 AS `desc`,
  1 AS `type`,
  1 AS `wd_prop`,
  1 AS `wd_qual`,
  1 AS `search_wp`,
  1 AS `active`,
  1 AS `owner`,
  1 AS `note`,
  1 AS `source_item`,
  1 AS `has_person_date`,
  1 AS `taxon_run`,
  1 AS `autoq` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_catalogs_with_possible_dates` AS SELECT
 1 AS `id`,
  1 AS `name`,
  1 AS `url`,
  1 AS `desc`,
  1 AS `type`,
  1 AS `wd_prop`,
  1 AS `wd_qual`,
  1 AS `search_wp`,
  1 AS `active`,
  1 AS `owner`,
  1 AS `note`,
  1 AS `source_item`,
  1 AS `has_person_date`,
  1 AS `taxon_run` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_catalogs_with_possible_dates_2` AS SELECT
 1 AS `id`,
  1 AS `name`,
  1 AS `url`,
  1 AS `desc`,
  1 AS `type`,
  1 AS `wd_prop`,
  1 AS `wd_qual`,
  1 AS `search_wp`,
  1 AS `active`,
  1 AS `owner`,
  1 AS `note`,
  1 AS `source_item`,
  1 AS `has_person_date`,
  1 AS `taxon_run` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_common_names` AS SELECT
 1 AS `name`,
  1 AS `cnt` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_dates` AS SELECT
 1 AS `born`,
  1 AS `died`,
  1 AS `year_born`,
  1 AS `year_died`,
  1 AS `entry_id`,
  1 AS `ext_id`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `catalog`,
  1 AS `user`,
  1 AS `q`,
  1 AS `in_wikidata`,
  1 AS `is_matched` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_issues_duplicate_items` AS SELECT
 1 AS `issue_id`,
  1 AS `entry_id`,
  1 AS `property`,
  1 AS `property_value`,
  1 AS `duplicate_items`,
  1 AS `label`,
  1 AS `description`,
  1 AS `matched_to_item` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_issues_mismatched_items` AS SELECT
 1 AS `issue_id`,
  1 AS `entry_id`,
  1 AS `property`,
  1 AS `property_value`,
  1 AS `mismatched_items`,
  1 AS `label`,
  1 AS `description`,
  1 AS `matched_to_item` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_issues_time_mismatch` AS SELECT
 1 AS `issue_id`,
  1 AS `entry_id`,
  1 AS `time_mismatch` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_kv` AS SELECT
 1 AS `kv_id`,
  1 AS `kv_key`,
  1 AS `kv_value`,
  1 AS `done`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_kv_entry` AS SELECT
 1 AS `kv_key`,
  1 AS `kv_value`,
  1 AS `kv_done`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_location` AS SELECT
 1 AS `lat`,
  1 AS `lon`,
  1 AS `precision`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_mnm_relation` AS SELECT
 1 AS `entry_id`,
  1 AS `property`,
  1 AS `target_entry_id`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_object_creator` AS SELECT
 1 AS `object_catalog`,
  1 AS `creator_catalog`,
  1 AS `object_title`,
  1 AS `object_q`,
  1 AS `object_user`,
  1 AS `object_entry_id`,
  1 AS `creator_entry_id`,
  1 AS `creator_q`,
  1 AS `search_query` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_object_creator_aux` AS SELECT
 1 AS `object_catalog`,
  1 AS `creator_catalog`,
  1 AS `object_title`,
  1 AS `object_q`,
  1 AS `object_user`,
  1 AS `object_entry_id`,
  1 AS `creator_entry_id`,
  1 AS `creator_q`,
  1 AS `search_query` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_overview` AS SELECT
 1 AS `catalog`,
  1 AS `total`,
  1 AS `noq`,
  1 AS `autoq`,
  1 AS `na`,
  1 AS `manual`,
  1 AS `nowd`,
  1 AS `multi_match` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_p214` AS SELECT
 1 AS `entry_id`,
  1 AS `P214`,
  1 AS `q` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_potential_missing_wd_catalog_matches` AS SELECT
 1 AS `property_num`,
  1 AS `property_name`,
  1 AS `id`,
  1 AS `name`,
  1 AS `url`,
  1 AS `desc`,
  1 AS `type`,
  1 AS `wd_prop`,
  1 AS `wd_qual`,
  1 AS `search_wp`,
  1 AS `active`,
  1 AS `owner`,
  1 AS `note`,
  1 AS `source_item`,
  1 AS `has_person_date`,
  1 AS `taxon_run` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_related_catalogs` AS SELECT
 1 AS `from_id`,
  1 AS `to_id`,
  1 AS `from_name`,
  1 AS `to_name` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_statement_text` AS SELECT
 1 AS `st_id`,
  1 AS `property`,
  1 AS `text`,
  1 AS `in_wikidata`,
  1 AS `entry_is_matched`,
  1 AS `st_q`,
  1 AS `st_user_id`,
  1 AS `id`,
  1 AS `catalog`,
  1 AS `ext_id`,
  1 AS `ext_url`,
  1 AS `ext_name`,
  1 AS `ext_desc`,
  1 AS `q`,
  1 AS `user`,
  1 AS `timestamp`,
  1 AS `random`,
  1 AS `type` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_unmatched_creators_with_matched_work` AS SELECT
 1 AS `creator_entry_id`,
  1 AS `creator_name`,
  1 AS `work_q` */;
SET character_set_client = @saved_cs_client;
SET @saved_cs_client     = @@character_set_client;
SET character_set_client = utf8mb4;
/*!50001 CREATE VIEW `vw_wd_matches` AS SELECT
 1 AS `status`,
  1 AS `cnt` */;
SET character_set_client = @saved_cs_client;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `wd_matches` (
  `entry_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `status` enum('UNKNOWN','SAME','DIFFERENT','WD_MISSING','MNM_MISSING','N/A','PARANOIA','MULTIPLE') NOT NULL DEFAULT 'UNKNOWN',
  `timestamp` varchar(16) NOT NULL DEFAULT '',
  `catalog` int(11) NOT NULL DEFAULT 0,
  PRIMARY KEY (`entry_id`),
  KEY `status` (`status`),
  KEY `catalog` (`catalog`),
  KEY `status_2` (`status`,`catalog`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb3 COLLATE=utf8mb3_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `wikidata_log` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `q` int(11) NOT NULL,
  `tool` varchar(64) NOT NULL,
  `message` mediumtext NOT NULL,
  `created` datetime NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `q` (`q`,`tool`,`message`(200)),
  KEY `tool` (`tool`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `yb_tmp` (
  `year_born` varchar(5) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci DEFAULT NULL,
  `catalog` int(10) unsigned NOT NULL,
  `ext_name` varchar(128) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=latin1 COLLATE=latin1_swedish_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!50001 DROP VIEW IF EXISTS `vw_aliases`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_aliases` AS select `aliases`.`language` AS `language`,`aliases`.`label` AS `label`,`entry`.`id` AS `entry_id`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`catalog` AS `catalog`,`entry`.`user` AS `user`,`entry`.`q` AS `q` from (`entry` join `aliases`) where `aliases`.`entry_id` = `entry`.`id` and `entry`.`catalog` in (select `catalog`.`id` from `catalog` where `catalog`.`active` = 1) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_artist_artwork`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_artist_artwork` AS select `artist`.`id` AS `artist_entry_id`,`artwork`.`id` AS `artwork_entry_id`,`artist`.`ext_name` AS `artist_name`,`artwork`.`ext_name` AS `artwork_name`,`artist`.`ext_desc` AS `artist_desc`,`artwork`.`ext_desc` AS `artwork_desc`,(select `person_dates`.`year_born` from `person_dates` where `artist`.`id` = `person_dates`.`entry_id` limit 1) AS `artist_year_born`,(select `person_dates`.`year_died` from `person_dates` where `artist`.`id` = `person_dates`.`entry_id` limit 1) AS `artist_year_died`,(select regexp_replace(`auxiliary`.`aux_name`,'^[+-]*(d+).*$','\\1') from `auxiliary` where `artwork`.`id` = `auxiliary`.`entry_id` and `mnm_relation`.`property` = 571 limit 1) AS `artwork_year_inception` from ((`entry` `artist` join `entry` `artwork`) join `mnm_relation`) where `mnm_relation`.`property` = 170 and `mnm_relation`.`entry_id` = `artwork`.`id` and `mnm_relation`.`target_entry_id` = `artist`.`id` and (`artwork`.`user` is null or `artwork`.`user` = 0) and (`artist`.`user` is null or `artist`.`user` = 0) and `artist`.`ext_name`  not like '%anonymous%' and `artist`.`ext_name`  not like '%unknown%' and `artist`.`ext_name`  not like '%various%' and !(`artist`.`ext_name` regexp '^\\S+$') and `artist`.`type` = 'Q5' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_artwork`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_artwork` AS select `entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type`,(select `auxiliary`.`aux_name` from `auxiliary` where `auxiliary`.`entry_id` = `entry`.`id` and `auxiliary`.`aux_p` = 170 limit 1) AS `creator`,(select `auxiliary`.`aux_name` from `auxiliary` where `auxiliary`.`entry_id` = `entry`.`id` and `auxiliary`.`aux_p` = 571 limit 1) AS `inception` from `entry` where `entry`.`type` = 'Q838948' and `entry`.`id` in (select `auxiliary`.`entry_id` from `auxiliary` where `auxiliary`.`aux_p` = 170) and `entry`.`id` in (select `auxiliary`.`entry_id` from `auxiliary` where `auxiliary`.`aux_p` = 571) and (`entry`.`q` is null or `entry`.`user` = 0) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_aux`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_aux` AS select `auxiliary`.`aux_p` AS `aux_p`,`auxiliary`.`aux_name` AS `aux_name`,`auxiliary`.`in_wikidata` AS `in_wikidata`,`auxiliary`.`id` AS `aux_id`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`auxiliary` join `entry`) where `auxiliary`.`entry_id` = `entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_catalogs2sandra`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_catalogs2sandra` AS select `catalog`.`id` AS `id`,concat('https://tools.wmflabs.org/mix-n-match/#/catalog/',`catalog`.`id`) AS `mnm_url`,`catalog`.`name` AS `name`,`catalog`.`desc` AS `desc`,`catalog`.`wd_prop` AS `wd_prop`,`catalog`.`wd_qual` AS `wd_qual`,`catalog`.`type` AS `type`,concat('Q',`catalog`.`source_item`) AS `source_q` from `catalog` where `catalog`.`active` = 1 order by `catalog`.`type`,`catalog`.`name` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_catalogs_for_quick_compare`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_catalogs_for_quick_compare` AS select `catalog`.`id` AS `id`,`catalog`.`name` AS `name`,`catalog`.`url` AS `url`,`catalog`.`desc` AS `desc`,`catalog`.`type` AS `type`,`catalog`.`wd_prop` AS `wd_prop`,`catalog`.`wd_qual` AS `wd_qual`,`catalog`.`search_wp` AS `search_wp`,`catalog`.`active` AS `active`,`catalog`.`owner` AS `owner`,`catalog`.`note` AS `note`,`catalog`.`source_item` AS `source_item`,`catalog`.`has_person_date` AS `has_person_date`,`catalog`.`taxon_run` AS `taxon_run`,`overview`.`autoq` AS `autoq` from (`catalog` join `overview`) where `overview`.`catalog` = `catalog`.`id` and `catalog`.`id` in (select distinct `kv_catalog`.`catalog_id` from `kv_catalog` where `kv_catalog`.`kv_key` in ('image_pattern','has_locations')) order by `catalog`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_catalogs_with_possible_dates`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_catalogs_with_possible_dates` AS select `catalog`.`id` AS `id`,`catalog`.`name` AS `name`,`catalog`.`url` AS `url`,`catalog`.`desc` AS `desc`,`catalog`.`type` AS `type`,`catalog`.`wd_prop` AS `wd_prop`,`catalog`.`wd_qual` AS `wd_qual`,`catalog`.`search_wp` AS `search_wp`,`catalog`.`active` AS `active`,`catalog`.`owner` AS `owner`,`catalog`.`note` AS `note`,`catalog`.`source_item` AS `source_item`,`catalog`.`has_person_date` AS `has_person_date`,`catalog`.`taxon_run` AS `taxon_run` from `catalog` where `catalog`.`has_person_date` = '' and `catalog`.`active` = 1 and !(`catalog`.`id` in (select `overview`.`catalog` from `overview` where `overview`.`noq` = 0)) and exists(select 1 from `entry` where `entry`.`catalog` = `catalog`.`id` and `entry`.`type` = 'Q5' and `entry`.`ext_desc` like '%1%' limit 1) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_catalogs_with_possible_dates_2`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_catalogs_with_possible_dates_2` AS select `catalog`.`id` AS `id`,`catalog`.`name` AS `name`,`catalog`.`url` AS `url`,`catalog`.`desc` AS `desc`,`catalog`.`type` AS `type`,`catalog`.`wd_prop` AS `wd_prop`,`catalog`.`wd_qual` AS `wd_qual`,`catalog`.`search_wp` AS `search_wp`,`catalog`.`active` AS `active`,`catalog`.`owner` AS `owner`,`catalog`.`note` AS `note`,`catalog`.`source_item` AS `source_item`,`catalog`.`has_person_date` AS `has_person_date`,`catalog`.`taxon_run` AS `taxon_run` from `catalog` where `catalog`.`has_person_date` = '' and `catalog`.`active` = 1 and exists(select 1 from (`entry` join `person_dates`) where `person_dates`.`entry_id` = `entry`.`id` and `entry`.`catalog` = `catalog`.`id` and `entry`.`type` = 'Q5' limit 1) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_common_names`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_common_names` AS select `entry`.`ext_name` AS `name`,count(distinct `entry`.`catalog`) AS `cnt` from `entry` where `entry`.`q` is null and `entry`.`catalog` in (select `catalog`.`id` from `catalog` where `catalog`.`active` = 1) group by `entry`.`ext_name` having `cnt` in (3,4,5,6) order by count(distinct `entry`.`catalog`) desc,`entry`.`ext_name` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_dates`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_dates` AS select `person_dates`.`born` AS `born`,`person_dates`.`died` AS `died`,`person_dates`.`year_born` AS `year_born`,`person_dates`.`year_died` AS `year_died`,`entry`.`id` AS `entry_id`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`catalog` AS `catalog`,`entry`.`user` AS `user`,`entry`.`q` AS `q`,`person_dates`.`in_wikidata` AS `in_wikidata`,`person_dates`.`is_matched` AS `is_matched` from (`entry` join `person_dates`) where `person_dates`.`entry_id` = `entry`.`id` and `entry`.`catalog` in (select `catalog`.`id` from `catalog` where `catalog`.`active` = 1) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_issues_duplicate_items`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_issues_duplicate_items` AS select `issues`.`id` AS `issue_id`,`entry`.`id` AS `entry_id`,concat('P',`catalog`.`wd_prop`) AS `property`,`entry`.`ext_id` AS `property_value`,`issues`.`json` AS `duplicate_items`,`entry`.`ext_name` AS `label`,`entry`.`ext_desc` AS `description`,concat('Q',`entry`.`q`) AS `matched_to_item` from ((`entry` join `issues`) join `catalog`) where `issues`.`entry_id` = `entry`.`id` and `entry`.`catalog` = `catalog`.`id` and `catalog`.`wd_prop` is not null and `catalog`.`wd_qual` is null and `issues`.`status` = 'OPEN' and `issues`.`type` = 'WD_DUPLICATE' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_issues_mismatched_items`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_issues_mismatched_items` AS select `issues`.`id` AS `issue_id`,`entry`.`id` AS `entry_id`,concat('P',`catalog`.`wd_prop`) AS `property`,`entry`.`ext_id` AS `property_value`,`issues`.`json` AS `mismatched_items`,`entry`.`ext_name` AS `label`,`entry`.`ext_desc` AS `description`,concat('Q',`entry`.`q`) AS `matched_to_item` from ((`entry` join `issues`) join `catalog`) where `issues`.`entry_id` = `entry`.`id` and `entry`.`catalog` = `catalog`.`id` and `catalog`.`wd_prop` is not null and `catalog`.`wd_qual` is null and `issues`.`status` = 'OPEN' and `issues`.`type` = 'MISMATCH' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_issues_time_mismatch`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_issues_time_mismatch` AS select `issues`.`id` AS `issue_id`,`issues`.`entry_id` AS `entry_id`,`issues`.`json` AS `time_mismatch` from `issues` where `issues`.`status` = 'OPEN' and `issues`.`type` = 'MISMATCH_DATES' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_kv`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_kv` AS select `kv_entry`.`id` AS `kv_id`,`kv_entry`.`kv_key` AS `kv_key`,`kv_entry`.`kv_value` AS `kv_value`,`kv_entry`.`done` AS `done`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`kv_entry` join `entry`) where `kv_entry`.`entry_id` = `entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_kv_entry`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_kv_entry` AS select `kv_entry`.`kv_key` AS `kv_key`,`kv_entry`.`kv_value` AS `kv_value`,`kv_entry`.`done` AS `kv_done`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`kv_entry` join `entry`) where `entry`.`id` = `kv_entry`.`entry_id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_location`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_location` AS select `location`.`lat` AS `lat`,`location`.`lon` AS `lon`,`location`.`precision` AS `precision`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`entry` join `location`) where `location`.`entry_id` = `entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_mnm_relation`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_mnm_relation` AS select `mnm_relation`.`entry_id` AS `entry_id`,`mnm_relation`.`property` AS `property`,`mnm_relation`.`target_entry_id` AS `target_entry_id`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`mnm_relation` join `entry`) where `mnm_relation`.`entry_id` = `entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_object_creator`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_object_creator` AS select `obj`.`catalog` AS `object_catalog`,`creator`.`catalog` AS `creator_catalog`,`obj`.`ext_name` AS `object_title`,`obj`.`q` AS `object_q`,`obj`.`user` AS `object_user`,`obj`.`id` AS `object_entry_id`,`creator`.`id` AS `creator_entry_id`,concat('Q',`creator`.`q`) AS `creator_q`,concat('"',`obj`.`ext_name`,'" haswbstatement:P',`rel`.`property`,'=Q',`creator`.`q`) AS `search_query` from ((`entry` `obj` join `mnm_relation` `rel`) join `entry` `creator`) where `rel`.`entry_id` = `obj`.`id` and `rel`.`target_entry_id` = `creator`.`id` and `rel`.`property` in (50,170) and `creator`.`user` > 0 and `creator`.`q` > 0 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_object_creator_aux`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_object_creator_aux` AS select `obj`.`catalog` AS `object_catalog`,0 AS `creator_catalog`,`obj`.`ext_name` AS `object_title`,`obj`.`q` AS `object_q`,`obj`.`user` AS `object_user`,`obj`.`id` AS `object_entry_id`,0 AS `creator_entry_id`,`aux`.`aux_name` AS `creator_q`,concat('"',`obj`.`ext_name`,'" haswbstatement:P',`aux`.`aux_p`,'=',`aux`.`aux_name`) AS `search_query` from (`entry` `obj` join `auxiliary` `aux`) where `obj`.`id` = `aux`.`entry_id` and `aux`.`aux_p` in (50,170) and `aux`.`aux_name` regexp '^Q\\d+$' */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_overview`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_overview` AS select `entry`.`catalog` AS `catalog`,count(0) AS `total`,sum(case when `entry`.`q` is null then 1 else 0 end) AS `noq`,sum(case when `entry`.`user` = 0 then 1 else 0 end) AS `autoq`,sum(case when `entry`.`q` = 0 then 1 else 0 end) AS `na`,sum(case when `entry`.`q` > 0 and `entry`.`user` > 0 then 1 else 0 end) AS `manual`,sum(case when `entry`.`q` = -1 then 1 else 0 end) AS `nowd`,(select count(0) from `multi_match` where `multi_match`.`catalog` = `entry`.`catalog`) AS `multi_match` from `entry` group by `entry`.`catalog` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_p214`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_p214` AS select `vw_aux`.`id` AS `entry_id`,`vw_aux`.`aux_name` AS `P214`,`vw_aux`.`q` AS `q` from `vw_aux` where `vw_aux`.`aux_p` = 214 union all select `entry`.`id` AS `id`,`entry`.`ext_id` AS `ext_id`,`entry`.`q` AS `q` from (`entry` join `catalog`) where `entry`.`catalog` = `catalog`.`id` and `catalog`.`wd_prop` = 214 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_potential_missing_wd_catalog_matches`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_potential_missing_wd_catalog_matches` AS select `props_todo`.`property_num` AS `property_num`,`props_todo`.`property_name` AS `property_name`,`catalog`.`id` AS `id`,`catalog`.`name` AS `name`,`catalog`.`url` AS `url`,`catalog`.`desc` AS `desc`,`catalog`.`type` AS `type`,`catalog`.`wd_prop` AS `wd_prop`,`catalog`.`wd_qual` AS `wd_qual`,`catalog`.`search_wp` AS `search_wp`,`catalog`.`active` AS `active`,`catalog`.`owner` AS `owner`,`catalog`.`note` AS `note`,`catalog`.`source_item` AS `source_item`,`catalog`.`has_person_date` AS `has_person_date`,`catalog`.`taxon_run` AS `taxon_run` from (`props_todo` join `catalog`) where `props_todo`.`status` = 'NO_CATALOG' and regexp_replace(`props_todo`.`property_name`,' *ID$','') = regexp_replace(`catalog`.`name`,' *ID$','') and `catalog`.`active` = 1 and `catalog`.`wd_prop` is null */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_related_catalogs`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_related_catalogs` AS select distinct `e1`.`catalog` AS `from_id`,`e2`.`catalog` AS `to_id`,`c1`.`name` AS `from_name`,`c2`.`name` AS `to_name` from ((((`entry` `e1` join `entry` `e2`) join `mnm_relation`) join `catalog` `c1`) join `catalog` `c2`) where `e1`.`id` = `mnm_relation`.`entry_id` and `e2`.`id` = `mnm_relation`.`target_entry_id` and `e1`.`catalog` = `c1`.`id` and `e2`.`catalog` = `c2`.`id` and `c1`.`active` = 1 and `c2`.`active` = 1 */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_statement_text`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_statement_text` AS select `statement_text`.`id` AS `st_id`,`statement_text`.`property` AS `property`,`statement_text`.`text` AS `text`,`statement_text`.`in_wikidata` AS `in_wikidata`,`statement_text`.`entry_is_matched` AS `entry_is_matched`,`statement_text`.`q` AS `st_q`,`statement_text`.`user_id` AS `st_user_id`,`entry`.`id` AS `id`,`entry`.`catalog` AS `catalog`,`entry`.`ext_id` AS `ext_id`,`entry`.`ext_url` AS `ext_url`,`entry`.`ext_name` AS `ext_name`,`entry`.`ext_desc` AS `ext_desc`,`entry`.`q` AS `q`,`entry`.`user` AS `user`,`entry`.`timestamp` AS `timestamp`,`entry`.`random` AS `random`,`entry`.`type` AS `type` from (`statement_text` join `entry`) where `statement_text`.`entry_id` = `entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_unmatched_creators_with_matched_work`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb4 */;
/*!50001 SET character_set_results     = utf8mb4 */;
/*!50001 SET collation_connection      = utf8mb4_unicode_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_unmatched_creators_with_matched_work` AS select `creator_entry`.`id` AS `creator_entry_id`,`creator_entry`.`ext_name` AS `creator_name`,group_concat(concat('Q',`work_entry`.`q`) separator ',') AS `work_q` from ((`entry` `work_entry` join `mnm_relation`) join `entry` `creator_entry`) where `mnm_relation`.`target_entry_id` = `creator_entry`.`id` and `mnm_relation`.`property` in (50,170,175) and `work_entry`.`id` = `mnm_relation`.`entry_id` and (`creator_entry`.`q` is null or `creator_entry`.`user` = 0) and `work_entry`.`user` > 0 and `work_entry`.`q` is not null and `work_entry`.`q` > 0 and `creator_entry`.`type` = 'Q5' and !(`creator_entry`.`ext_name` regexp '(unknown|anonymous|GmBH)') group by `creator_entry`.`id` */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!50001 DROP VIEW IF EXISTS `vw_wd_matches`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = utf8mb3 */;
/*!50001 SET character_set_results     = utf8mb3 */;
/*!50001 SET collation_connection      = utf8mb3_general_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 SQL SECURITY DEFINER */
/*!50001 VIEW `vw_wd_matches` AS select `wd_matches`.`status` AS `status`,count(0) AS `cnt` from `wd_matches` group by `wd_matches`.`status` order by count(0) desc */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*M!100616 SET NOTE_VERBOSITY=@OLD_NOTE_VERBOSITY */;

CREATE TABLE IF NOT EXISTS `page` (
  `page_id` int(8) unsigned NOT NULL AUTO_INCREMENT,
  `page_namespace` int(11) NOT NULL DEFAULT 0,
  `page_title` varbinary(255) NOT NULL DEFAULT '',
  `page_is_redirect` tinyint(1) unsigned NOT NULL DEFAULT 0,
  PRIMARY KEY (`page_id`),
  UNIQUE KEY `page_name_title` (`page_namespace`,`page_title`)
);
CREATE TABLE IF NOT EXISTS `redirect` (
  `rd_from` int(8) unsigned NOT NULL DEFAULT 0,
  `rd_namespace` int(11) NOT NULL DEFAULT 0,
  `rd_title` varbinary(255) NOT NULL DEFAULT '',
  PRIMARY KEY (`rd_from`)
);
CREATE TABLE IF NOT EXISTS `linktarget` (
  `lt_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `lt_namespace` int(11) NOT NULL,
  `lt_title` varbinary(255) NOT NULL,
  PRIMARY KEY (`lt_id`),
  UNIQUE KEY `lt_namespace_title` (`lt_namespace`,`lt_title`)
);
CREATE TABLE IF NOT EXISTS `pagelinks` (
  `pl_from` int(8) unsigned NOT NULL DEFAULT 0,
  `pl_target_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`pl_from`,`pl_target_id`)
);
CREATE TABLE IF NOT EXISTS `wbt_text` (
  `wbx_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `wbx_text` varbinary(255) NOT NULL,
  PRIMARY KEY (`wbx_id`),
  UNIQUE KEY `wbx_text` (`wbx_text`)
);
CREATE TABLE IF NOT EXISTS `wbt_text_in_lang` (
  `wbxl_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `wbxl_language` varbinary(35) NOT NULL,
  `wbxl_text_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`wbxl_id`),
  UNIQUE KEY `wbxl_text_language` (`wbxl_text_id`,`wbxl_language`)
);
CREATE TABLE IF NOT EXISTS `wbt_term_in_lang` (
  `wbtl_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `wbtl_type_id` smallint(5) unsigned NOT NULL DEFAULT 1,
  `wbtl_text_in_lang_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`wbtl_id`)
);
CREATE TABLE IF NOT EXISTS `wbt_item_terms` (
  `wbit_id` bigint(20) unsigned NOT NULL AUTO_INCREMENT,
  `wbit_item_id` bigint(20) unsigned NOT NULL,
  `wbit_term_in_lang_id` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`wbit_id`)
);

