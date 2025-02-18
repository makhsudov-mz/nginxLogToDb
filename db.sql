/*
SQLyog Ultimate v11.11 (64 bit)
MySQL - 5.5.5-10.3.13-MariaDB : Database - test_test
*********************************************************************
*/

/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
CREATE DATABASE /*!32312 IF NOT EXISTS*/`test_test` /*!40100 DEFAULT CHARACTER SET utf8 */;

USE `test_test`;

/*Table structure for table `infotable` */

DROP TABLE IF EXISTS `infotable`;

CREATE TABLE `infotable` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `ip` varchar(15) DEFAULT NULL,
  `unk1` text DEFAULT NULL,
  `unk2` text DEFAULT NULL,
  `date_t` datetime DEFAULT NULL,
  `zapros` text DEFAULT NULL,
  `res` int(4) DEFAULT NULL,
  `raz` int(11) DEFAULT NULL,
  `unk3` text DEFAULT NULL,
  `ustroystvo` text DEFAULT NULL,
  `unk4` text DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;
