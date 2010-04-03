--
-- $Id: mysql_casts.sql 665 2010-04-01 07:35:52Z fabien $
--

DROP FUNCTION IF EXISTS biginttoint8;
DROP FUNCTION IF EXISTS biginttoint4;
DROP FUNCTION IF EXISTS biginttoint2;

CREATE FUNCTION biginttoint8 RETURNS INTEGER SONAME 'mysql_casts.so';
CREATE FUNCTION biginttoint4 RETURNS INTEGER SONAME 'mysql_casts.so';
CREATE FUNCTION biginttoint2 RETURNS INTEGER SONAME 'mysql_casts.so';
