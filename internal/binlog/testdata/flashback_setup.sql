CREATE DATABASE IF NOT EXISTS shop;
DROP TABLE IF EXISTS shop.wide;
DROP TABLE IF EXISTS shop.heap;
CREATE TABLE shop.wide (
  id BIGINT NOT NULL,
  bucket INT NOT NULL,
  i_tiny TINYINT NULL,
  i_tiny_u TINYINT UNSIGNED NULL,
  i_small SMALLINT NULL,
  i_int INT NULL,
  i_int_u INT UNSIGNED NULL,
  i_big BIGINT NULL,
  i_big_u BIGINT UNSIGNED NULL,
  qty INT NULL,
  price DECIMAL(18,4) NULL,
  created_at DATETIME(6) NULL,
  updated_at TIMESTAMP(6) NULL,
  note VARCHAR(255) NULL,
  raw BLOB NULL,
  bits VARBINARY(16) NULL,
  payload JSON NULL,
  color ENUM('red','blue','green') NULL,
  flags SET('a','b','c') NULL,
  PRIMARY KEY (id, bucket)
);
CREATE TABLE shop.heap (
  id INT NULL,
  note VARCHAR(64) NULL
);
DROP TABLE IF EXISTS shop.jdoc;
DROP TABLE IF EXISTS shop.jheap;
DROP TABLE IF EXISTS shop.chars;
DROP TABLE IF EXISTS shop.gen;
DROP TABLE IF EXISTS shop.es;
DROP TABLE IF EXISTS shop.cj;
DROP TABLE IF EXISTS shop.yearnum;
CREATE TABLE shop.jdoc (
  id INT NOT NULL,
  doc JSON NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.jheap (
  doc JSON NULL
);
CREATE TABLE shop.chars (
  id INT NOT NULL,
  l1 VARCHAR(64) CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL,
  u16 VARCHAR(64) CHARACTER SET utf16 COLLATE utf16_general_ci NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.gen (
  id INT NOT NULL,
  base INT NULL,
  virt INT AS (base + 1) VIRTUAL,
  stor INT AS (base * 2) STORED,
  PRIMARY KEY (id)
);
CREATE TABLE shop.es (
  id INT NOT NULL,
  e ENUM('plain', 'café', 'Ã©') CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL,
  s SET('x', 'thé') CHARACTER SET latin1 COLLATE latin1_swedish_ci NULL,
  eu ENUM('ok', '中文') CHARACTER SET gbk COLLATE gbk_chinese_ci NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.cj (
  id INT NOT NULL,
  j JSON NULL,
  PRIMARY KEY (id)
);
CREATE TABLE shop.yearnum (
  yr YEAR NOT NULL,
  yr2 YEAR NOT NULL,
  region SMALLINT NOT NULL,
  id INT UNSIGNED NOT NULL,
  note VARCHAR(20) NULL,
  n DECIMAL(6,1) NULL,
  si INT NULL,
  ui BIGINT UNSIGNED NULL,
  sb BIGINT NULL,
  tu TINYINT UNSIGNED NULL,
  mi MEDIUMINT UNSIGNED NULL,
  ss SMALLINT NULL,
  PRIMARY KEY (yr, yr2, region, id)
);
SET time_zone = '+00:00';
INSERT INTO shop.wide VALUES
  (1, 1, -128, 255, -32768, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, NULL, -123456789012.3400, '2026-10-06 14:05:01.123456', '2026-10-06 14:05:01.500000', 'it''s "bad" \\ 雪', x'DEADBEEFFF00', x'00FFFE', JSON_OBJECT('sku', 'Z', 'n', 1, 'ok', true), 'blue', 'a,c'),
  (2, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0.0001, '2026-10-06 14:00:02.000000', '2026-10-06 14:00:02.000000', 'beta', NULL, '', JSON_OBJECT('sku', 'B'), 'red', ''),
  (3, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, 1, 1, 1, 1, 1, 1, 1, 1, 9, 19.9900, '2026-10-06 15:00:00.000001', '2026-10-06 15:00:00.000001', 'keep', x'01', x'02', JSON_ARRAY(1, 2), 'green', 'b');
INSERT INTO shop.heap VALUES (1, 'keep'), (2, 'gone'), (3, NULL);
INSERT INTO shop.jdoc (id, doc) VALUES
  (1, JSON_OBJECT(
    'dec', 9.99,
    'wide', CAST(9.99 AS DECIMAL(11,2)),
    'ten', 10.0,
    'n', 10,
    'dbl', 10.0E0,
    'z', CAST(-0.0E0 AS JSON),
    'when', CAST('2020-01-02 03:04:05.500000' AS DATETIME(6)),
    'nest', JSON_ARRAY(JSON_OBJECT('p', 1.50, 'ok', true, 'miss', NULL), 's')
  )),
  (2, CAST('null' AS JSON)),
  (3, NULL);
INSERT INTO shop.jheap (doc) VALUES
  (JSON_OBJECT('n', 1, 'd', 10.0, 'z', CAST(-0.0E0 AS JSON))),
  (JSON_OBJECT('n', 9, 'd', 1.0));
INSERT INTO shop.chars (id, l1, u16) VALUES
  (1, _latin1 0xE9, _utf16 0x00410042),
  (2, _latin1 0xC3A9, NULL),
  (3, _latin1 X'', _utf16 X''),
  (4, _latin1 0xC2A335, _utf16 0x0041);
INSERT INTO shop.gen (id, base) VALUES (1, 10), (2, 20), (3, NULL);
INSERT INTO shop.es (id, e, s, eu) VALUES
  (1, 'café', 'x,thé', '中文'),
  (2, 'Ã©', 'thé', 'ok'),
  (3, 'plain', '', 'ok');
INSERT INTO shop.cj (id, j) VALUES
  (1, JSON_OBJECT('a', 1)),
  (2, JSON_OBJECT('dec', 9.99, 'z', CAST(-0.0E0 AS JSON)));
INSERT INTO shop.yearnum VALUES
  (2026, 1999, 1, 4000000000, 'original', -12.5, -5, 18446744073709551615, -9223372036854775808, 255, 1000000, -32768),
  (2026, 1999, 1, 7, 'small', 0.0, 0, 0, 0, 0, 0, 0);
