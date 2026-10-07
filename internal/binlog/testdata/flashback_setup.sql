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
SET time_zone = '+00:00';
INSERT INTO shop.wide VALUES
  (1, 1, -128, 255, -32768, -2147483648, 4294967295, -9223372036854775808, 18446744073709551615, NULL, -123456789012.3400, '2026-10-06 14:05:01.123456', '2026-10-06 14:05:01.500000', 'it''s "bad" \\ 雪', x'DEADBEEFFF00', x'00FFFE', JSON_OBJECT('sku', 'Z', 'n', 1, 'ok', true), 'blue', 'a,c'),
  (2, 1, 0, 0, 0, 0, 0, 0, 0, 4, 0.0001, '2026-10-06 14:00:02.000000', '2026-10-06 14:00:02.000000', 'beta', NULL, '', JSON_OBJECT('sku', 'B'), 'red', ''),
  (3, 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (4, 1, 1, 1, 1, 1, 1, 1, 1, 9, 19.9900, '2026-10-06 15:00:00.000001', '2026-10-06 15:00:00.000001', 'keep', x'01', x'02', JSON_ARRAY(1, 2), 'green', 'b');
INSERT INTO shop.heap VALUES (1, 'keep'), (2, 'gone'), (3, NULL);
