SET time_zone = '+00:00';
START TRANSACTION;
DELETE FROM shop.wide WHERE id = 1 AND bucket = 1;
DELETE FROM shop.heap WHERE id = 2;
COMMIT;
START TRANSACTION;
UPDATE shop.wide SET note = 'changed', qty = qty + 1, price = 1.5000, color = 'green', flags = 'a,b', payload = JSON_OBJECT('sku', 'Q') WHERE id IN (2, 4);
UPDATE shop.wide SET id = 30, bucket = 9 WHERE id = 3 AND bucket = 2;
UPDATE shop.heap SET note = 'x' WHERE id = 3;
COMMIT;
START TRANSACTION;
INSERT INTO shop.wide (
  id, bucket, i_tiny, i_tiny_u, i_small, i_int, i_int_u, i_big, i_big_u,
  qty, price, created_at, updated_at, note, raw, bits, payload, color, flags
) VALUES
  (10, 1, 7, 8, -9, 9, 10, 11, 12, 1, 2.5000, '2026-10-07 01:02:03.000456', '2026-10-07 01:02:03.000789', 'batch "one"', x'AA', x'BB', JSON_OBJECT('n', 10), 'red', 'c'),
  (11, 1, -1, 1, NULL, -2, 2, -3, 3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
  (12, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0.0000, '2020-01-01 00:00:00.000000', '2020-01-01 00:00:00.000000', 'batch \\ end', x'', x'', JSON_OBJECT('s', 'a\\b'), 'blue', 'a,b,c');
INSERT INTO shop.heap VALUES (4, 'new'), (5, 'it''s');
COMMIT;
START TRANSACTION;
UPDATE shop.jdoc SET doc = JSON_OBJECT('n', 1) WHERE id = 1;
UPDATE shop.jdoc SET doc = JSON_OBJECT('n', 2) WHERE id = 2;
DELETE FROM shop.jdoc WHERE id = 3;
INSERT INTO shop.jdoc (id, doc) VALUES (4, JSON_OBJECT('n', 4, 'd', 10.0));
UPDATE shop.jheap SET doc = JSON_OBJECT('n', 2) WHERE JSON_EXTRACT(doc, '$.n') = 1;
UPDATE shop.chars SET l1 = _latin1 0x61, u16 = _utf16 0x0043 WHERE id = 1;
DELETE FROM shop.chars WHERE id = 2;
INSERT INTO shop.chars (id, l1, u16) VALUES (9, _latin1 0x62, _utf16 0x0044);
UPDATE shop.gen SET base = 11 WHERE id = 1;
DELETE FROM shop.gen WHERE id = 2;
INSERT INTO shop.gen (id, base) VALUES (4, 40);
COMMIT;
