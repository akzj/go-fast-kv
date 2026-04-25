-- name: extract_year
-- description: EXTRACT extracts year from timestamp
-- setup: CREATE TABLE events (id INT, ts TIMESTAMP);
-- setup: INSERT INTO events VALUES (1, '2024-01-15 10:30:00');

SELECT EXTRACT(YEAR FROM ts) FROM events;
-- result:
-- rows: 1
-- [0][0]: 2024
