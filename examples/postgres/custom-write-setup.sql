CREATE SCHEMA IF NOT EXISTS gengo_write_example;
CREATE TABLE IF NOT EXISTS gengo_write_example.accounts (
    id integer PRIMARY KEY,
    balance bigint NOT NULL
);
INSERT INTO gengo_write_example.accounts (id, balance)
SELECT id, 0 FROM generate_series(1, 1000) AS id
ON CONFLICT (id) DO NOTHING;
