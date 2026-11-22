\set account_id random(1, 1000)
SELECT balance FROM gengo_write_example.accounts WHERE id = :account_id;
UPDATE gengo_write_example.accounts SET balance = balance + 1
WHERE id = :account_id;
