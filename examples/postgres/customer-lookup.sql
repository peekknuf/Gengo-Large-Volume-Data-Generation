\set customer_id random(1, :customer_count)
SELECT customer_id, first_name, last_name, email
FROM gengo.dim_customers
WHERE customer_id = :customer_id;
