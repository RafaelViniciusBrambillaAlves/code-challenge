-- docker exec -it code-challenge-db-final-1 bash

psql -U postgres

\c Northwind

SET search_path TO tap_csv_part2;

\dt

\COPY (SELECT T1.order_id,
               T1.customer_id,
               T1.employee_id,
               T1.order_date,
               T1.required_date,
               T1.shipped_date,
               T1.ship_via,
               T1.freight,
               T1.ship_name,
               T1.ship_address,
               T1.ship_city,
               T1.ship_region,
               T1.ship_postal_code,
               T1.ship_country,
               T2.product_id,
               T2.unit_price,
               T2.quantity,
               T2.discount
       FROM orders AS T1
       INNER JOIN order_details AS T2 ON T1.order_id = T2.order_id) 
TO '/opt/query/result/result.csv' WITH CSV HEADER;