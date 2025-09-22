CREATE TABLE dbjobs.default.orders (
  id INT,
  name STRING,
  price DOUBLE,
  status STRING
);

INSERT INTO dbjobs.default.orders VALUES
  (1, 'book', 35.3, 'Shipped'),
  (2, 'magazine', 5.99, 'Processing'),
  (3, 'music', 10.99, 'Shipped'),
  (4, 'book', 8.99, 'Processing');


