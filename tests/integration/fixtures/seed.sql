-- Integration test seed schema.
-- Designed to give the MCP tools real, predictable data to analyze.
-- Loaded once per session by tests/integration/conftest.py.

CREATE TABLE users (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY idx_email (email)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE orders (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    user_id INT UNSIGNED NOT NULL,
    status VARCHAR(32) NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    -- composite "good" index, expected to be used by warmup queries
    KEY idx_user_status (user_id, status),
    -- deliberately never queried; find_unused_indexes must report this
    KEY idx_created_at_unused (created_at),
    -- duplicate of the leading column of idx_user_status; find_unused_indexes must report this as redundant
    KEY idx_user_id_dup (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE products (
    id INT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
    -- deliberately NOT indexed; full-scan warmup query relies on this
    category VARCHAR(64) NOT NULL,
    name VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Seed users: 100 rows
INSERT INTO users (email)
SELECT CONCAT('user', n, '@example.com')
FROM (
    SELECT a.n + b.n * 10 AS n
    FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
    CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
) seq
WHERE n < 100;

-- Seed orders: 1000 rows (10 per user)
INSERT INTO orders (user_id, status, total)
SELECT
    1 + (seq.n MOD 100),
    ELT(1 + (seq.n MOD 4), 'pending', 'shipped', 'delivered', 'cancelled'),
    ROUND(10 + (seq.n MOD 500) + RAND() * 10, 2)
FROM (
    SELECT a.n + b.n * 10 + c.n * 100 AS n
    FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
    CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) b
    CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
                UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) c
) seq
WHERE n < 1000;

-- Seed products: 50 rows
INSERT INTO products (category, name)
SELECT
    ELT(1 + (seq.n MOD 5), 'books', 'electronics', 'clothing', 'home', 'toys'),
    CONCAT('Product ', seq.n)
FROM (
    SELECT a.n + b.n * 10 AS n
    FROM (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4
          UNION SELECT 5 UNION SELECT 6 UNION SELECT 7 UNION SELECT 8 UNION SELECT 9) a
    CROSS JOIN (SELECT 0 n UNION SELECT 1 UNION SELECT 2 UNION SELECT 3 UNION SELECT 4) b
) seq
WHERE n < 50;
