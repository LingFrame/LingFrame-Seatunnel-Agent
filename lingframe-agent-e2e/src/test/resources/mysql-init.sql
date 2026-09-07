-- MySQL E2E 测试初始化建表脚本
CREATE DATABASE IF NOT EXISTS test DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE test;

DROP TABLE IF EXISTS t_source;
CREATE TABLE t_source (
    id INT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    score DOUBLE DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 利用 MySQL 8.0 递归 CTE 瞬间生成 1000 行基线测试数据（耗时 < 0.05s）
INSERT INTO t_source (id, name, score)
WITH RECURSIVE seq AS (
    SELECT 1 AS id
    UNION ALL
    SELECT id + 1 FROM seq WHERE id < 1000
)
SELECT 
    id, 
    CONCAT('user_', LPAD(id, 6, '0')), 
    ROUND(50.0 + (RAND() * 50.0), 2)
FROM seq;

DROP TABLE IF EXISTS t_sink;
CREATE TABLE t_sink (
    id INT PRIMARY KEY,
    name VARCHAR(64) NOT NULL,
    score DOUBLE DEFAULT 0.0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
