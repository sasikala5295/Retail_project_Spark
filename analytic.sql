
CREATE OR REPLACE TABLE delta.`dbfs:/Volumes/workspace/default/input/gold`
AS
SELECT product,
       SUM(revenue) as total_revenue
FROM delta.`dbfs:/Volumes/workspace/default/input/silver`
GROUP BY product
ORDER BY total_revenue DESC


