WITH active_cust AS (
	SELECT city,
		COUNT(cu.customer_id) as active_customers
	FROM customer cu
	JOIN address a ON cu.address_id = a.address_id
	JOIN city c ON a.city_id = c.city_id
	WHERE cu.active = 1
	GROUP BY city
),
inactive_cust AS (
	SELECT city,
		COUNT(cu.customer_id) as inactive_customers
	FROM customer cu
	JOIN address a ON cu.address_id = a.address_id
	JOIN city c ON a.city_id = c.city_id
	WHERE cu.active = 0
	GROUP BY city
)

SELECT COALESCE(ac.city, ic.city) AS city,
	COALESCE(active_customers, 0)   AS active_customers,
    COALESCE(inactive_customers, 0) AS inactive_customers
FROM active_cust ac
FULL OUTER JOIN inactive_cust ic ON ac.city = ic.city
ORDER BY inactive_customers DESC