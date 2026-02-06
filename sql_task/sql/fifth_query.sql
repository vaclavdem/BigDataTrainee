WITH actors_count AS (
	SELECT a.actor_id,
		a.first_name,
		a.last_name,
		COUNT(f.film_id) as appeared_times,
		RANK() OVER (ORDER BY COUNT(f.film_id) DESC) AS rnk
	FROM film f
	JOIN film_category fc ON f.film_id = fc.film_id
	JOIN category c ON fc.category_id = c.category_id
	JOIN film_actor fa ON f.film_id = fa.film_id
	JOIN actor a ON fa.actor_id = a.actor_id
	WHERE c.name = 'Children'
	GROUP BY a.actor_id
)

SELECT actor_id, first_name, last_name, appeared_times
FROM actors_count
WHERE rnk <= 3
ORDER BY appeared_times DESC;