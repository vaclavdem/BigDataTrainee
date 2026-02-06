SELECT a.actor_id,
	a.first_name,
	a.last_name,
	COUNT(r.rental_id) AS rented_times
FROM rental r
JOIN inventory i ON r.inventory_id = i.inventory_id
JOIN film_actor fa ON i.film_id = fa.film_id
JOIN actor a ON a.actor_id = fa.actor_id
GROUP BY a.actor_id
ORDER BY rented_times DESC
LIMIT 10