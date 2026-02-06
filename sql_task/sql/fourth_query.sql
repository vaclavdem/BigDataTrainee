SELECT title
FROM film f
LEFT JOIN inventory i ON f.film_id = i.film_id
WHERE inventory_id IS NULL