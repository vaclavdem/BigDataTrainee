WITH city_category_hours AS (
    SELECT 
        c.city,
        ca.name AS category,
        SUM(f.rental_duration) AS hours_watched,
        RANK() OVER (
            PARTITION BY c.city
            ORDER BY SUM(f.rental_duration) DESC
        ) AS rnk
    FROM payment p
    JOIN customer cu ON p.customer_id = cu.customer_id
    JOIN address a ON cu.address_id = a.address_id
    JOIN city c ON a.city_id = c.city_id
    JOIN rental r ON p.rental_id = r.rental_id
    JOIN inventory i ON r.inventory_id = i.inventory_id
    JOIN film f ON i.film_id = f.film_id
    JOIN film_category fc ON f.film_id = fc.film_id
    JOIN category ca ON fc.category_id = ca.category_id
    WHERE ca.name ILIKE 'a%'
      AND c.city LIKE '%-%'
    GROUP BY c.city, ca.name
)

SELECT city, category, hours_watched
FROM city_category_hours
WHERE rnk = 1
ORDER BY city;