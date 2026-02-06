SELECT c.name,
    COUNT(film_id)
FROM film_category fc
JOIN category c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY c.name