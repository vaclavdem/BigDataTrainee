SELECT room
FROM students
GROUP BY room
HAVING COUNT(DISTINCT sex) = 2;