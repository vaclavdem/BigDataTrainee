SELECT rooms.id, AGE(MAX(students.birthday), MIN(students.birthday))
FROM rooms
JOIN students ON rooms.id = students.room
GROUP BY rooms.id
ORDER BY 2 DESC
LIMIT 5;