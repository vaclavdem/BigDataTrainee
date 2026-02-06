SELECT rooms.id, AVG(AGE(CURRENT_DATE, students.birthday))
FROM rooms
JOIN students ON rooms.id = students.room
GROUP BY rooms.id
ORDER BY AVG DESC
LIMIT 5;