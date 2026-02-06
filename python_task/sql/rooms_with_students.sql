SELECT rooms.id,
       ARRAY_AGG(students.name ORDER BY students.name)
FROM rooms
LEFT JOIN students ON rooms.id = students.room
GROUP BY rooms.id
ORDER BY rooms.id;