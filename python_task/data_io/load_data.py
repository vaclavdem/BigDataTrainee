def insert_rooms(cursor, rooms):
    """
    inserting rooms info into db

    :param cursor: cursor db
    :param rooms: list of objects of Room class
    """
    for room in rooms:
        cursor.execute(
            """
            INSERT INTO rooms (id, name)
            VALUES (%s, %s)
            ON CONFLICT (name) DO NOTHING
            """,
            (room.id, room.name)
        )


def insert_students(cursor, students):
    """
    inserting students info into db

    :param cursor: cursor db
    :param students: list of objects of Student class
    """
    for student in students:
        cursor.execute(
            """
            INSERT INTO students (id, name, birthday, sex, room)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id)
            DO UPDATE SET room = EXCLUDED.room
            """,
            (
                student.id,
                student.name,
                student.birthday,
                student.sex,
                student.room,
            )
        )



