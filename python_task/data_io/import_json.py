import json
from models.room import Room
from models.student import Student
from pathlib import Path

def import_rooms_info(cursor, args):
    """
    importing of rooms.json file

    :param cursor: db cursor
    :param args: args of the command line
    :return: list of objects of Room class
    """
    path = Path("input_files") / args.rooms
    with open(path, encoding="utf-8") as f:
        rooms_info = json.load(f)
        rooms = []
        for room in rooms_info:
            rooms.append(Room(room["id"], room["name"]))
    return rooms

def import_students_info(cursor, args):
    """
    importing of students.json file

    :param cursor: db cursor
    :param args: args of the command line
    :return: list of objects of Student class
    """
    path = Path("input_files") / args.students
    with open(path, encoding="utf-8") as f:
        students_info = json.load(f)
        students = []
        for student in students_info:
            students.append(Student(student["birthday"], student["id"], student["name"],\
                                 student["room"], student["sex"]))
    return students