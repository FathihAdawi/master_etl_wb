from configparser import ConfigParser
# import pyodbc
# import pandas as pd

def db_init(filename, section):
    parser = ConfigParser()
    parser.read(filename)

    db_info = {}
    if parser.has_section(section):
        key_tuple_section = parser.items(section)
        for item in key_tuple_section:
            # item[0] is key, item[1] is value of the key
            db_info[item[0]] = item[1]

    return db_info