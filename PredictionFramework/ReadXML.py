#!/usr/bin/env python3
# Copyright (c) 2008 Qtrac Ltd. All rights reserved.
# This program or module is free software: you can redistribute it and/or
# modify it under the terms of the GNU General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version. It is provided for educational
# purposes and is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.

import datetime
import itertools
import os
import sqlite3
import sys
import tempfile
import xml.etree.ElementTree
import xml.parsers.expat
import xml.sax.saxutils
import Console
import Util
from locale import str


def import_db_struct_fromXML(db):
    tables = []
    headers = []
    types = []
    user_input = []
    attributes = []
    discretize = []
    scripts = []
    partition_sizes = []
    offsets = []
    foreign_keys = []
    references = []
    header_list = []
    type_list = []
    user_input_list = []
    attribute_list = []
    discretize_list = []
    script_list = []
    partition_size_list = []
    offset_list = []
    foreign_key_list = []
    reference_list = []
    #filename = Console.get_string("Import from", "filename")
    filename = "tables.xml"
    if not filename:
        return
    try:
        tree = xml.etree.ElementTree.parse(filename)
    except (EnvironmentError,
            xml.parsers.expat.ExpatError) as err:
        print("ERROR:", err)
        return
    for element in tree.findall("main_table"):
        try:
            tables.append(element.get("name"))
        except ValueError as err:
            print("ERROR:", err)
            break
        for field in element.findall("field"):
            try:
                header_list.append(field.get("name"))
                type_list.append(field.get("type"))
                user_input_list.append(field.get("user_input"))
                attribute_list.append(field.get("field_attr"))
                discretize_list.append(field.get("discretize"))
                script_list.append(field.get("script"))
                partition_size_list.append(field.get("partition_size"))
                offset_list.append(field.get("offset"))
            except ValueError as err:
                print("ERROR:", err)
                break
        for field in element.findall("foreign_key"):
            try:
                foreign_key_list.append(field.get("name"))
                reference_list.append(field.get("reference"))
            except ValueError as err:
                print("ERROR:", err)
                break
        headers.append(header_list)
        types.append(type_list)
        user_input.append(user_input_list)
        attributes.append(attribute_list)
        discretize.append(discretize_list)
        scripts.append(script_list)
        partition_sizes.append(partition_size_list)
        offsets.append(offset_list)
        foreign_keys.append(foreign_key_list)
        references.append(reference_list)
        header_list = []
        type_list = []
        user_input_list = []
        attribute_list = []
        discretize_list = []
        script_list = []
        partition_size_list = []
        offset_list = []
        foreign_key_list = []
        reference_list = []
    
    for element in tree.findall("table"):
        try:
            tables.append(element.get("name"))
        except ValueError as err:
            print("ERROR:", err)
            break
        for field in element.findall("field"):
            try:
                header_list.append(field.get("name"))
                type_list.append(field.get("type"))
                user_input_list.append(field.get("user_input"))
                attribute_list.append(field.get("field_attr"))
                discretize_list.append(field.get("discretize"))
                script_list.append(field.get("script"))
                partition_size_list.append(field.get("partition_size"))
                offset_list.append(field.get("offset"))
            except ValueError as err:
                print("ERROR:", err)
                break
        headers.append(header_list)
        types.append(type_list)
        user_input.append(user_input_list)
        attributes.append(attribute_list)
        discretize.append(discretize_list)
        scripts.append(script_list)
        partition_sizes.append(partition_size_list)
        offsets.append(offset_list)
        header_list = []
        type_list = []
        user_input_list = []
        attribute_list = []
        discretize_list = []
        script_list = []
        partition_size_list = []
        offset_list = []
    store_db_struct(db, tables, headers, types, user_input, attributes, discretize, 
                    scripts, partition_sizes, offsets, foreign_keys, references)
    create_tables(db, tables, headers, attributes, foreign_keys, references)
    list_structure_records(db)    



def create_tables(db, tables, headers, attributes, foreign_keys, references):
    '''This function is only executed the first time the DB is created'''
    cursor = db.cursor()
    for i, table in enumerate(tables):
        cursor.execute("DROP TABLE IF EXISTS " + table) 
    for i, table_headers in enumerate(headers):
        elements = ""
        for j in range(len(table_headers)):
            elements += headers[i][j] + " "
            elements += attributes[i][j] + ", "
        if(i < len(foreign_keys)):
            for k in range(len(foreign_keys[i])):            
                elements += ' FOREIGN KEY ('
                elements += foreign_keys[i][k]
                elements += ') REFERENCES '
                elements += references[i][k] + ", "
        elements = elements[:-2]
        sql = 'CREATE TABLE {} ({})'.format(tables[i], elements) 
        cursor.execute(sql)
        
        
def get_db_struct(db):
    tables = []
    headers = []
    types = []
    user_input = []
    discretize = []
    scripts = []
    partition_sizes = []
    offsets = []
    foreign_keys = []
    
    '''Get table names'''
    cursor = db.cursor()
    sql = ("SELECT name FROM _tables ORDER BY id")
    cursor.execute(sql)
    for name in cursor:
        tables.append(name[0])
    
    '''Get header values'''
    for table_id in range(len(tables)):
        headers.append(get_attrib_from_headers(db, "name", table_id))
        types.append(get_attrib_from_headers(db, "type", table_id))
        user_input.append(get_attrib_from_headers(db, "user_input", table_id))
        discretize.append(get_attrib_from_headers(db, "discretize", table_id))
        scripts.append(get_attrib_from_headers(db, "script", table_id))
        partition_sizes.append(get_attrib_from_headers(db, "partition_size", table_id))
        offsets.append(get_attrib_from_headers(db, "offset", table_id))
    
    '''Get foreign keys'''
    cursor = db.cursor()
    sql = ("SELECT _foreign_keys.name, _foreign_keys.reference, _tables.name "
           " FROM _foreign_keys, _tables WHERE _foreign_keys.table_id = _tables.id") 
    sql += " ORDER BY _foreign_keys.id"
    cursor.execute(sql)
    for key in cursor:
        foreign_keys.append([key[0], key[1], key[2]])
    
    return (tables, headers, types, user_input, discretize, scripts,
            partition_sizes, offsets, foreign_keys)


def get_attrib_from_headers(db, var, table_id):
    var_list = []
    cursor = db.cursor()
    sql = ("SELECT "+ var +" FROM _headers"
            " WHERE table_id = ") 
    sql += str(table_id + 1)
    sql += " ORDER BY id"
    cursor.execute(sql)
    
    for elem in cursor:
        var_list.append(elem[0])
    return var_list

        
def store_db_struct(db, tables, headers, types, user_input, attributes, discretize, 
                    scripts, partition_sizes, offsets, foreign_keys, references):
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS _tables")
    cursor.execute("DROP TABLE IF EXISTS _headers")
    cursor.execute("DROP TABLE IF EXISTS _foreign_keys")
    create_db_struct(db)
    
    for i, table_name in enumerate(tables):
        cursor.execute("INSERT INTO _tables "
                        "(name) "
                        "VALUES (?)",
                        (tables[i],))
    
    for i, table_headers in enumerate(headers):
        for j, elem in enumerate(table_headers):
            cursor.execute("INSERT INTO _headers "
                            "(table_id, name, type, user_input, attributes, discretize, script, partition_size, offset) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (i + 1, headers[i][j], types[i][j], user_input[i][j],
                              attributes[i][j], discretize[i][j], scripts[i][j],
                               partition_sizes[i][j], offsets[i][j]))
    for i, foreign_key in enumerate(foreign_keys):
        for j, elem in enumerate(foreign_key):
            cursor.execute("INSERT INTO _foreign_keys "
                            "(table_id, name, reference) "
                            "VALUES (?, ?, ?)",
                            (i + 1, foreign_key[j], references[i][j],))
    db.commit()

    
def connect(filename):
    create = not os.path.exists(filename)
    db = sqlite3.connect(filename)
    if create:
        create_db_struct(db)
    return db

    
def create_db_struct(db):
    cursor = db.cursor()
    cursor.execute("CREATE TABLE _tables ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "name TEXT UNIQUE NOT NULL) ")
    cursor.execute("CREATE TABLE _headers ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "table_id INTEGER NOT NULL, "
            "name TEXT NOT NULL, "
            "type TEXT NOT NULL, "
            "user_input INTEGER NOT NULL, "
            "attributes TEXT NOT NULL, "
            "discretize INTEGER NOT NULL, "
            "script TEXT, "
            "partition_size INTEGER, "
            "offset INTEGER) ")
    cursor.execute("CREATE TABLE _foreign_keys ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "table_id INTEGER NOT NULL, "
            "name TEXT NOT NULL, "
            "reference TEXT NOT NULL) ")
    db.commit()

def list_structure_records(db):
    cursor = db.cursor()
    sql = ("SELECT _tables.name, _headers.name, _headers.type, _headers.attributes"
           " FROM _tables, _headers WHERE _headers.table_id = _tables.id") 
    sql += " ORDER BY _headers.id"
    print()
    cursor.execute(sql)
    for headers in cursor:
        print("{0[0]}: {0[1]} {0[2]} {0[3]} ".format(headers))
    
    cursor = db.cursor()
    sql = ("SELECT _tables.name, _foreign_keys.name, _foreign_keys.reference "
           " FROM _tables, _foreign_keys WHERE _foreign_keys.table_id = _tables.id") 
    sql += " ORDER BY _foreign_keys.id"
    cursor.execute(sql)
    for foreign_key in cursor:
        print("{0[0]}: FOREIGN KEY {0[1]} {0[2]}".format(foreign_key))

def list_tables(db, tables, headers, foreign_keys, references):
    cursor = db.cursor()
    vars = ''
    table = ''
    refs = ''
    for i, table_headers in enumerate(headers):
        for j, elem in enumerate(table_headers):
            vars += tables[i] + "."
            vars += headers[i][j] + ", "
    vars = vars[:-2]
    for i, table_name in enumerate(tables):
        table += tables[i] + ", "
    table = table[:-2]
    for i, keys in enumerate(foreign_keys):
        for j, elem in enumerate(keys):
            refs += tables[i]+ "."
            refs += foreign_keys[i][j]+ " = "
            refs += references[i][j]
            refs += ".id and "
    refs = refs[:-5]
    sql = 'SELECT {} FROM {} WHERE {}'.format(vars, table, refs)
    cursor.execute(sql)
    print("Current database:")
    for record in cursor:
        print(record)


def import_(db, tables, headers, types, foreign_keys, references):
    value_table = []
    value_row = []
    value = []
    filename = Console.get_string("Import from", "filename")
    if not filename:
        return
    try:
        tree = xml.etree.ElementTree.parse(filename)
    except (EnvironmentError,
            xml.parsers.expat.ExpatError) as err:
        print("ERROR:", err)
        return

    cursor = db.cursor()
    for i, table_header in enumerate(headers):
        cursor.execute("DELETE FROM " + tables[i])
        for element in tree.findall(tables[i]):
            try:
                for j, name in enumerate(table_header):
                    if j > 0 and headers[i][j] not in foreign_keys[0]:
                        if types[i][j] == "int" or types[i][j] == "float":
                            instr = 'value.append(' + types[i][j] + '(element.get("' + headers[i][j] + '")))'
                        else:
                            instr = 'value.append(element.get("' + headers[i][j] + '"))'
                        eval(instr)
                value_row.append(value)
                value = []
            except ValueError as err:
                db.rollback()
                print("ERROR:", err)
                break
        value_table.append(value_row)
        value_row = []
    print("Imported data:",value_table)
    idx = 0
    if len(foreign_keys) > 0:
        for i, table_keys in enumerate(foreign_keys):
            for j, current_key in enumerate(table_keys):
                for p, table in enumerate(tables):
                    if table == references[i][j]:
                        idx = p
                for k, current_value_row in enumerate(value_table[idx]):
                    try:
                        key_value = get_and_set_foreign(db, tables[idx], headers[idx], current_key, references[i][j], current_value_row)
                        value_table[i][k].append(key_value)
                        insert_sqlite(db, tables[i], headers[i], value_table[i][k])
                    except ValueError as err:
                        db.rollback()
                        print("ERROR:", err)
                        break
    else:
        db.commit()
    #       count = record_count(db)
    #       print("Imported {0} record{1}".format(count, Util.s(count)))

        
def insert_sqlite(db, tablename, headers, values):
    cursor = db.cursor()
    columns = ', '.join(headers)
    columns = columns[4:]
    placeholders = ', '.join('?' * len(values))
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(tablename, columns, placeholders)
    cursor.execute(sql, values)

 
def get_and_set_foreign(db, table, header, foreign_key, reference, value_row):
    foreign_id = get_foreign_id(db, header[1], reference, value_row[0])
    if foreign_id is not None:
        return foreign_id
    insert_sqlite(db, table, header, value_row)
    db.commit()
    return get_foreign_id(db, header[1], reference, value_row[0])


def get_foreign_id(db, header, reference, value):
    cursor = db.cursor()
    cursor.execute("SELECT id FROM " + reference + " WHERE " + header + "=?",(value,))
    fields = cursor.fetchone()
    return fields[0] if fields is not None else None

###############################################
db = connect("test.sdb") 
import_db_struct_fromXML(db)
'''TODO: Remove attributes from get_db_struct'''
(tables, headers, types, user_input, discretize, scripts,
            partition_sizes, offsets, foreign_keys) = get_db_struct(db)
print("Tables:", tables)
print("Headers:", headers)
print("Types:", types)
print("User input:", user_input)
print("Discretize:", discretize)
print("Scripts:", scripts)
print("Partition sizes:", partition_sizes)
print("Offsets:", offsets)
print("Foreign keys:", foreign_keys)            

#import_(db, tables, headers, types, foreign_keys)
#list_tables(db, tables, headers, foreign_keys)