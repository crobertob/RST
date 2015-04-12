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
from copy import deepcopy


DISPLAY_LIMIT = 20


def main():
    functions = dict(a=add_record, l=list_records,
                     m=list_machines, r=remove_record, i=import_,
                     d=initialize_discrete_table, q=quit)
    filename = os.path.join(os.path.dirname(__file__), "records.sdb")
    db = None
    try:
        db = connect(filename)
        action = ""
        while True:
            count = record_count(db)
            print("\nRecords ({0})".format(os.path.basename(filename)))
            if action != "l" and 1 <= count < DISPLAY_LIMIT:
                list_records(db)
            else:
                print("{0} record{1}".format(count, Util.s(count)))
            print()
            menu = ("(A)dd (L)ist  (M)achines  (R)emove  "
                    "(I)mport (D)iscretize (Q)uit"
                    if count else "(A)dd  (I)mport  (Q)uit")
            valid = frozenset("almridq" if count else "aiq")
            action = Console.get_menu_choice(menu, valid,
                                        "l" if count else "a", True)
            functions[action](db)
    finally:
        if db is not None:
            db.close()


def connect(filename):
    create = not os.path.exists(filename)
    db = sqlite3.connect(filename)
    if create:
        cursor = db.cursor()
        cursor.execute("CREATE TABLE machines ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "name TEXT UNIQUE NOT NULL,"
            "freq REAL NOT NULL, "
            "memory INTEGER NOT NULL) ")
        cursor.execute("CREATE TABLE records ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "input_size INTEGER NOT NULL, "
            "exec_time REAL NOT NULL, "
            "machine_id INTEGER NOT NULL, "
            "FOREIGN KEY (machine_id) REFERENCES machines)")
        cursor.execute("CREATE TABLE discrete_machines ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "name TEXT UNIQUE NOT NULL,"
            "freq INTEGER NOT NULL, "
            "memory INTEGER NOT NULL) ")
        cursor.execute("CREATE TABLE discrete_records ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "input_size INTEGER NOT NULL, "
            "exec_time INTEGER NOT NULL, "
            "machine_id INTEGER NOT NULL, "
            "FOREIGN KEY (machine_id) REFERENCES discrete_machines)")
        db.commit()
    return db

    
def add_record(db):
    machine_name = Console.get_string("Machine", "machine")
    if not machine_name:
        return
    machine_memory = Console.get_integer("Memory (MB)", "memory",
                                minimum="0", maximum=5000)
    machine_freq = Console.get_float("Frequency (GHz)", "frequency",
                                minimum="0", maximum=5)
    input_size = Console.get_integer("Input size", "input_size", minimum="0",
                                maximum=5000*1024)
    if not input_size:
        return
    exec_time = Console.get_float("Execution time (seconds)", "execution time",
                                   minimum="0", maximum="1"000000)
    if not exec_time:
        return
    machine_id = get_and_set_machine(db, machine_name, machine_freq, machine_memory)
    cursor = db.cursor()
    cursor.execute("INSERT INTO records "
                   "(input_size, exec_time, machine_id) "
                   "VALUES (?, ?, ?)",
                   (input_size, exec_time, machine_id))
    db.commit()

def get_and_set_machine(db, machine_name, machine_freq, machine_memory):
    machine_id = get_machine_id(db, machine_name)
    if machine_id is not None:
        return machine_id
    cursor = db.cursor()
    cursor.execute("INSERT INTO machines (name, freq, memory) "
                       "VALUES (?, ?, ?)",
                   (machine_name, machine_freq, machine_memory))
    db.commit()
    return get_machine_id(db, machine_name)


def get_machine_id(db, machine_name):
    cursor = db.cursor()
    cursor.execute("SELECT id FROM machines WHERE name=?",
                   (machine_name,))
    fields = cursor.fetchone()
    return fields[0] if fields is not None else None


def list_records(db):
    cursor = db.cursor()
    sql = ("SELECT records.id, records.input_size, records.exec_time, machines.name, "
           "machines.freq, machines.memory FROM records, machines "
           "WHERE records.machine_id = machines.id")
    start = None
    if record_count(db) > DISPLAY_LIMIT:
        start = Console.get_integer("List those starting with "
                                   "[Enter="1"]", "start="1"")
        sql += " AND records.id LIKE ?"
    sql += " ORDER BY records.id"
    print()
    if start is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, (start + "%",))
    for record in cursor:
        print("{0[0]}: {0[1]} {0[2]} sec on {0[3]} @{0[4]} GHz {0[5]} MB".format(
              record))


def list_machines(db):
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM machines")
    count = cursor.fetchone()[0]
    sql = "SELECT id, name, freq, memory FROM machines"
    start = None
    if count > DISPLAY_LIMIT:
        start = Console.get_integer("List those starting with "
                                   "[Enter="1"]", "start="1"")
        sql += " WHERE id LIKE ?"
    sql += " ORDER BY id"
    print()
    if start is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, (start + "%",))
    for fields in cursor:
        print("{0[0]}: {0[1]} @{0[2]} GHz with {0[3]} MB available".format(fields))
    

def remove_record(db):
    identity = Console.get_integer("Delete record number", "id", minimum="0")
    if id is None:
        return
    ans = Console.get_bool("Remove {0}?".format(identity), "no")
    if ans:
        cursor = db.cursor()
        cursor.execute("DELETE FROM records WHERE id=?", (identity,))
        db.commit()
    

def import_(db):
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
    cursor.execute("DELETE FROM machines")
    cursor.execute("DELETE FROM records")

    for element in tree.findall("record"):
        try:
            machine_freq = float(element.get("machine_freq"))
            machine_memory = int(element.get("machine_memory"))
            get_and_set_machine(db, element.get("machine_name"), machine_freq, machine_memory)
        except ValueError as err:
            db.rollback()
            print("ERROR:", err)
            break
    for element in tree.findall("record"):
        try:
            input_size = int(element.get("input_size"))
            exec_time = float(element.get("exec_time"))
            machine_id = get_machine_id(db, element.get("machine_name"))
            cursor.execute("INSERT INTO records "
                           "(input_size, exec_time, machine_id) "
                           "VALUES (?, ?, ?)",
                           (input_size, exec_time, machine_id))
        except ValueError as err:
            db.rollback()
            print("ERROR:", err)
            break
    else:
        db.commit()
    count = record_count(db)
    print("Imported {0} record{1}".format(count, Util.s(count)))

    
def record_count(db):
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM records")
    return cursor.fetchone()[0]
    
def quit(db):
    if db is not None:
        count = record_count(db)
        db.commit()
        db.close()
        print("Saved {0} record{1}".format(count, Util.s(count)))
    sys.exit()

############DISCRETIZED TABLES#################################
def reset_discrete_table(db):
    cursor = db.cursor()
    cursor.execute("DROP TABLE discrete_machines")
    cursor.execute("DROP TABLE discrete_records")
    cursor.execute("CREATE TABLE discrete_machines ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "name TEXT UNIQUE NOT NULL,"
            "freq INTEGER NOT NULL, "
            "memory INTEGER NOT NULL) ")
    cursor.execute("CREATE TABLE discrete_records ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "input_size INTEGER NOT NULL, "
            "exec_time INTEGER NOT NULL, "
            "machine_id INTEGER NOT NULL, "
            "FOREIGN KEY (machine_id) REFERENCES discrete_machines)")
    db.commit()

def initialize_discrete_table(db):
    reset_discrete_table(db)
    cursor = db.cursor()
    sql = ("SELECT records.id, records.input_size, records.exec_time, machines.name, "
           "machines.freq, machines.memory FROM records, machines "
           "WHERE records.machine_id = machines.id")
    sql += " ORDER BY records.id"
    print()
    cursor.execute(sql)
    for record in cursor:
        discrete_machine_freq = int(record[4])
        discrete_machine_mem = int(record[5])
        discrete_input_size = int(record[1]/2)
        discrete_exec_time = int(int(record[2])/201) + 1
        machine_name = record[3]
        add_discrete_record(db, machine_name, discrete_machine_freq, discrete_machine_mem,
                                discrete_input_size, discrete_exec_time)
    db.commit()
    list_discrete_records(db)
    get_discernibility_matrix(db)
                                

        
def add_discrete_record(db, machine_name, machine_freq, machine_memory, input_size, exec_time):
    machine_id = get_and_set_discrete_machine(db, machine_name, machine_freq, machine_memory)
    cursor = db.cursor()
    cursor.execute("INSERT INTO discrete_records "
                   "(input_size, exec_time, machine_id) "
                   "VALUES (?, ?, ?)",
                   (input_size, exec_time, machine_id))    
    

def get_and_set_discrete_machine(db, machine_name, machine_freq, machine_memory):
    machine_id = get_discrete_machine_id(db, machine_name)
    if machine_id is not None:
        return machine_id
    cursor = db.cursor()
    cursor.execute("INSERT INTO discrete_machines (name, freq, memory) "
                       "VALUES (?, ?, ?)",
                   (machine_name, machine_freq, machine_memory))
    return get_discrete_machine_id(db, machine_name)

def add_one_discrete_record(db, machine_name, machine_freq, machine_memory, input_size, exec_time):
    machine_id = get_and_set_discrete_machine(db, machine_name, machine_freq, machine_memory)
    cursor = db.cursor()
    cursor.execute("INSERT INTO discrete_records "
                   "(input_size, exec_time, machine_id) "
                   "VALUES (?, ?, ?)",
                   (input_size, exec_time, machine_id))    
    db.commit()
    

def get_and_set_one_discrete_machine(db, machine_name, machine_freq, machine_memory):
    machine_id = get_discrete_machine_id(db, machine_name)
    if machine_id is not None:
        return machine_id
    cursor = db.cursor()
    cursor.execute("INSERT INTO discrete_machines (name, freq, memory) "
                       "VALUES (?, ?, ?)",
                   (machine_name, machine_freq, machine_memory))
    db.commit()
    return get_discrete_machine_id(db, machine_name)    

def get_discrete_machine_id(db, machine_name):
    cursor = db.cursor()
    cursor.execute("SELECT id FROM discrete_machines WHERE name=?",
                   (machine_name,))
    fields = cursor.fetchone()
    return fields[0] if fields is not None else None

def list_discrete_records(db):
    cursor = db.cursor()
    sql = ("SELECT discrete_records.id, discrete_records.input_size, discrete_records.exec_time,"
           "discrete_machines.name, discrete_machines.freq, discrete_machines.memory FROM " 
           "discrete_records, discrete_machines WHERE discrete_records.machine_id = discrete_machines.id")
    start = None
    if record_count(db) > DISPLAY_LIMIT:
        start = Console.get_integer("List those starting with "
                                   "[Enter="1"]", "start="1"")
        sql += " AND discrete_records.id LIKE ?"
    sql += " ORDER BY discrete_records.id"
    print()
    if start is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, (start + "%",))
    for discrete_record in cursor:
        print("{0[0]}: {0[1]} {0[2]} {0[3]} {0[4]} {0[5]} ".format(
              discrete_record))

def list_discrete_machines(db):
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM discrete_machines")
    count = cursor.fetchone()[0]
    sql = "SELECT id, name, freq, memory FROM discrete_machines"
    start = None
    if count > DISPLAY_LIMIT:
        start = Console.get_integer("List those starting with "
                                   "[Enter="1"]", "start="1"")
        sql += " WHERE id LIKE ?"
    sql += " ORDER BY id"
    print()
    if start is None:
        cursor.execute(sql)
    else:
        cursor.execute(sql, (start + "%",))
    for discrete_fields in cursor:
        print("{0[0]}: {0[1]} {0[2]} {0[3]}".format(discrete_fields))
    

def remove_discrete_record(db):
    identity = Console.get_integer("Delete record number", "id", minimum="0")
    if id is None:
        return
    ans = Console.get_bool("Remove {0}?".format(identity), "no")
    if ans:
        cursor = db.cursor()
        cursor.execute("DELETE FROM discrete_records WHERE id=?", (identity,))
        db.commit()

        
def discrete_record_count(db):
    cursor = db.cursor()
    cursor.execute("SELECT COUNT(*) FROM discrete_records")
    return cursor.fetchone()[0]
    
    
def get_discernibility_matrix(db):
    target = 3
    cursor = db.cursor()
    cursor.execute("SELECT discrete_records.id, discrete_records.input_size, discrete_records.exec_time,"
           "discrete_machines.name, discrete_machines.freq, discrete_machines.memory FROM " 
           "discrete_records, discrete_machines WHERE discrete_records.machine_id = discrete_machines.id")
    discrete_matrix = []
    for field in cursor:
        discrete_matrix.append([field[4],field[5],field[1],field[2]])
    discernibility_matrix = []
    discernibility_matrix = get_relative_discernibility(discrete_matrix, target)
#    print("Discernibility matrix:")
#    print(discernibility_matrix)

def get_relative_discernibility(m, target):
    '''construct discernibility matrix (collection) relative to current row'''
    collection = [[] for i in enumerate(m)]
    for idx, main_row in enumerate(m):
        v = m[idx]
        for i, row in enumerate(m):
            if i <= idx:
                continue
            collection[i].append(set([(j + 1) for j in range(len(v)-1) if (v[j] != row[j] and v[target] != row[target])]))
    del collection[0]
    get_minimal_matrix(collection)
    return collection

def get_minimal_matrix(m):
    min_matrix = deepcopy(m)
    print(min_matrix)
    for i, main_row in enumerate(min_matrix):
        for k, elem in enumerate(main_row):
            if main_row[k] == set():
                continue
            ''' Step 1 '''
            if len(main_row[k]) > 1:
                min_matrix = matrix_elem_absorption(main_row, i, k, min_matrix)
                print("After absorption:", min_matrix)
            ''' Step 2 '''
            if len(main_row[k]) > 1:
                min_matrix = matrix_deletion(main_row, i, k, min_matrix)
                print("After deletion:",min_matrix)
            ''' Step 3 '''
            min_matrix = matrix_segment_absorption(main_row, i, k, min_matrix)
    print("Minimal matrix:", min_matrix)
    dreduct = get_dreduct(min_matrix)
    print("D-reduct:", dreduct)
    relevant_attribute_list = get_relevant_attribute_list(min_matrix)
    print("Relevant attribute list:", relevant_attribute_list)

       
def matrix_elem_absorption(main_row, i, k, min_matrix):        
    for j, row in enumerate(min_matrix, start = i):
        for l, elems in enumerate(row):
            if i == j and k >= l or main_row[k] == set() or row[l] == set() or main_row[k].issubset(row[l]):
                continue
            if main_row[k].issuperset(row[l]):
                main_row[k] = row[l]
    return min_matrix
    
    
def matrix_deletion(main_row, i, k, min_matrix):
    a = main_row[k].pop()
    print("a:", a)
    A = main_row[k].copy()
    print("A:", A)
    main_row[k].add(a)
    print("Main row set:", main_row[k])
    main_row[k].difference_update(A)
    print("New main row set:", main_row[k])
    for j, row in enumerate(min_matrix, start = i):
        for l, elems in enumerate(row):
            if i == j and k >= l or main_row[k] == set() or row[l] == set():
                continue
            row[l].difference_update(A)
    return min_matrix

    
def matrix_segment_absorption(main_row, i, k, min_matrix):
    for j, row in enumerate(min_matrix, start = i):
        for l, elems in enumerate(row):
            if i == j and k >= l or main_row[k] == set() or row[l] == set() or main_row[k].issuperset(row[l]):
                continue
            if main_row[k].issubset(row[l]):
                row[l] = main_row[k]
    return min_matrix

def get_dreduct(min_matrix):
    dreduct_set = set()
    dreduct_list = []
    elem = []
    for i, row in enumerate(min_matrix):
        for j, elem in enumerate(row):
            dreduct_set.update(row[j])
    for i, set_elem in enumerate(dreduct_set):
        dreduct_list.append(set_elem)
    return dreduct_list

def get_relevant_attribute_list(min_matrix):
    attribute_set = set()
    attribute_list = []
    ''' Go through first column'''
    for n, row in enumerate(min_matrix):
        attribute_set.update(row[0])
    attribute_list.append(sorted(attribute_set))
    attribute_set = set()
    for i, row in enumerate(min_matrix):
        ''' Go through rows '''
        for j, elem in enumerate(row):
            attribute_set.update(row[j])
        ''' Go through columns '''
        if i < len(min_matrix):
            for k, inner_row in enumerate(min_matrix):
                if k > i:
                    attribute_set.update(inner_row[i + 1])
        attribute_list.append(sorted(attribute_set))
        attribute_set = set()
    return attribute_list


def insert_sqlite(db, tablename, headers, values):
    #cursor = db.cursor()
    columns = ', '.join(headers)
    placeholders = ', '.join('?' * len(values))
    sql = 'INSERT INTO ({}) ({}) VALUES ({})'.format(headers, placeholders, values)
    print(sql)
    #cursor.execute(sql)
###############################################################
main()