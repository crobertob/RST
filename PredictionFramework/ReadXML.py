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
import logging
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
from builtins import len
from copy import deepcopy
from locale import str
from math import ceil





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
    decision = []
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
    decision_list = []
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
                decision_list.append(field.get("decision"))
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
        decision.append(decision_list)
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
        decision_list = []
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
                decision_list.append(field.get("decision"))
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
        decision.append(decision_list)
        header_list = []
        type_list = []
        user_input_list = []
        attribute_list = []
        discretize_list = []
        script_list = []
        partition_size_list = []
        offset_list = []
        decision_list = []
    store_db_struct(db, tables, headers, types, user_input, attributes, discretize, 
                    scripts, partition_sizes, offsets, decision, foreign_keys, references)
    discrete_tables = list_discrete_tables(tables)
    discrete_attributes = change_to_discrete_attrib(attributes)
    create_tables(db, tables, headers, attributes, foreign_keys, references)
    create_tables(db, discrete_tables, headers, discrete_attributes, foreign_keys, references)
    list_structure_records(db)    


def list_discrete_tables(tables):
    discrete_tables = ["discrete_" + s for s in tables]
    return discrete_tables


def change_to_discrete_attrib(attributes):
    new_attributes = []
    for i, attr_tables in enumerate(attributes):
        new_attributes.append([])
        for attribute in attr_tables:
            attribute = attribute.replace("REAL", "INTEGER")
            new_attributes[i].append(attribute)
    return new_attributes


def reset_tables(db, tables):
    cursor = db.cursor()
    for table in tables:
        cursor.execute("delete from " + table)
        cursor.execute("delete from sqlite_sequence where name='" + table + "'")
        cursor.execute("vacuum") 


def drop_tables(db, tables):
    cursor = db.cursor()
    for table in tables:
        cursor.execute("DROP TABLE IF EXISTS " + table)


def refresh_discrete_tables(db, tables, headers, types, foreign_keys, discretize, 
                            offsets, decision, partition_sizes, user_input):
    value_table = []
    value = []

    discrete_tables = list_discrete_tables(tables)
    reset_tables(db, discrete_tables)
    records = get_records(db, tables, headers, foreign_keys)
    
    for i in tables:
        value_table.append([])
    for record in records:
        for i, table_headers in enumerate(headers):
            try:
                for j in range(len(table_headers)):
                    if user_input[i][j] == 1:
                        if discretize[i][j] == 1:
                            if types[i][j] == "int":
                                value.append(int(ceil(int(record[i*len(table_headers) + j]/
                                                     partition_sizes[i][j]))) + offsets[i][j])
                            elif types[i][j] == "float":
                                value.append(int(ceil(float(record[i*len(table_headers) + j]/
                                                       partition_sizes[i][j]))) + offsets[i][j])
                        else:
                            value.append(record[i*len(table_headers)+j])
                value_table[i].append(value)
                value = []
            except ValueError as err:
                db.rollback()
                print("ERROR:", err)
                break
    logging.debug("Imported data: %s", value_table)
    ref_table_id = 0
    main_table_id = 0
    if len(foreign_keys) > 0:
        for i, current_key in enumerate(foreign_keys):
            for p, table in enumerate(discrete_tables):
                '''Store the index of the table that contains the reference'''
                if table == "discrete_" + foreign_keys[i][1]:
                    ref_table_id = p
                if table == "discrete_" + foreign_keys[i][2]:
                    main_table_id = p
            for k, current_value_row in enumerate(value_table[ref_table_id]):
                try:
                    key_value = get_and_set_foreign(db, discrete_tables[ref_table_id],
                                                      headers[ref_table_id], 
                                                      current_key, current_value_row)
                    value_table[main_table_id][k].append(key_value)
                    insert_sqlite(db, discrete_tables[main_table_id], headers[main_table_id], 
                                   value_table[main_table_id][k])
                except ValueError as err:
                    db.rollback()
                    print("ERROR:", err)
                    break
    else:
        db.commit()
                     

def read_record_from_scripts(headers, user_input, modules, decision):
    idx_script = 0
    idx_decision = 0
    record = []
    value = []
    
    for i, table_headers in enumerate(headers):
        for j in range(len(table_headers)):
            if user_input[i][j] == 1:
                if decision[i][j] == 0:
                    try:
                        value.append(modules[idx_script].run())
                        idx_script += 1
                        idx_decision += 1
                    except ValueError as err:
                        print("ERROR:", err)
                        break
                else:
                    decision_script = idx_script
                    decision_i = i
                    decision_j = idx_decision
                    real_j = j
                    idx_script += 1
                    idx_decision += 1
        idx_decision = 0
        record.append(value)
        value = []
    logging.debug("Record added from scripts: %s", record)
    logging.debug("i: %s, j: %s", decision_i, decision_j)
    logging.debug("Decision script: %s", modules[decision_script])
    return record, decision_script, decision_i, decision_j, real_j


def discretize_record(headers, types, discretize, offsets, decision,
                       partition_sizes, user_input, new_record):
    
    idx = 0
    discrete_record = []
    value = []
    
    for i, table_headers in enumerate(headers):
        for j in range(len(table_headers)):
            if user_input[i][j] == 1:
                if discretize[i][j] == 1:
                    if decision[i][j] == 0:
                        if types[i][j] == "int":
                            value.append(int(ceil(int(new_record[i][idx]/
                                                 partition_sizes[i][j]))) + offsets[i][j])
                            idx += 1
                        elif types[i][j] == "float":
                            value.append(int(ceil(float(new_record[i][idx]/
                                                   partition_sizes[i][j]))) + offsets[i][j])
                            idx += 1
                else:
                    value.append(new_record[i][idx])
                    idx += 1
        discrete_record.append(value)
        value = []
        idx = 0
    logging.debug("Discrete record: %s", discrete_record)
    return discrete_record
                    
def obtain_real_decision_value(record, discrete_record, decision_script, decision_i, decision_j, real_j,
                          modules, types, discretize, partition_sizes, offsets):
    decision_value = modules[decision_script].run()
    if discretize[decision_i][real_j] == 1:
        if types[decision_i][real_j] == "int" or types[decision_i][real_j] == "float":
            decision_discrete_value = int(ceil(decision_value/partition_sizes[decision_i][real_j])) + offsets[decision_i][real_j]
        else:
            decision_discrete_value = decision_value
    else:
        decision_discrete_value = decision_value
    new_record = add_decision_to_record(record, decision_value, decision_i,
                                         decision_j)
    new_discrete_record = add_decision_to_record(discrete_record, decision_discrete_value,
                                                  decision_i, decision_j)
    return new_record, new_discrete_record, decision_value


def add_decision_to_record(record, decision_value, decision_i, decision_j):
    record[decision_i].insert(decision_j, decision_value)
    return record

    
def store_record_in_DB(db, tables, headers, foreign_keys, new_record):
    
    logging.debug("Record to be added: %s", new_record)
    ref_table_id = 0
    main_table_id = 0
    if len(foreign_keys) > 0:
        for i, current_key in enumerate(foreign_keys):
            for p, table in enumerate(tables):
                '''Store the index of the table that contains the reference'''
                if table == foreign_keys[i][1]:
                    ref_table_id = p
                if table == foreign_keys[i][2]:
                    main_table_id = p
            try:
                key_value = get_and_set_foreign(db, tables[ref_table_id],
                                                  headers[ref_table_id], 
                                                  current_key, new_record[ref_table_id])
                new_record[main_table_id].append(key_value)
                insert_sqlite(db, tables[main_table_id], headers[main_table_id], 
                               new_record[main_table_id])
            except ValueError as err:
                db.rollback()
                print("ERROR:", err)
                break
    else:
        db.commit()



def store_discrete_record(db, discrete_tables, headers, foreign_keys,
                         discrete_record):
    
    logging.debug("Discrete record to be added: %s", discrete_record)
    ref_table_id = 0
    main_table_id = 0
    if len(foreign_keys) > 0:
        for i, current_key in enumerate(foreign_keys):
            for p, table in enumerate(discrete_tables):
                '''Store the index of the table that contains the reference'''
                if table == "discrete_" + foreign_keys[i][1]:
                    ref_table_id = p
                if table == "discrete_" + foreign_keys[i][2]:
                    main_table_id = p
            try:
                key_value = get_and_set_foreign(db, discrete_tables[ref_table_id],
                                                  headers[ref_table_id], 
                                                  current_key, discrete_record[ref_table_id])
                discrete_record[main_table_id].append(key_value)
                insert_sqlite(db, discrete_tables[main_table_id], headers[main_table_id], 
                               discrete_record[main_table_id])
            except ValueError as err:
                db.rollback()
                print("ERROR:", err)
                break
    else:
        db.commit()



def get_discernibility_matrix(db, tables, headers, foreign_keys, discretize,
                               decision):
    discrete_records = get_records(db, tables, headers, foreign_keys)
    discrete_matrix = []
    discrete_row = []
    decision_value = 0
    map_i = []
    map_j = []
    decision_i = 0
    decision_j = 0
    
    for k, field in enumerate(discrete_records):
        for i, table_headers in enumerate(headers):
            for j in range(len(table_headers)):
                if discretize[i][j] == 1:
                    '''Decision value should be appended last to the row'''
                    if decision[i][j] == 0:
                        discrete_row.append(field[i*len(table_headers)+j])
                        if k == 0:
                            map_i.append(i)
                            map_j.append(j)
                    elif decision[i][j] == 1:
                        '''Because of this it will only work with ONE decision value'''
                        decision_value = field[i*len(table_headers)+j]
                        target = len(table_headers) - 1
                        decision_i = i
                        decision_j = j
        discrete_row.append(decision_value)
        discrete_matrix.append(discrete_row)
        discrete_row = []
    map_i.append(decision_i)
    map_j.append(decision_j)
    logging.debug("Map i: %s", map_i)
    logging.debug("Map j: %s", map_j)
    logging.debug("Discrete matrix: %s", discrete_matrix)
    logging.debug("Target: %s", target)
    discernibility_matrix = get_relative_discernibility(discrete_matrix, target)
    return discernibility_matrix, map_i, map_j


def get_relative_discernibility(m, target):
    '''construct discernibility matrix (collection) relative to current row'''
    collection = [[] for i in enumerate(m)]
    for idx in range(len(m)):
        v = m[idx]
        for i, row in enumerate(m):
            if i <= idx:
                continue
            collection[i].append(set([(j + 1) for j in range(len(v)-1) if (v[j] != row[j] and v[target] != row[target])]))
    del collection[0]
    return collection


def get_minimal_matrix(m):
    min_matrix = deepcopy(m)
    for i, main_row in enumerate(min_matrix):
        for k in range(len(main_row)):
            if main_row[k] == set():
                continue
            ''' Step 1 '''
            if len(main_row[k]) > 1:
                min_matrix = matrix_elem_absorption(main_row, i, k, min_matrix)
                logging.debug("After absorption: %s", min_matrix)
            ''' Step 2 '''
            if len(main_row[k]) > 1:
                min_matrix = matrix_deletion(main_row, i, k, min_matrix)
                logging.debug("After deletion: %s", min_matrix)
            ''' Step 3 '''
            min_matrix = matrix_segment_absorption(main_row, i, k, min_matrix)
    logging.debug("Minimal matrix: %s", min_matrix)
    return min_matrix
       
def matrix_elem_absorption(main_row, i, k, min_matrix):        
    for j, row in enumerate(min_matrix, start = i):
        for l in range(len(row)):
            if i == j and k >= l or main_row[k] == set() or row[l] == set() or main_row[k].issubset(row[l]):
                continue
            if main_row[k].issuperset(row[l]):
                main_row[k] = row[l]
    return min_matrix
    
    
def deletion_no_empty_sets(A, i, k, min_matrix):
    for j, row in enumerate(min_matrix, start = i):
        for l in range(len(row)):
            current_value = row[l].difference(A)
            if (i == j and k >= l) or row[l] == set():
                continue
            elif current_value == set():
                return False
    return True


def delete_matrix_elements(main_row, i, k, A, min_matrix):
    logging.debug("Main row set: %s", main_row[k])
    main_row[k].difference_update(A)
    logging.debug("New main row set: %s", main_row[k])
    for j, row in enumerate(min_matrix, start = i):
        for l in range(len(row)):
            if i == j and k >= l or row[l] == set():
                continue
            row[l].difference_update(A)
    return min_matrix


def matrix_deletion(main_row, i, k, min_matrix):
    a = main_row[k].copy()
    logging.debug("a: %s", a)
    found_A = False
    if len(a) > 1:
        A = {a.pop()}
        logging.debug("A: %s", A)
        while A != set():
            found_A = deletion_no_empty_sets(A, i, k, min_matrix)
            if found_A:
                break
            else:
                A = set(a.pop())
                logging.debug("a: %s", a)
                logging.debug("A: %s", A)
        logging.debug("Final A: %s", A)
        if A != set():
            min_matrix = delete_matrix_elements(main_row, i, k, A, min_matrix)
    return min_matrix

    
def matrix_segment_absorption(main_row, i, k, min_matrix):
    for j, row in enumerate(min_matrix, start = i):
        for l in range(len(row)):
            if i == j and k >= l or main_row[k] == set() or row[l] == set() or main_row[k].issuperset(row[l]):
                continue
            if main_row[k].issubset(row[l]):
                row[l] = main_row[k]
    return min_matrix


def get_dreduct(min_matrix):
    dreduct_set = set()
    dreduct_list = []
    for row in min_matrix:
        for j in range(len(row)):
            dreduct_set.update(row[j])
    for set_elem in dreduct_set:
        dreduct_list.append(set_elem)
    logging.debug("D-reduct: %s", dreduct_list)
    return dreduct_list


def get_relevant_attribute_list(min_matrix):
    attribute_set = set()
    attribute_list = []
    ''' Go through first column'''
    for row in min_matrix:
        attribute_set.update(row[0])
    attribute_list.append(sorted(attribute_set))
    attribute_set = set()
    for i, row in enumerate(min_matrix):
        ''' Go through rows '''
        for j in range(len(row)):
            attribute_set.update(row[j])
        ''' Go through columns '''
        if i < len(min_matrix):
            for k, inner_row in enumerate(min_matrix):
                if k > i:
                    attribute_set.update(inner_row[i + 1])
        attribute_list.append(sorted(attribute_set))
        attribute_set = set()
    logging.debug("Relevant attribute list: %s", attribute_list)
    return attribute_list

                    
def get_records(db, tables, headers, foreign_keys):
    cursor = db.cursor()
    var_string = ''
    table = ''
    refs = ''
    for i, table_headers in enumerate(headers):
        for j in range(len(table_headers)):
            var_string += tables[i] + "."
            var_string += headers[i][j] + ", "
    var_string = var_string[:-2]
    for i in range(len(tables)):
        table += tables[i] + ", "
    table = table[:-2]
    if tables[0].find("discrete", 0) >= 0:
        for i in range(len(foreign_keys)):
            refs += "discrete_"
            refs += foreign_keys[i][2]+ "."
            refs += foreign_keys[i][0]+ " = "
            refs += "discrete_"
            refs += foreign_keys[i][1]
            refs += ".id and "
    else:
        for i in range(len(foreign_keys)):
            refs += foreign_keys[i][2]+ "."
            refs += foreign_keys[i][0]+ " = "
            refs += foreign_keys[i][1]
            refs += ".id and "
    refs = refs[:-5]
    sql = 'SELECT {} FROM {} WHERE {}'.format(var_string, table, refs)
    cursor.execute(sql)
    return cursor


def get_individual_records(db, tables, table_headers, foreign_keys):
    cursor = db.cursor()
    var_string = ''
    table = ''
    refs = ''
    var_string += table_headers[0] + "."
    var_string += table_headers[1] + ", "
    var_string = var_string[:-2]
    for table_name in tables:
        table += table_name + ", "
    table = table[:-2]
    if table_headers[0].find("discrete", 0) >= 0:
        for i in range(len(foreign_keys)):
            refs += "discrete_"
            refs += foreign_keys[i][2]+ "."
            refs += foreign_keys[i][0]+ " = "
            refs += "discrete_"
            refs += foreign_keys[i][1]
            refs += ".id and "
    else:
        for i in range(len(foreign_keys)):
            refs += foreign_keys[i][2]+ "."
            refs += foreign_keys[i][0]+ " = "
            refs += foreign_keys[i][1]
            refs += ".id and "
    refs = refs[:-5]
    sql = 'SELECT {} FROM {} WHERE {}'.format(var_string, table, refs)
    logging.debug("SQL query: %s", sql)
    cursor.execute(sql)
    return cursor


def print_records(cursor):    
    logging.debug("Current database:")
    for record in cursor:
        logging.debug(record)


def create_tables(db, tables, headers, attributes, foreign_keys, references):
    '''This function is only executed the first time the DB is created'''
    cursor = db.cursor()
    drop_tables(db, tables)
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
        sql = 'CREATE TABLE IF NOT EXISTS {} ({})'.format(tables[i], elements) 
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
    decision = []
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
        decision.append(get_attrib_from_headers(db, "decision", table_id))
    '''Get foreign keys'''
    cursor = db.cursor()
    sql = ("SELECT _foreign_keys.name, _foreign_keys.reference, _tables.name "
           " FROM _foreign_keys, _tables WHERE _foreign_keys.table_id = _tables.id") 
    sql += " ORDER BY _foreign_keys.id"
    cursor.execute(sql)
    for key in cursor:
        foreign_keys.append([key[0], key[1], key[2]])
    
    discrete_tables = list_discrete_tables(tables)
    
    ''' Print db structure'''
    logging.debug("Tables: %s", tables)
    logging.debug("Discrete Tables: %s", discrete_tables)
    logging.debug("Headers: %s", headers)
    logging.debug("Types: %s", types)
    logging.debug("User input: %s", user_input)
    logging.debug("Discretize: %s", discretize)
    logging.debug("Scripts: %s", scripts)
    logging.debug("Partition sizes: %s", partition_sizes)
    logging.debug("Offsets: %s", offsets)
    logging.debug("Decision: %s", decision)
    logging.debug("Foreign keys: %s", foreign_keys)  
    return (tables, discrete_tables, headers, types, user_input, discretize, scripts,
            partition_sizes, offsets, decision, foreign_keys)


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
                    scripts, partition_sizes, offsets, decision, foreign_keys, references):
    cursor = db.cursor()
    cursor.execute("DROP TABLE IF EXISTS _tables")
    cursor.execute("DROP TABLE IF EXISTS _headers")
    cursor.execute("DROP TABLE IF EXISTS _foreign_keys")
    create_db_struct(db)
    
    for i in range(len(tables)):
        cursor.execute("INSERT INTO _tables "
                        "(name) "
                        "VALUES (?)",
                        (tables[i],))
    
    for i, table_headers in enumerate(headers):
        for j in range(len(table_headers)):
            cursor.execute("INSERT INTO _headers "
                            "(table_id, name, type, user_input, attributes, discretize, script, partition_size, offset, decision) "
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            (i + 1, headers[i][j], types[i][j], user_input[i][j],
                              attributes[i][j], discretize[i][j], scripts[i][j],
                               partition_sizes[i][j], offsets[i][j], decision[i][j]))
    for i, foreign_key in enumerate(foreign_keys):
        for j in range(len(foreign_key)):
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
    cursor.execute("CREATE TABLE IF NOT EXISTS _tables ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "name TEXT UNIQUE NOT NULL) ")
    cursor.execute("CREATE TABLE IF NOT EXISTS _headers ("
            "id INTEGER PRIMARY KEY AUTOINCREMENT UNIQUE NOT NULL, "
            "table_id INTEGER NOT NULL, "
            "name TEXT NOT NULL, "
            "type TEXT NOT NULL, "
            "user_input INTEGER NOT NULL, "
            "attributes TEXT NOT NULL, "
            "discretize INTEGER NOT NULL, "
            "script TEXT, "
            "partition_size INTEGER, "
            "offset INTEGER, "
            "decision INTEGER) ")
    cursor.execute("CREATE TABLE IF NOT EXISTS _foreign_keys ("
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
    logging.debug("Structure:")
    cursor.execute(sql)
    for headers in cursor:
        logging.debug("{0[0]}: {0[1]} {0[2]} {0[3]} ".format(headers))
    
    cursor = db.cursor()
    sql = ("SELECT _tables.name, _foreign_keys.name, _foreign_keys.reference "
           " FROM _tables, _foreign_keys WHERE _foreign_keys.table_id = _tables.id") 
    sql += " ORDER BY _foreign_keys.id"
    cursor.execute(sql)
    for foreign_key in cursor:
        logging.debug("{0[0]}: FOREIGN KEY {0[1]} {0[2]}".format(foreign_key))


def import_records_fromXML(db, tables, headers, types, foreign_keys, user_input):
    value_table = []
    value_row = []
    value = []
    filename = "records.xml"
    #filename = Console.get_string("Import from", "filename")
    if not filename:
        return
    try:
        tree = xml.etree.ElementTree.parse(filename)
    except (EnvironmentError,
            xml.parsers.expat.ExpatError) as err:
        print("ERROR:", err)
        return

    for i, table_header in enumerate(headers):
        reset_tables(db, [tables[i]])
        for element in tree.findall(tables[i]):
            try:
                for j in range(len(table_header)):
                    if user_input[i][j]==1:
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
    logging.debug("Imported data: %s", value_table)
    ref_table_id = 0
    main_table_id = 0
    if len(foreign_keys) > 0:
        for i, current_key in enumerate(foreign_keys):
            for p, table in enumerate(tables):
                '''Store the index of the table that contains the reference'''
                if table == foreign_keys[i][1]:
                    ref_table_id = p
                if table == foreign_keys[i][2]:
                    main_table_id = p
            for k, current_value_row in enumerate(value_table[ref_table_id]):
                try:
                    key_value = get_and_set_foreign(db, tables[ref_table_id],
                                                     headers[ref_table_id], 
                                                     current_key, current_value_row)
                    value_table[main_table_id][k].append(key_value)
                    insert_sqlite(db, tables[main_table_id], headers[main_table_id], 
                                  value_table[main_table_id][k])
                except ValueError as err:
                    db.rollback()
                    print("ERROR:", err)
                    break
    else:
        db.commit()

        
def insert_sqlite(db, tablename, headers, values):
    cursor = db.cursor()
    columns = ', '.join(headers)
    columns = columns[4:]
    placeholders = ', '.join('?' * len(values))
    sql = 'INSERT INTO {} ({}) VALUES ({})'.format(tablename, columns, placeholders)
    logging.debug("SQL: %s", sql)
    cursor.execute(sql, values)

 
def get_and_set_foreign(db, table, header, foreign_key, value_row):
    '''TODO: Why header[1]?'''
    foreign_id = get_foreign_id(db, header[1], table, value_row[0])
    if foreign_id is not None:
        return foreign_id
    insert_sqlite(db, table, header, value_row)
    db.commit()
    return get_foreign_id(db, header[1], table, value_row[0])


def get_foreign_id(db, header, reference, value):
    cursor = db.cursor()
    cursor.execute("SELECT id FROM " + reference + " WHERE " + header + "=?",(value,))
    fields = cursor.fetchone()
    return fields[0] if fields is not None else None

def import_modules(scripts):
    moduleNames = []
    for table in scripts:
        for script in table:
            if script != '':
                moduleNames.append(script)
    modules = map(__import__, moduleNames)
    return list(modules)

def obtain_reduced_table(db, discrete_tables, tables, headers, foreign_keys,
                          relevant_attribute_list, dreduct, map_i, map_j):
    reduced_table = []
    table_header_names = []
    records = []
    results = []
    for attribute in dreduct:
        reduced_table.append([])
        i = map_i[attribute-1]
        j = map_j[attribute-1]
        table_header_names.append([discrete_tables[i],headers[i][j]])
    i =  map_i[len(map_i)-1]
    j =  map_j[len(map_j)-1]
    table_header_names.append([tables[i],headers[i][j]])
    logging.debug("Table-header pairs: %s", table_header_names)
    for k in range(len(table_header_names)):
        if k < len(table_header_names) - 1: 
            records.append(get_individual_records(db, discrete_tables,
                                                   table_header_names[k], foreign_keys))
        else:
            results_record = get_individual_records(db, tables, table_header_names[k], 
                                                  foreign_keys)
    #records = get_records(db, new_tables, new_headers, foreign_keys)
    for record in results_record:
        results.append(record[0])
    for i, record in enumerate(records):
        for j, value in enumerate(record):
            if relevant_attribute_list[j].count(dreduct[i]) > 0:
                reduced_table[i].append([value[0], results[j]])
                logging.debug("i: %s, j:%s Value: %s, Result: %s, Relevant: %s", i, j,
                          value[0], results[j], relevant_attribute_list[j])
    logging.debug("Reduced table: %s", reduced_table)                        
    return reduced_table

def prediction_algorithm(discrete_record, reduced_table, dreduct, map_i, map_j, headers):
    avg_sum = 0
    num_records = 0
    for i, attribute in enumerate(dreduct):
        logging.debug("i: %s, attribute: %s", i,
                       headers[map_i[attribute-1]][map_j[attribute-1]])
        for record in reduced_table[i]:
            if discrete_record[map_i[attribute-1]][map_j[attribute-1]-1] == record[0]:
                avg_sum += record[1]
                num_records += 1
                logging.debug("Record matches: %s of %s, exec time: %s",
                              record[0], headers[map_i[attribute-1]][map_j[attribute-1]],
                               record[1])
    if num_records > 0:
        predicted_decision_value = avg_sum/num_records
    else:
        predicted_decision_value = -1
    logging.debug("Predicted decision value: %s", predicted_decision_value)
    return predicted_decision_value
                 
'''############################################################################
                            Start main program                               '''
''' Change level from DEBUG to avoid showing messages'''
logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)
''' Connect to test.sdb database, change as required'''
db = connect("test.sdb") 

'''#############################################################################
                            Initialize database                              '''

''' Import the database structure from tables.xml file in same directory; 
store the structure in the database and create the required tables if they don't exist'''
import_db_struct_fromXML(db)
''' If the db_struct is not imported, we can read the structure directly from the db'''
(tables, discrete_tables, headers, types, user_input, discretize, scripts,
            partition_sizes, offsets, decision, foreign_keys) = get_db_struct(db)          
''' Import records from XML if available'''
import_records_fromXML(db, tables, headers, types, foreign_keys, user_input)
''' Print regular DB for debugging purposes'''
print_records(get_records(db, tables, headers, foreign_keys))
''' Discretize imported values'''
refresh_discrete_tables(db, tables, headers, types, foreign_keys, discretize, 
                            offsets, decision, partition_sizes, user_input)
''' Print discrete DB for debugging'''
print_records(get_records(db, discrete_tables, headers, foreign_keys))

'''#############################################################################
                             Rough Set Theory                            '''
''' Obtain discernibility matrix, probably this could be in one function'''
discernibility_matrix, map_i, map_j = get_discernibility_matrix(db, discrete_tables, headers, 
                                                  foreign_keys, discretize, decision)
min_matrix = get_minimal_matrix(discernibility_matrix)
''' Obtain the d-reduct of the matrix'''
dreduct = get_dreduct(min_matrix)
''' Obtain the list of relevant attributes'''
''' Map this list to the actual indices'''
relevant_attribute_list = get_relevant_attribute_list(min_matrix)

''' Translate relevant attributes into reduced decision table'''
reduced_table = obtain_reduced_table(db, discrete_tables, tables, headers, 
                                     foreign_keys, relevant_attribute_list,
                                     dreduct, map_i, map_j)
'''#############################################################################
                            Obtain values for prediction                 '''
''' Importing relevant modules from script list'''
modules = import_modules(scripts)
''' Add individual record using scripts'''
(record, decision_script, decision_i, decision_j, real_j) = \
    read_record_from_scripts(headers, user_input, modules, decision)
''' Discretize new record '''
discrete_record = discretize_record(headers, types, discretize, offsets, decision,
                       partition_sizes, user_input, record)
'''#############################################################################
                                Prediction algorithm:
         1) First search for the records that match
         2) Next average the execution time of all these records'''
predicted_decision_value = prediction_algorithm(discrete_record, reduced_table, 
                                                dreduct, map_i, map_j, headers)

''' Measure execution time using script and append to records'''
record, discrete_record, real_decision_value = \
        obtain_real_decision_value(record, discrete_record, decision_script,
                                   decision_i, decision_j, real_j, modules, types,
                                   discretize, partition_sizes, offsets)
''' Store individual record in original DB'''
store_record_in_DB(db, tables, headers, foreign_keys, record)

''' Store discrete record in discrete DB'''
store_discrete_record(db, discrete_tables, headers, foreign_keys,
                         discrete_record)
''' Print regular DB for debugging purposes'''
print_records(get_records(db, tables, headers, foreign_keys))
''' Print discrete DB for debugging'''
print_records(get_records(db, discrete_tables, headers, foreign_keys))

''' Show prediction result'''
percent_error = abs(real_decision_value-predicted_decision_value)/real_decision_value * 100
logging.info("Predicted value: %s Actual value: %s Percent error: %.2f%%",
              predicted_decision_value, real_decision_value, percent_error)
'''TODO: Export DB options'''
