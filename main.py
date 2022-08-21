from mysqlx import Schema
from schema import Database
from schema.mysql import MySQLDatabase
from schema.compare import SchemaCompare, TopoSort
from util.database_credentials import read_credentials_file
from typing import List
import click

#
# execute schema compare
#

def read_database_from_env(envfile) -> Database:
    dbenv = read_credentials_file(envfile)
    dbname = dbenv['database']
    db = MySQLDatabase(dbname)
    db.connect(**dbenv)
    db.import_schema(dbname)
    return db

def diff_schema(db1:Database, db2:Database, canonicalize=None):
    cmp = SchemaCompare(db1, db2, canonicalize=canonicalize)
    tabsboth, tabsonly1, tabsonly2 = cmp.diff_table_list()

    for table1,table2 in tabsboth:
        print('TABLE: ' + table1 + ' ==? ' + table2)
        tbl1 = cmp.db1.get_table(table1)
        tbl2 = cmp.db2.get_table(table2)
        tsame, tdiff, tonly1, tonly2 = tbl1.diff_columns(tbl2, canonicalize=canonicalize)
        if len(tsame) >= 0 and len(tdiff) == 0 and len(tonly1) == 0 and len(tonly2) == 0:
            print("\tALL COLUMNS MATCH!")
        else:
            print("\tSame Columns:" + str(len(tsame)))
            if len(tdiff) > 0:
                cnames = [ c.name for c in tdiff ]
                print("\tDiff Columns:" + str(cnames))
            if len(tonly1) > 0:
                cnames = [ c.name for c in tonly1 ]
                print("\tOnly in DB1 Columns:" + str(cnames))
            if len(tonly2) > 0:
                cnames = [ c.name for c in tonly2 ]
                print("\tOnly in DB2 Columns:" + str(cnames))
        # diff auto_increment
        tbl1_ai = tbl1.auto_increment
        tbl2_ai = tbl2.auto_increment
        if tbl1_ai != tbl2_ai:
            print("\tAUTOINCREMENT: db1:" + str(tbl1_ai) + " <> db2:" + str(tbl2_ai))
        # diff constraints
        csame, cdiff, conly1, conly2 = tbl1.diff_constraints(tbl2, canonicalize=canonicalize)
        if len(csame) >= 0 and len(cdiff) == 0 and len(conly1) == 0 and len(conly2) == 0:
            print("\tALL CONSTRAINTS MATCH!")
        else:
            if len(csame) > 0:
                cnames = [ c.name for c in csame ]
                print("\tSame Constraints:" + str(cnames))
            if len(cdiff) > 0:
                cnames = [ c.name for c in cdiff ]
                print("\tDiff Constraints: " + str(cnames))
            if len(conly1) > 0:
                cnames = [ c.name for c in conly1 ]
                print("\tOnly in DB1 Constraints:" + str(cnames))
            if len(conly2) > 0:
                cnames = [ c.name for c in conly2 ]
                print("\tOnly in DB2 Constraints:" + str(cnames))
        # diff indexes
        isame, idiff, ionly1, ionly2 = tbl1.diff_indexes(tbl2, canonicalize=canonicalize)
        if len(isame) >= 0 and len(idiff) == 0 and len(ionly1) == 0 and len(ionly2) == 0:
            print("\tALL INDEXES MATCH!")
        else:
            if len(isame) > 0:
                inames = [ ix.name for ix in isame ]
                print("\tSame Indexes:" + str(inames))
            if len(idiff) > 0:
                inames = [ ix.name for ix in idiff ]
                print("\tDiff Indexes: " + str(inames))
            if len(ionly1) > 0:
                inames = [ ix.name for ix in ionly1 ]
                print("\tOnly in DB1 Indexes:" + str(inames))
            if len(ionly2) > 0:
                inames = [ ix.name for ix in ionly2 ]
                print("\tOnly in DB2 Indexes:" + str(inames))

    if len(tabsonly1) > 0:
        print("\nTABLES ONLY IN DB1")
        for canontable,origtable in tabsonly1:
            print("\t" + origtable)
    if len(tabsonly2) > 0:
        print("\nTABLES ONLY IN DB2")
        for canontable,origtable in tabsonly2:
            print("\t" + origtable)

def diff_procs(db1:Database, db2:Database):
    cmp = SchemaCompare(db1, db2)
    both, only1, only2 = cmp.diff_procedure_list()
    for procname in both:
        proc1 = cmp.db1.get_procedure(procname)
        proc2 = cmp.db2.get_procedure(procname)
        if proc1 == proc2:
            print("PROC " + procname + " MATCH!")
        else:
            print("PROC " + procname + " DOES NOT MATCH!")
    if len(only1) > 0:
        print("\nPROCS Only in DB1")
        for procname in only1:
            print("\t" + procname)
    if len(only2) > 0:
        print("\nPROCS Only in DB2")
        for procname in only2:
            print("\t" + procname)

# This method returns a list of tables in reverse-dependency order.
# i.e. If table A has a foreign key link to table B, it will print B,A
# This is the order in which to save or restore the tables without breaking FK links.
#
# The canonicalize argument (if provided) will convert table names to canonical form
# and ignore duplicate tables with the same canonical name.  This is intended to deal
# with some corrupted mysql databases where there are duplicate tables with names
# like 'DATA_TABLE' and 'data_table'.
def table_list(db:Database, canonicalize=None):
    tablelist = db.get_table_list()
    tabledict = {}
    for tablename in tablelist:
        if canonicalize is None:
            tablenamekey = tablename
        else:
            tablenamekey = canonicalize(tablename)
        table = db.get_table(tablenamekey)
        if table is None:
            raise Exception('No Table ' + tablenamekey)
        # if canonicalize(tablename) is already in tabledict then ignore it
        if tablenamekey not in tabledict:
            tabledict[tablenamekey] = table

    # sort tables in dependency order (topological sort)
    sorted = TopoSort.sort(tabledict.values())
    for table in sorted:
        print(table.name + ': columns ' + str(len(table.columns)))
        tabcons = table.get_constraints()
        for tabcon in tabcons:
            if tabcon.is_foreign_key():
                print('\tFK -> ' + str(tabcon.reference_table))


def str_upper(s:str) -> str:
    return s.upper()

def str_lower(s:str) -> str:
    return s.lower()

@click.group()
def schemadiff():
    pass

@click.command()
@click.argument('db1')
@click.argument('db2')
@click.option('--uppercase', '--upper', default=False)
@click.option('--lowercase', '--lower', default=False)
def diffschema(db1, db2, uppercase, lowercase):
    canonicalize = None
    if uppercase:
        canonicalize = str_upper
    elif lowercase:
        canonicalize = str_lower
    dbobj1 = read_database_from_env(db1)
    dbobj2 = read_database_from_env(db2)
    diff_schema(dbobj1, dbobj2, canonicalize)

@click.command()
@click.argument('db1')
@click.argument('db2')
def diffprocs(db1, db2):
    dbobj1 = read_database_from_env(db1)
    dbobj2 = read_database_from_env(db2)
    diff_procs(dbobj1, dbobj2)

@click.command()
@click.argument('db')
@click.option('--uppercase', '--upper', default=False)
@click.option('--lowercase', '--lower', default=False)
def tablelist(db, uppercase, lowercase):
    dbobj = read_database_from_env(db)
    canonicalize = None
    if uppercase:
        canonicalize = str_upper
    elif lowercase:
        canonicalize = str_lower
    table_list(dbobj, canonicalize)


schemadiff.add_command(diffschema)
schemadiff.add_command(diffprocs)
schemadiff.add_command(tablelist)

if __name__ == "__main__":
    schemadiff()
