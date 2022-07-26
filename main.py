from schema.mysql import MySQLDatabase
from schema.compare import SchemaCompare

db1 = None
db2 = None

db1 = MySQLDatabase('source_db')
db1.connect(host='src_host',user='src_user',password='src_password')
db1.import_schema(db1.name)

db2 = MySQLDatabase('dest_db')
db2.connect(host='dest_host', user='dest_user', password='dest_password')
db2.import_schema(db2.name)

#
# execute schema compare
#
cmp = SchemaCompare(db1, db2)

def diff_tables():
    tabsboth, tabsonly1, tabsonly2 = cmp.diff_table_list()
    for table in tabsboth:
        print('TABLE: ' + table)
        tbl1 = cmp.db1.get_table(table)
        tbl2 = cmp.db2.get_table(table)
        tsame, tdiff, tonly1, tonly2 = tbl1.diff_columns(tbl2)
        if len(tsame) >= 0 and len(tdiff) == 0 and len(tonly1) == 0 and len(tonly2) == 0:
            print("\tCOLUMNS MATCH!")
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
        csame, cdiff, conly1, conly2 = tbl1.diff_constraints(tbl2)
        if len(csame) >= 0 and len(cdiff) == 0 and len(conly1) == 0 and len(conly2) == 0:
            print("\tCONSTRAINTS MATCH!")
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
        isame, idiff, ionly1, ionly2 = tbl1.diff_indexes(tbl2)
        if len(isame) >= 0 and len(idiff) == 0 and len(ionly1) == 0 and len(ionly2) == 0:
            print("\tINDEXES MATCH!")
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
        for table in tabsonly1:
            print("\t" + table)
    if len(tabsonly2) > 0:
        print("\nTABLES ONLY IN DB2")
        for table in tabsonly2:
            print("\t" + table)

def diff_procs():
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

if __name__ == "__main__":
    diff_tables()
    diff_procs()
