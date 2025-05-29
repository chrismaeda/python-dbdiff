import pytest
from dbdiff.schema.compare import SchemaCompare
from dbdiff.schema.mysql import MySQLDatabase, MySQLTable


class TestSchemaCompare:
    def test_table_diff_same(self):
        db1 = MySQLDatabase('db1')
        db2 = MySQLDatabase('db2')

        t1 = MySQLTable(TABLE_NAME='table1')
        t2 = MySQLTable(TABLE_NAME='table2')
        t3 = MySQLTable(TABLE_NAME='table3')

        db1.tables['table1'] = t1
        db1.tables['table2'] = t2
        db1.tables['table3'] = t3

        db2.tables['table1'] = t1
        db2.tables['table2'] = t2
        db2.tables['table3'] = t3

        cmp = SchemaCompare(db1, db2)
        both, only1, only2 = cmp.diff_table_list()
        assert len(both) == 3
        assert ('table1','table1') in both
        assert ('table2','table2') in both
        assert ('table3','table3') in both
        assert len(only1) == 0
        assert len(only2) == 0

    def test_table_diff_only12(self):
        db1 = MySQLDatabase('db1')
        db2 = MySQLDatabase('db2')

        t1 = MySQLTable(TABLE_NAME='table1')
        t2 = MySQLTable(TABLE_NAME='table2')
        t3 = MySQLTable(TABLE_NAME='table3')

        db1.tables['table1'] = t1
        db1.tables['table2'] = t2
        # db1.tables['table3'] = t3

        db2.tables['table1'] = t1
        # db2.tables['table2'] = t2
        db2.tables['table3'] = t3

        cmp = SchemaCompare(db1, db2)
        both, only1, only2 = cmp.diff_table_list()
        assert len(both) == 1
        assert ('table1','table1') in both
        assert len(only1) == 1
        assert ('table2','table2') in only1
        assert len(only2) == 1
        assert ('table3','table3') in only2

