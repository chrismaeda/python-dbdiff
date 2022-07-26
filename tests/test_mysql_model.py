import pytest

from schema.mysql import MySQLColumn

class TestMySQLColumn:
    def test_create_column(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        assert col1.tableName == 'table'
        assert col1.name == 'column'
        assert col1.type == 'varchar(255)'
        assert col1.defaultValue is None
        assert not col1.nullable
        assert col1.position == 1

    def test_column_equals(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        col2 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        assert col1 == col2

    def test_column_notequals(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        col2 = MySQLColumn('table', 'column', 'varchar(255)', None, 'YES', None, 1)
        assert col1 != col2
        assert col2.nullable

    def test_equals_mistype(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        col2 = {'table':'table', 'column':'column', 'type':'varchar(255)', 'nullable':False, 'position':1}
        assert col1 != col2

    def test_diff_equals(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        col2 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        difftest, dict1, dict2 = col1.diff(col2)
        assert not difftest
        assert dict1 is None
        assert dict2 is None

    def test_diff_mistype(self):
        col1 = MySQLColumn('table', 'column', 'varchar(255)', None, '', None, 1)
        col2 = {'table':'table', 'column':'column', 'type':'varchar(255)', 'nullable':False, 'position':1}
        with pytest.raises(TypeError):
            difftest, dict1, dict2 = col1.diff(col2)

    def test_diff_colname(self):
        col1 = MySQLColumn('table', 'column1', 'varchar(255)', None, '', None, 1)
        col2 = MySQLColumn('table', 'column2', 'varchar(255)', None, '', None, 1)
        difftest, dict1, dict2 = col1.diff(col2)
        assert difftest
        assert dict1 == {'name':'column1'}
        assert dict2 == {'name':'column2'}

    def test_diff_colname_position(self):
        col1 = MySQLColumn('table', 'column1', 'varchar(255)', None, '', None, 1)
        col2 = MySQLColumn('table', 'column2', 'varchar(255)', None, '', None, 2)
        difftest, dict1, dict2 = col1.diff(col2)
        assert difftest
        assert dict1 == {'name':'column1', 'position':1}
        assert dict2 == {'name':'column2', 'position':2}

    def test_diff_nullable_default(self):
        col1 = MySQLColumn('table', 'column1', 'varchar(255)', None, 'YES', None, 1)
        col2 = MySQLColumn('table', 'column1', 'varchar(255)', None, '', "0", 1)
        difftest, dict1, dict2 = col1.diff(col2)
        print(dict1)
        print(dict2)
        assert difftest
        assert dict1 == {'nullable':True, 'defaultValue':None}
        assert dict2 == {'nullable':False, 'defaultValue':"0"}

