#
# Base Classes for Database Schema
#

from typing import Dict, List, Tuple


class Column:
    def __init__(self, name, type, tableName=None, nullable=True, primaryKey=False, defaultValue=None, constraints=None, position=None):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.primaryKey = primaryKey
        self.defaultValue = defaultValue
        self.constraints = constraints
        self.position = position
        self.tableName = tableName

    def __eq__(self, __o: object) -> bool:
        if type(self) is not type(__o):
            return False
        try:
            return self.name == __o.name \
                and self.type == __o.type \
                and self.nullable == __o.nullable \
                and self.primaryKey == __o.primaryKey \
                and self.defaultValue == __o.defaultValue \
                and self.constraints == __o.constraints \
                and self.position == __o.position \
                and self.tableName == __o.tableName
        except:
            return False

    def diff(self, col: object) -> Tuple[bool, dict, dict]:
        if type(self) is not type(col):
            raise TypeError(str(type(col)))
        if self == col:
            return False, None, None
        diff1 = dict()
        diff2 = dict()
        for key in self.__dict__.keys():
            val1 = self.__dict__[key]
            val2 = col.__dict__[key]
            if val1 != val2:
                diff1[key] = val1
                diff2[key] = val2
        return True, diff1, diff2


class Index:
    def __init__(self, name, tableName):
        self.name = name
        self.tableName = tableName
        self.columns = []
        self.unique = False

    def get_column(self, name:str) -> dict:
        for col in self.columns:
            colname = col['name']
            if colname == name:
                return col
        return None

    def __eq__(self, __o: object) -> bool:
        if type(self) is not type(__o):
            return False
        try:
            return self.name == __o.name \
                and self.tableName == __o.tableName \
                and self.unique == __o.unique \
                and self.columns == __o.columns
        except:
            return False       

    def diff_columns(self, index):
        sames = []
        diffs = []
        only1 = []
        only2 = []
        for col in self.columns:
            colname = col['name']
            col2 = index.get_column(colname)
            if col2 is None:
                only1.append(col)
            elif col == col2:
                sames.append(col)
            else:
                diffs.append(col)
        # find columns only in 2
        for col2 in index.columns:
            col2name = col2['name']
            col1 = self.get_column(col2name)
            if col1 is None:
                only2.append(col2)
        return sames, diffs, only1, only2


class Constraint:
    def __init__(self, name:str=None, type:str=None, table:str=None):
        self.name = name
        self.type = type
        self.table = table
        self.columns = []
        self.reference_table = None
        self.reference_columns = None

    def __eq__(self, __o: object) -> bool:
        if type(self) is not type(__o):
            return False
        try:
            return self.name == __o.name \
                and self.type == __o.type \
                and self.table == __o.table \
                and self.columns == __o.columns \
                and self.reference_table == __o.reference_table \
                and self.reference_columns == __o.reference_columns
        except:
            return False


class Table:
    def __init__(self, name, schema=None, rows=None):
        self.name = name
        self.columns = []
        self.constraints = []
        self.indexes = []
        self.rows = rows
        self.data_length = 0
        self.index_length = 0
        self.auto_increment = None
        self.table_collation = None        

    def get_column(self, name) -> Column:
        match = None
        for column in self.columns:
            if column.name == name:
                match = column
                break
        return match

    def get_constraint(self, name) -> Constraint:
        for constraint in self.constraints:
            if constraint.name == name:
                return constraint
        return None

    def get_index(self, name) -> Index:
        for index in self.indexes:
            if index.name == name:
                return index
        return None

    def get_primary_key_columns(self) -> List[Column]:
        pklist = [ col for col in self.columns if col.primaryKey ]
        return pklist

    def diff_columns(self, table) -> Tuple[List[Column],List[Column],List[Column],List[Column]]:
        same = []
        notsame = []
        only1 = []
        only2 = []
        for col in self.columns:
            colname = col.name
            table2col = table.get_column(colname)
            if table2col is None:
                only1.append(col)
            elif col == table2col:
                same.append(col)
            else:
                notsame.append(col)
        # check for only2 columns
        for col2 in table.columns:
            col2name = col2.name
            table1col = self.get_column(col2name)
            if table1col is None:
                only2.append(col2)
        return same, notsame, only1, only2

    def diff_indexes(self, table):
        same = []
        notsame = []
        only1 = []
        only2 = []
        for idx in self.indexes:
            idxname = idx.name
            tab2idx = table.get_index(idxname)
            if tab2idx is None:
                only1.append(idx)
            elif idx == tab2idx:
                same.append(idx)
            else:
                notsame.append(idx)
        # check for only2 columns
        for idx2 in table.indexes:
            idx2name = idx2.name
            tab1idx = self.get_index(idx2name)
            if tab1idx is None:
                only2.append(idx2)
        return same, notsame, only1, only2

    def diff_constraints(self, table) -> Tuple[List[Constraint],List[Constraint],List[Constraint],List[Constraint]]:
        same = []
        notsame = []
        only1 = []
        only2 = []
        for con in self.constraints:
            conname = con.name
            tab2con = table.get_constraint(conname)
            if tab2con is None:
                only1.append(con)
            elif con == tab2con:
                same.append(con)
            else:
                notsame.append(con)
        # check for only2 columns
        for con2 in table.constraints:
            con2name = con2.name
            tab1con = self.get_constraint(con2name)
            if tab1con is None:
                only2.append(con2)
        return same, notsame, only1, only2

class Database:
    def __init__(self, name):
        self.name = name

    def get_table(self, tablename) -> Table:
        return None

    def get_table_list(self) -> List[str]:
        return None

    def get_procedure_list(self) -> List[str]:
        return None

    def get_procedure(self, name:str) -> dict:
        return None

    def fetch_table_rows(self, tablename:str, where:str, orderby:str) -> List[Dict]:
        return None