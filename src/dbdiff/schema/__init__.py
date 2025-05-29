#
# Base Classes for Database Schema
#

from typing import Dict, List, Tuple
import copy

class Column:
    def __init__(self, name, type, tableName=None, schema=None, nullable=True, primaryKey=False, defaultValue=None, constraints=None, position=None):
        self.name = name
        self.type = type
        self.nullable = nullable
        self.primaryKey = primaryKey
        self.defaultValue = defaultValue
        self.constraints = constraints
        self.position = position
        self.tableName = tableName
        self.schema = schema

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

    # copy this object, optionally converting table names to canonical form
    def copy(self, canonicalize=None):
        colcopy = copy.deepcopy(self)
        if canonicalize is not None:
            colcopy.tableName = canonicalize(colcopy.tableName)
        return colcopy

class Index:
    def __init__(self, name, tableName, schema:str = None):
        self.name = name
        self.tableName = tableName
        self.schema = schema
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

    # copy this object, optionally converting table names to canonical form
    def copy(self, canonicalize=None):
        idxcopy = copy.deepcopy(self)
        if canonicalize is not None:
            idxcopy.tableName = canonicalize(idxcopy.tableName)
        return idxcopy


class Constraint:
    def __init__(self, name:str=None, schema:str=None, type:str=None, table:str=None):
        self.name = name
        self.schema = schema
        self.type = type
        self.table = table
        self.columns = []
        self.reference_schema = None
        self.reference_table = None
        self.reference_columns = None

    def __eq__(self, __o: object) -> bool:
        if type(self) is not type(__o):
            return False
        try:
            return self.name == __o.name \
                and self.schema == __o.schema \
                and self.type == __o.type \
                and self.table == __o.table \
                and self.columns == __o.columns \
                and self.reference_schema == __o.reference_schema \
                and self.reference_table == __o.reference_table \
                and self.reference_columns == __o.reference_columns
        except:
            return False

    def is_foreign_key(self) -> bool:
        return False

    def is_primary_key(self) -> bool:
        return False

    # copy this object, optionally converting table names to canonical form
    def copy(self, canonicalize=None):
        concopy = copy.deepcopy(self)
        if canonicalize is not None:
            if concopy.table is not None:
                concopy.table = canonicalize(concopy.table)
            if concopy.reference_table is not None:
                concopy.reference_table = canonicalize(concopy.reference_table)
        return concopy


class Routine:
    def __init__(self, name, schema=None, type=None, definition=None):
        self.name = name
        self.schema = schema
        self.type = type
        self.definition = definition
        
    

class Table:
    def __init__(self, name, schema=None, rows=None):
        self.name = name
        self.schema = schema
        self.columns = []
        self.constraints = []
        self.indexes = []
        self.rows = rows
        self.data_length = 0
        self.index_length = 0
        self.auto_increment = None
        self.table_collation = None        

    def get_full_name(self) -> str:
        if self.schema is not None:
            return self.schema + '.' + self.name
        else:
            return self.name

    def get_columns(self) -> List[Column]:
        return self.columns

    def get_constraints(self) -> List[Constraint]:
        return self.constraints

    def get_indexes(self) -> List[Index]:
        return self.indexes

    def get_column(self, name:str) -> Column:
        match = None
        for column in self.columns:
            if column.name == name:
                match = column
                break
        return match

    def get_column_by_position(self, position: int) -> Column:
        match = None
        for column in self.columns:
            if column.position == position:
                match = column
                break
        return match

    def get_constraint(self, name:str) -> Constraint:
        for constraint in self.constraints:
            if constraint.name == name:
                return constraint
        return None

    def get_index(self, name:str) -> Index:
        for index in self.indexes:
            if index.name == name:
                return index
        return None

    def add_index(self, index:Index):
        self.indexes.append(index)
        
    def get_primary_key_columns(self) -> List[Column]:
        pklist = [ col for col in self.columns if col.primaryKey ]
        return pklist

    def diff_columns(self, table, canonicalize=None) -> Tuple[List[Column],List[Column],List[Column],List[Column]]:
        same = []
        notsame = []
        only1 = []
        only2 = []
        for col in self.get_columns():
            colname = col.name
            table2col = table.get_column(colname)
            if table2col is None:
                only1.append(col)
            elif col == table2col:
                same.append(col)
            elif canonicalize is not None:
                col1copy = col.copy(canonicalize=canonicalize)
                col2copy = table2col.copy(canonicalize=canonicalize)
                if col1copy == col2copy:
                    same.append(col)
                else:
                    notsame.append(col)    
            else:
                notsame.append(col)
        # check for only2 columns
        for col2 in table.columns:
            col2name = col2.name
            table1col = self.get_column(col2name)
            if table1col is None:
                only2.append(col2)
        return same, notsame, only1, only2

    def diff_indexes(self, table, canonicalize=None) -> Tuple[List[Index],List[Index],List[Index],List[Index]]:
        same = []
        notsame = []
        only1 = []
        only2 = []
        for idx in self.get_indexes():
            idxname = idx.name
            tab2idx = table.get_index(idxname)
            if tab2idx is None:
                only1.append(idx)
            elif idx == tab2idx:
                same.append(idx)
            elif canonicalize is not None:
                idx1copy = idx.copy(canonicalize=canonicalize)
                idx2copy = tab2idx.copy(canonicalize=canonicalize)
                if idx1copy == idx2copy:
                    same.append(idx)
                else:
                    notsame.append(idx)    
            else:
                notsame.append(idx)
        # check for only2 columns
        for idx2 in table.indexes:
            idx2name = idx2.name
            tab1idx = self.get_index(idx2name)
            if tab1idx is None:
                only2.append(idx2)
        return same, notsame, only1, only2

    def diff_constraints(self, table, canonicalize=None) -> Tuple[List[Constraint],List[Constraint],List[Constraint],List[Constraint]]:
        same = []
        notsame = []
        only1 = []
        only2 = []
        for con in self.get_constraints():
            conname = con.name
            tab2con = table.get_constraint(conname)
            if tab2con is None:
                only1.append(con)
            elif con == tab2con:
                same.append(con)
            elif canonicalize is not None:
                con1copy = con.copy(canonicalize=canonicalize)
                con2copy = tab2con.copy(canonicalize=canonicalize)
                if con1copy == con2copy:
                    same.append(con)
                else:
                    notsame(con)
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
        self.reset(name)

    def reset(self, dbname):
        self.name = dbname
        self.tables: Dict[Table] = dict()
        self.indexes: Dict[Index] = dict()
        self.routines: Dict[Routine] = dict()

    def get_table(self, tablename) -> Table:
        if tablename in self.tables:
            return self.tables[tablename]
        else:
            return None

    def add_table(self, table: Table):
        self.tables[table.name] = table
        
    def add_routine(self, routine: Routine):
        self.routines[routine.name] = routine
        
    def get_table_list(self, canonicalize=None) -> List[str]:
        if canonicalize is None:
            tablelist = [ table.get_full_name() for table in self.tables.values() ]
        else:
            tablelist = [ canonicalize(table.get_full_name()) for table in self.tables.values() ]
        return tablelist

    def find_constraint(self, name:str) -> Constraint:
        for table in self.tables.values():
            constraint = table.get_constraint(name)
            if constraint is not None:
                return constraint
        return None

    def get_procedure_list(self) -> List[str]:
        proclist = [ proc.name for proc in self.routines.values() ]
        return proclist

    def get_procedure(self, name:str) -> Routine:
        if name in self.routines:
            return self.routines[name]
        else:
            return None

    def fetch_table_rows(self, tablename:str, where:str, orderby:str) -> List[Dict]:
        return None

    def fetch_table_rowcount(self, tablename:str, where:str) -> int:
        return None


class SchemaAwareDatabase:
    def __init__(self, name: str, schemas: List[str] = None, default_schema: str = None):
        self.reset(name, schemas, default_schema)

    # use a nested Database class to represent a schema
    class Schema(Database):
        def __init__(self, name):
            super().__init__(name)

    def reset(self, dbname: str, schemas: List[str] = None, default_schema: str = None):
        self.name = dbname
        self.default_schema = default_schema
        self.schemas = dict()
        if schemas is not None:
            for schema in schemas:
                self.schemas[schema] = self.Schema(schema)

    def get_table(self, tablename: str, schema: str = None) -> Table:
        if schema is None:
            if '.' in tablename:
                schema, tablename = tablename.split('.')
            else:
                schema = self.default_schema
        dbschema = self.schemas.get(schema)
        if dbschema is None:
            raise ValueError(f"Invalid schema: {schema}")
        return dbschema.get_table(tablename)
    
    def add_table(self, table: Table):
        schema = table.schema
        dbschema = self.schemas.get(schema)
        if dbschema is None:
            dbschema = self.Schema(schema)
            self.schemas[schema] = dbschema
        dbschema.add_table(table)
        
    def get_table_list(self, canonicalize=None) -> List[str]:
        # build list of tables from all schemas
        tablelist = []
        for schema in self.schemas.values():
            tablelist.extend(schema.get_table_list(canonicalize))
        return tablelist
    
    def find_constraint(self, name:str, schema:str) -> Constraint:
        dbschema = self.schemas.get(schema)
        if dbschema is None:
            raise ValueError(f"Invalid schema: {schema}")
        return dbschema.find_constraint(name)

    def get_procedure_list(self) -> List[str]:
        # build list of procedures from all schemas
        proclist = []
        for schema in self.schemas.values():
            proclist.extend(schema.get_procedure_list())
        return proclist

    def get_procedure(self, name:str, schema: str = None) -> dict:
        schema = schema if schema is not None else self.default_schema
        dbschema = self.schemas.get(schema)
        if dbschema is None:
            raise ValueError(f"Invalid schema: {schema}")
        return dbschema.get_procedure(name)

    def add_routine(self, routine: Routine):
        schema = routine.schema
        dbschema = self.schemas.get(schema)
        if dbschema is None:
            dbschema = self.Schema(schema)
            self.schemas[schema] = dbschema
        dbschema.add_routine(routine)
        
    def fetch_table_rows(self, tablename:str, schema:str, where:str, orderby:str) -> List[Dict]:
        return None

    def fetch_table_rowcount(self, tablename:str, schema:str, where:str) -> int:
        return None
