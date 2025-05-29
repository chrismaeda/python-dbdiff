#
# import mysql schema
#

from typing import Dict, List
from . import Constraint, Database, Table, Column, Index
import mysql.connector

# Sample record from information_schema.tables
# {'TABLE_CATALOG': 'def',
#  'TABLE_SCHEMA': 'source_db',
#  'TABLE_NAME': 'tablename',
#  'TABLE_TYPE': 'BASE TABLE',
#  'ENGINE': 'InnoDB',
#  'VERSION': 10,
#  'ROW_FORMAT': 'Dynamic',
#  'TABLE_ROWS': 1403,
#  'AVG_ROW_LENGTH': 22806,
#  'DATA_LENGTH': 31997952,
#  'MAX_DATA_LENGTH': 0,
#  'INDEX_LENGTH': 655360,
#  'DATA_FREE': 5242880,
#  'AUTO_INCREMENT': None,
#  'CREATE_TIME': datetime.datetime(2020, 5, 10, 2, 24, 30),
#  'UPDATE_TIME': datetime.datetime(2022, 7, 20, 1, 34, 2),
#  'CHECK_TIME': None,
#  'TABLE_COLLATION': 'utf8_bin',
#  'CHECKSUM': None,
#  'CREATE_OPTIONS': '',
#  'TABLE_COMMENT': ''}
#
class MySQLTable(Table):
    def __init__(self,
        TABLE_CATALOG=None, TABLE_SCHEMA=None, TABLE_NAME=None, TABLE_TYPE=None,
        ENGINE=None, VERSION=None, ROW_FORMAT=None,
        TABLE_ROWS=None, AVG_ROW_LENGTH=None, DATA_LENGTH=None, MAX_DATA_LENGTH=None,
        INDEX_LENGTH=None, DATA_FREE=None, AUTO_INCREMENT=None,
        CREATE_TIME=None, UPDATE_TIME=None, CHECK_TIME=None, 
        TABLE_COLLATION=None, CHECKSUM=None, CREATE_OPTIONS=None, TABLE_COMMENT=None):        
        super().__init__(TABLE_NAME, rows=TABLE_ROWS)
        self.data_length = DATA_LENGTH
        self.index_length = INDEX_LENGTH
        self.auto_increment = AUTO_INCREMENT
        self.table_collation = TABLE_COLLATION


# Note: information_schema.columns has additional columns that break down COLUMN_TYPE
class MySQLColumn(Column):
    def __init__(self,
        TABLE_NAME=None, COLUMN_NAME=None, COLUMN_TYPE=None, COLUMN_KEY=None,
        IS_NULLABLE=None, COLUMN_DEFAULT=None, ORDINAL_POSITION=None):
        super().__init__(COLUMN_NAME, COLUMN_TYPE)
        if IS_NULLABLE == 'YES':
            self.nullable = True
        else:
            self.nullable = False
        if COLUMN_KEY == 'PRI':
            self.primaryKey = True
        else:
            self.primaryKey = False
        self.defaultValue = COLUMN_DEFAULT
        self.position = ORDINAL_POSITION
        self.tableName = TABLE_NAME

class MySQLConstraint(Constraint):
    def __init__(self, TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE):
        super().__init__(name=CONSTRAINT_NAME, type=CONSTRAINT_TYPE, table=TABLE_NAME)

    def is_primary_key(self):
        return self.type == 'PRIMARY KEY'

    def is_foreign_key(self):
        return self.type == 'FOREIGN KEY'

class MySQLIndex(Index):
    def __init__(self, name, tableName, unique=False):
        super().__init__(name, tableName)
        self.unique = unique

    def add_column(self, name, collation, nullable, subPart, position):
        coldict = {'name':name, 'collation':collation, 'nullable':nullable, 'subpart':subPart, 'position':position}
        self.columns.append(coldict)


class MySQLDatabase(Database):
    def __init__(self, name):
        super().__init__(name)

    def connect(self, **kwargs):
        self.conn = mysql.connector.connect(**kwargs)

    def fetch_table_rows(self, tablename: str, where: str = None, orderby: str = None) -> List[Dict]:
        query = 'SELECT * FROM ' + tablename
        if where is not None:
            query = query + ' WHERE ' + where
        if orderby is not None:
            query = query + ' ORDER BY ' + orderby
        rows = []
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(query)
            for row in cursor:
                rows.append(row)
        return rows

    def fetch_table_rowcount(self, tablename: str, where: str = None) -> int:
        query = 'SELECT COUNT(*) FROM ' + tablename
        if where is not None:
            query = query + ' WHERE ' + where
        with self.conn.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            rowcount = row[0]
        return rowcount

    def fetch_tables(self, dbname) -> List[Table]:
        dbname = dbname or self.name
        mysql_tables_query = """SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE table_schema = %s"""
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(mysql_tables_query, (dbname,))
            dbrows = cursor.fetchall()
            tables = []
            for dbrow in dbrows:
                table = MySQLTable(**dbrow)
                self.add_table(table)
            return tables

    def fetch_columns(self, dbname) -> List[Column]:
        dbname = dbname or self.name
        mysql_columns_query = """SELECT 
                TABLE_NAME, COLUMN_NAME, COLUMN_TYPE,
                COLUMN_KEY, IS_NULLABLE, COLUMN_DEFAULT,
                ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE table_schema = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION"""
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(mysql_columns_query, (dbname,))
            dbrows = cursor.fetchall()
            columns = []
            for dbrow in dbrows:
                column = MySQLColumn(**dbrow)
                columns.append(column)
            return columns

    def fetch_index_columns(self, dbname) -> List[Index]:
        dbname = dbname or self.name
        mysql_indexes_query = """SELECT 
                TABLE_NAME, INDEX_NAME, NON_UNIQUE,
                COLUMN_NAME, COLLATION, SUB_PART, NULLABLE,
                SEQ_IN_INDEX
            FROM INFORMATION_SCHEMA.STATISTICS 
            WHERE table_schema = %s
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX"""
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(mysql_indexes_query, (dbname,))
            dbrows = cursor.fetchall()
            return dbrows

    def fetch_constraints(self, dbname) -> List[Constraint]:
        dbname = dbname or self.name
        mysql_constraints_query = """SELECT 
                TABLE_NAME, CONSTRAINT_NAME, CONSTRAINT_TYPE
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
            WHERE table_schema = %s
            ORDER BY TABLE_NAME, CONSTRAINT_NAME"""        
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(mysql_constraints_query, (dbname,))
            dbrows = cursor.fetchall()
            constraints = []
            for dbrow in dbrows:
                constraint = MySQLConstraint(**dbrow)
                constraints.append(constraint)

            # fetch column data for constraints
            mysql_constraints_columns_query = """SELECT 
                    CONSTRAINT_NAME, TABLE_NAME, 
                    COLUMN_NAME, ORDINAL_POSITION, 
                    POSITION_IN_UNIQUE_CONSTRAINT,
                    REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                WHERE table_schema = %s
                ORDER BY TABLE_NAME, CONSTRAINT_NAME, ORDINAL_POSITION"""
            cursor.execute(mysql_constraints_columns_query, (dbname,))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['CONSTRAINT_NAME']
                ctabname = dbrow['TABLE_NAME']
                cmatch = None
                for constraint in constraints:
                    if constraint.table == ctabname and constraint.name == cname:
                        cmatch = constraint
                        break
                if cmatch is None:
                    raise ValueError("Missing Constraint " + cname)
                # add column data to constraint
                colname = dbrow['COLUMN_NAME']
                cmatch.columns.append(colname)
                reftable = dbrow['REFERENCED_TABLE_NAME']
                if reftable is not None:
                    if cmatch.reference_table is None:
                        cmatch.reference_table = reftable
                    elif cmatch.reference_table != reftable:
                        raise ValueError('Constraint ' + cmatch.name 
                            + ' ref table mismatch ' + reftable + ' is not ' + cmatch.reference_table)
                refcolumn = dbrow['REFERENCED_COLUMN_NAME']
                if refcolumn is not None:
                    if cmatch.reference_columns is None:
                        cmatch.reference_columns = []
                    cmatch.reference_columns.append(refcolumn)
        return constraints                

    def fetch_routines(self, dbname) -> List[dict]:
        dbname = dbname or self.name
        mysql_routines_query = """SELECT 
                ROUTINE_NAME, ROUTINE_TYPE, ROUTINE_DEFINITION 
            FROM INFORMATION_SCHEMA.ROUTINES 
            WHERE ROUTINE_SCHEMA = %s
            ORDER BY ROUTINE_NAME"""        
        with self.conn.cursor(dictionary=True) as cursor:
            cursor.execute(mysql_routines_query, (dbname,))
            dbrows = cursor.fetchall()
            routines = []
            for dbrow in dbrows:
                rname = dbrow['ROUTINE_NAME']
                rtype = dbrow['ROUTINE_TYPE']
                rdef = dbrow['ROUTINE_DEFINITION']
                routine = {'name':rname, 'type':rtype, 'definition':rdef}
                routines.append(routine)
            return routines

    def import_schema(self, dbname):
        self.reset(dbname)
        # import table list
        dbtables = self.fetch_tables(self.name)
        for dbtable in dbtables:
            tablename = dbtable.name
            self.tables[tablename] = dbtable

        # import columns
        dbcolumns = self.fetch_columns(self.name)
        for dbcolumn in dbcolumns:
            tablename = dbcolumn.tableName
            table = self.get_table(tablename)
            if table == None:
                raise ValueError(tablename)
            table.columns.append(dbcolumn)

        # import index columns
        dbindexrows = self.fetch_index_columns(self.name)
        for dbrow in dbindexrows:
            tablename = dbrow['TABLE_NAME']
            indexName = dbrow['INDEX_NAME']
            nonUnique = dbrow['NON_UNIQUE']
            colName = dbrow['COLUMN_NAME']
            collation = dbrow['COLLATION']
            subpart = dbrow['SUB_PART']
            nullable = dbrow['NULLABLE']
            position = dbrow['SEQ_IN_INDEX']

            # find table
            table = self.get_table(tablename)
            if table == None:
                raise ValueError(tablename)
            # find or create index in table
            index = table.get_index(indexName)
            if index == None:
                # create new index object
                isUnique = nonUnique == 0
                index = MySQLIndex(indexName, tablename, isUnique)
                table.indexes.append(index)

            # add column to index
            # convert nullable to boolean
            nullable = (nullable == 'YES')
            index.add_column(colName, collation, nullable, subpart, position)

        # import constraints
        dbconstraints = self.fetch_constraints(self.name)
        for dbconstraint in dbconstraints:
            tablename = dbconstraint.table
            basetable = self.get_table(tablename)
            if basetable == None:
                raise ValueError('constraint ' + dbconstraint.name + ' missing table ' + tablename)
            basetable.constraints.append(dbconstraint)

        # import routines
        self.routines = self.fetch_routines(self.name)
