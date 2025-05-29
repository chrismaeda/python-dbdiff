from typing import List, Dict
from . import Column, Constraint, Database, Index, Routine, SchemaAwareDatabase, Table
from psycopg.conninfo import make_conninfo
from psycopg.rows import dict_row
import psycopg


class PostgresTable(Table):
    # the __init__ method must accept all the fields from the postgres information_schema.tables query
    def __init__(self,
        table_catalog=None, table_schema=None, table_name=None, table_type=None,
        self_referencing_column_name=None, reference_generation=None, user_defined_type_catalog=None,
        user_defined_type_schema=None, user_defined_type_name=None, is_insertable_into=None,
        is_typed=None, commit_action=None,
        TABLE_ROWS=None):
        super().__init__(table_name, table_schema, rows=TABLE_ROWS)
        # from mysql...
        #self.data_length = DATA_LENGTH
        #self.index_length = INDEX_LENGTH
        #self.auto_increment = AUTO_INCREMENT
        #self.table_collation = TABLE_COLLATION


class PostgresColumn(Column):
    # the __init__ method must accept all the fields from the postgres information_schema.columns query
    def __init__(self,
        table_catalog=None, table_schema=None, table_name=None, column_name=None, ordinal_position=None,
        column_default=None, is_nullable=None, data_type=None, character_maximum_length=None,
        character_octet_length=None, numeric_precision=None, numeric_precision_radix=None,
        numeric_scale=None, datetime_precision=None, interval_type=None, interval_precision=None,
        character_set_catalog=None, character_set_schema=None, character_set_name=None,
        collation_catalog=None, collation_schema=None, collation_name=None, domain_catalog=None,
        domain_schema=None, domain_name=None, udt_catalog=None, udt_schema=None, udt_name=None,
        scope_catalog=None, scope_schema=None, scope_name=None, maximum_cardinality=None,
        dtd_identifier=None, is_self_referencing=None, is_identity=None, identity_generation=None,
        identity_start=None, identity_increment=None, identity_maximum=None, identity_minimum=None,
        identity_cycle=None, is_generated=None, generation_expression=None, is_updatable=None
        ):
        if data_type == 'character varying':
            pg_data_type = f"{udt_name}({character_maximum_length})"
        else:
            pg_data_type = udt_name
        super().__init__(column_name, 
                         pg_data_type, 
                         tableName=table_name, 
                         schema=table_schema, 
                         position=ordinal_position,
                         defaultValue=column_default)
        if is_nullable == 'YES':
            self.nullable = True
        else:
            self.nullable = False


class PostgresConstraint(Constraint):
    # the __init__ method must accept all the fields from the postgres information_schema.table_constraints query
    def __init__(self,
        constraint_catalog=None, constraint_schema=None, constraint_name=None, constraint_type=None,
        table_catalog=None, table_schema=None, table_name=None, is_deferrable=None,
        initially_deferred=None, enforced=None, nulls_distinct=None
        ):
        super().__init__(name=constraint_name, schema=constraint_schema, type=constraint_type, table=table_name)
        self.check_clause = None

    def is_primary_key(self):
        return self.type == 'PRIMARY KEY'

    def is_foreign_key(self):
        return self.type == 'FOREIGN KEY'

    # example: 'fieldname IS NOT NULL'
    def add_check_clause(self, check_clause):
        parts = check_clause.split(' ')
        fieldname = parts[0]
        if fieldname not in self.columns:
            self.columns.append(fieldname)
        self.check_clause = check_clause
        

class PostgresIndex(Index):
    # the __init__ method must accept all the fields from the postgres pg_indexes query
    def __init__(self, 
        schemaname=None, tablename=None, indexname=None, tablespace=None, indexdef=None
        ):
        super().__init__(indexname, tablename, schemaname)
        self.tablespace = tablespace
        self.indexdef = indexdef

    def add_column(self, name, collation, nullable, subPart, position):
        coldict = {'name':name, 'collation':collation, 'nullable':nullable, 'subpart':subPart, 'position':position}
        self.columns.append(coldict)


class PostgresRoutine(Routine):
    # the __init__ method must accept all the fields from the postgres information_schema.routines query
    def __init__(self,
        specific_catalog=None, specific_schema=None, specific_name=None, 
        routine_catalog=None, routine_schema=None, routine_name=None, routine_type=None,
        module_catalog=None, module_schema=None, module_name=None, 
        udt_catalog=None, udt_schema=None, udt_name=None, data_type=None,
        character_maximum_length=None, character_octet_length=None, character_set_catalog=None,
        character_set_schema=None, character_set_name=None, 
        collation_catalog=None, collation_schema=None, collation_name=None,
        numeric_precision=None, numeric_precision_radix=None, numeric_scale=None,
        datetime_precision=None, interval_type=None, interval_precision=None,
        type_udt_catalog=None, type_udt_schema=None, type_udt_name=None,
        scope_catalog=None, scope_schema=None, scope_name=None, maximum_cardinality=None,
        dtd_identifier=None, routine_body=None, routine_definition=None,
        external_name=None, external_language=None, parameter_style=None,
        is_deterministic=None, sql_data_access=None, is_null_call=None,
        sql_path=None, schema_level_routine=None, max_dynamic_result_sets=None,
        is_user_defined_cast=None, is_implicitly_invocable=None, security_type=None,
        to_sql_specific_catalog=None, to_sql_specific_schema=None, to_sql_specific_name=None,
        as_locator=None, created=None, last_altered=None, 
        new_savepoint_level=None, is_udt_dependent=None, result_cast_from_data_type=None,
        result_cast_as_locator=None, result_cast_char_max_length=None, result_cast_char_octet_length=None,
        result_cast_char_set_catalog=None, result_cast_char_set_schema=None, result_cast_char_set_name=None,
        result_cast_collation_catalog=None, result_cast_collation_schema=None, result_cast_collation_name=None,
        result_cast_numeric_precision=None, result_cast_numeric_precision_radix=None, result_cast_numeric_scale=None,
        result_cast_datetime_precision=None, result_cast_interval_type=None, result_cast_interval_precision=None,
        result_cast_type_udt_catalog=None, result_cast_type_udt_schema=None, result_cast_type_udt_name=None,
        result_cast_scope_catalog=None, result_cast_scope_schema=None, result_cast_scope_name=None, result_cast_maximum_cardinality=None,
        result_cast_dtd_identifier=None
        ):
        super().__init__(routine_name, routine_schema, routine_type, routine_definition)


class PostgresDatabase(SchemaAwareDatabase):
    def __init__(self, name:str, schemas: List[str] = None):
        super().__init__(name, schemas)

    def connect(self, **kwargs):
        # groom kwargs from env file for psycopg
        if 'database' in kwargs:
            dbname = kwargs['database']
            del kwargs['database']
            if 'dbname' not in kwargs:
                kwargs['dbname'] = dbname
        if 'schemalist' in kwargs:
            # caller needs to have saved this prior to calling connect
            del kwargs['schemalist']
        conninfo = make_conninfo(**kwargs)
        self.conn = psycopg.connect(conninfo)

    def cursor(self):
        cur = self.conn.cursor(row_factory=dict_row)
        return cur
    
    def fetch_table_rows(self, tablename: str, where: str = None, orderby: str = None) -> List[Dict]:
        query = 'SELECT * FROM ' + tablename
        if where is not None:
            query = query + ' WHERE ' + where
        if orderby is not None:
            query = query + ' ORDER BY ' + orderby
        rows = []
        with self.cursor() as cursor:
            cursor.execute(query)
            for row in cursor:
                rows.append(row)
        return rows

    def fetch_table_rowcount(self, tablename: str, where: str = None) -> int:
        query = 'SELECT COUNT(*) FROM ' + tablename
        if where is not None:
            query = query + ' WHERE ' + where
        with self.cursor() as cursor:
            cursor.execute(query)
            row = cursor.fetchone()
            rowcount = row[0]
        return rowcount

    def fetch_tables(self, dbname: str, schema: str, table_type: str = 'BASE TABLE') -> List[Table]:
        dbname = dbname or self.name
        sql = """SELECT * FROM information_schema.tables
                 WHERE table_catalog = %s
                 AND table_schema = %s 
                 AND table_type = %s"""
        with self.cursor() as cursor:
            cursor.execute(sql, (dbname, schema, table_type))
            dbrows = cursor.fetchall()
            tables = []
            for dbrow in dbrows:
                table = PostgresTable(**dbrow)
                tables.append(table)
            return tables

    def fetch_columns(self, dbname: str, schema: str) -> List[Column]:
        dbname = dbname or self.name
        sql = """SELECT * FROM information_schema.columns
                 WHERE table_catalog = %s
                 AND table_schema = %s
                 ORDER BY table_name, ordinal_position""" 
        with self.cursor() as cursor:
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            columns = []
            for dbrow in dbrows:
                column = PostgresColumn(**dbrow)
                columns.append(column)
            return columns

    def fetch_indexes(self, dbname: str, schema: str) -> List[Index]:
        dbname = dbname or self.name
        sql = """SELECT * FROM pg_indexes WHERE schemaname = %s"""
        with self.cursor() as cursor:
            cursor.execute(sql, (schema,))
            dbrows = cursor.fetchall()
        for dbrow in dbrows:
            index = PostgresIndex(**dbrow)
            index_table = index.tableName
            table = self.get_table(index_table, schema)
            if table == None:
                raise ValueError(f"table {index_table} not found for index {index.indexname}")
            table.add_index(index)

    def fetch_foreign_keys_pgcat(self, dbname: str, schema: str) -> int:
        sql = """SELECT c.conname as constraint_name,
                        cl.relname as table_name, clns.nspname as table_schema,
                        refcl.relname as ref_table_name, refclns.nspname as ref_table_namespace,
                        c.conkey as columns, c.confkey as ref_columns
                 FROM pg_catalog.pg_constraint c
                 JOIN pg_catalog.pg_namespace ns ON c.connamespace = ns.oid 
                 JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
                 JOIN pg_catalog.pg_namespace clns on cl.relnamespace = clns.oid 
                 JOIN pg_catalog.pg_class refcl on c.confrelid = refcl.oid
                 JOIN pg_catalog.pg_namespace refclns on refcl.relnamespace = refclns.oid 
                 WHERE ns.nspname = %s and c.contype ='f'"""
        fkcount = 0
        with self.cursor() as cursor:
            cursor.execute(sql, (schema,))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['constraint_name']
                tabname = dbrow['table_name']
                tabschema = dbrow['table_schema']
                reftabname = dbrow['ref_table_name']
                reftabschema = dbrow['ref_table_namespace']
                columns = dbrow['columns']
                refcolumns = dbrow['ref_columns']
                
                constraint = PostgresConstraint(constraint_name=cname, 
                                                constraint_schema=schema, 
                                                constraint_type='FOREIGN KEY', 
                                                table_name=tabname,
                                                table_schema=tabschema)
                constraint.reference_table = reftabname
                constraint.reference_schema = reftabschema
                # map base table columns
                basetable = self.get_table(tabname, tabschema)
                if basetable is None:
                    raise ValueError(f"table not found:{tabname} schema:{tabschema}")
                for colindex in columns:
                    column = basetable.get_column_by_position(colindex)
                    if column is None:
                        raise ValueError(f"column not found:{colindex} in table {tabname}")
                    constraint.columns.append(column.name)
                # map ref table columns
                reftable = self.get_table(reftabname, reftabschema)
                if reftable is None:
                    raise ValueError(f"ref table not found:{reftabname} schema:{reftabschema}")
                for colindex in refcolumns:
                    column = reftable.get_column_by_position(colindex)
                    if column is None:
                        raise ValueError(f"column not found:{colindex} in table {reftabname}")
                    if constraint.reference_columns is None:
                        constraint.reference_columns = []
                    constraint.reference_columns.append(column.name)
                # add constraint to table
                basetable.constraints.append(constraint)
                fkcount += 1
        return fkcount

    def fetch_primary_keys_pgcat(self, dbname: str, schema: str) -> int:
        sql = """SELECT c.conname as constraint_name,
                        cl.relname as table_name, clns.nspname as table_schema,
                        c.conkey as columns
                 FROM pg_catalog.pg_constraint c
                 JOIN pg_catalog.pg_namespace ns ON c.connamespace = ns.oid 
                 JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
                 JOIN pg_catalog.pg_namespace clns on cl.relnamespace = clns.oid 
                 WHERE ns.nspname = %s and c.contype ='p'"""
        pkcount = 0
        with self.cursor() as cursor:
            cursor.execute(sql, (schema,))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['constraint_name']
                tabname = dbrow['table_name']
                tabschema = dbrow['table_schema']
                columns = dbrow['columns']
                
                constraint = PostgresConstraint(constraint_name=cname, 
                                                constraint_schema=schema, 
                                                constraint_type='PRIMARY KEY', 
                                                table_name=tabname,
                                                table_schema=tabschema)
                # map base table columns
                basetable = self.get_table(tabname, tabschema)
                if basetable is None:
                    raise ValueError(f"table not found:{tabname} schema:{tabschema}")
                for colindex in columns:
                    column = basetable.get_column_by_position(colindex)
                    if column is None:
                        raise ValueError(f"column not found:{colindex} in table {tabname}")
                    constraint.columns.append(column.name)
                    column.primaryKey = True
                # add constraint to table
                basetable.constraints.append(constraint)
                pkcount += 1
        return pkcount

    def fetch_unique_constraints_pgcat(self, dbname: str, schema: str):
        sql = """SELECT c.conname as constraint_name,
                        cl.relname as table_name, clns.nspname as table_schema,
                        c.conkey as columns
                 FROM pg_catalog.pg_constraint c
                 JOIN pg_catalog.pg_namespace ns ON c.connamespace = ns.oid 
                 JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
                 JOIN pg_catalog.pg_namespace clns on cl.relnamespace = clns.oid 
                 WHERE ns.nspname = %s and c.contype ='u'"""
        uqcount = 0
        with self.cursor() as cursor:
            cursor.execute(sql, (schema,))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['constraint_name']
                tabname = dbrow['table_name']
                tabschema = dbrow['table_schema']
                columns = dbrow['columns']
                
                constraint = PostgresConstraint(constraint_name=cname, 
                                                constraint_schema=schema, 
                                                constraint_type='UNIQUE', 
                                                table_name=tabname,
                                                table_schema=tabschema)
                # map base table columns
                basetable = self.get_table(tabname, tabschema)
                if basetable is None:
                    raise ValueError(f"table not found:{tabname} schema:{tabschema}")
                for colindex in columns:
                    column = basetable.get_column_by_position(colindex)
                    if column is None:
                        raise ValueError(f"column not found:{colindex} in table {tabname}")
                    constraint.columns.append(column.name)
                # add constraint to table
                basetable.constraints.append(constraint)
                uqcount += 1
        return uqcount

    def fetch_constraints_pgcat(self, dbname, schema) -> int:
        ccount = 0
        ccount += self.fetch_primary_keys_pgcat(dbname, schema)
        ccount += self.fetch_unique_constraints_pgcat(dbname, schema)
        ccount += self.fetch_foreign_keys_pgcat(dbname, schema)
        return ccount
                
    def fetch_constraints_infoschema(self, dbname, schema) -> int:
        dbname = dbname or self.name
        dbschema = self.schemas[schema]
        constr_count = 0
        
        sql = """SELECT * FROM information_schema.table_constraints 
                 WHERE constraint_catalog = %s
                 AND constraint_schema = %s
                 ORDER BY constraint_name"""
        with self.cursor() as cursor:
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                constraint = PostgresConstraint(**dbrow)
                # add constraint to table since constraint names are not unique
                tabname = constraint.table
                pgtable = self.get_table(tabname, schema)
                if pgtable == None:
                    raise ValueError(f"table not found:{tabname} schema:{schema}")
                pgtable.constraints.append(constraint)
                constr_count += 1

            # fetch data for constraint_table_usage
            sql = """SELECT * FROM information_schema.constraint_table_usage
                     WHERE table_catalog = %s
                     AND table_schema = %s
                     ORDER BY constraint_name"""
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['constraint_name']
                tabschema = dbrow['table_schema']
                tabname = dbrow['table_name']
                cmatch = dbschema.find_constraint(cname)
                if cmatch is None:
                    raise ValueError(f"constraint not found:{cname} in schema {schema}")
                if cmatch.type == 'FOREIGN KEY':
                    cmatch.reference_schema = tabschema
                    cmatch.reference_table = tabname
                else:
                    pass    # nothing to do

            # fetch data for constraint_column_usage
            # From https://www.postgresql.org/docs/current/infoschema-constraint-column-usage.html
            # "For a foreign key constraint, this view identifies the columns that the foreign key references."
            sql = """SELECT * FROM information_schema.constraint_column_usage
                     WHERE constraint_catalog = %s
                     AND constraint_schema = %s
                     ORDER BY constraint_name"""
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                tabschema = dbrow['table_schema']
                tabname = dbrow['table_name']
                cname = dbrow['constraint_name']
                colname = dbrow['column_name']
                cmatch = dbschema.find_constraint(cname)
                if cmatch is None:
                    raise ValueError(f"constraint not found:{cname} in schema {schema}")
                if cmatch.type == 'FOREIGN KEY':
                    if cmatch.reference_columns is None:
                        cmatch.reference_columns = []
                    cmatch.reference_columns.append(colname)
                else:
                    # check, unique, or primary key constraint
                    cmatch.columns.append(colname)

            # Note: we skip information_schema.key_column_usage since it is redundant with constraint_column_usage

            # fetch data for check constraints
            sql = """SELECT * FROM information_schema.check_constraints
                     WHERE constraint_catalog = %s
                     AND constraint_schema = %s
                     ORDER BY constraint_name"""
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            for dbrow in dbrows:
                cname = dbrow['constraint_name']
                # check constraints records are not linked to tables
                # so we have to use a brute force search
                cmatch = dbschema.find_constraint(cname)
                if cmatch is None:
                    raise ValueError(f"constraint not found:{cname} in schema {schema}")
                cclause = dbrow['check_clause']
                cmatch.add_check_clause(cclause)

        # scan tables for primary key constraints
        # for each, mark the columns as primary key columns
        for table in dbschema.tables.values():
            for dbconstraint in table.constraints:
                if dbconstraint.is_primary_key():
                    for colname in dbconstraint.columns:
                        col = table.get_column(colname)
                        col.primaryKey = True

        return constr_count

    def fetch_routines(self, dbname, schema) -> List[dict]:
        dbname = dbname or self.name
        sql = """SELECT * FROM information_schema.routines 
                 WHERE specific_catalog = %s
                 AND specific_schema = %s"""
        with self.cursor() as cursor:
            cursor.execute(sql, (dbname, schema))
            dbrows = cursor.fetchall()
            routines = []
            for dbrow in dbrows:
                routine = PostgresRoutine(**dbrow)
                self.add_routine(routine)
            return routines

    def import_schema(self, dbname, schema: str):
        # import table list
        dbtables = self.fetch_tables(self.name, schema)
        for dbtable in dbtables:
            self.add_table(dbtable)

        # import columns
        dbcolumns = self.fetch_columns(self.name, schema)
        for dbcolumn in dbcolumns:
            tablename = dbcolumn.tableName
            table = self.get_table(tablename, schema)
            if table == None:
                raise ValueError(tablename)
            table.columns.append(dbcolumn)

        # import indexes
        self.fetch_indexes(self.name, schema)
        
        # import constraints, constraints are saved in the table objects
        self.fetch_constraints_pgcat(self.name, schema)

        # import routines
        self.routines = self.fetch_routines(self.name, schema)
