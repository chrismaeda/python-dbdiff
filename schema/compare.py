#
# compare schema of 2 databases
#

from typing import List, Tuple
from schema import Database, Table

class SchemaCompare:
    def __init__(self, db1: Database, db2: Database, canonicalize=None) -> None:
        self.db1 = db1
        self.db2 = db2 
        self.canonicalize = canonicalize

    # compare table sets in each db
    # return tables in both, tables only in 1, tables only in 2
    def diff_table_list(self) -> Tuple[List[Tuple], List[Tuple], List[Tuple]]:
        both = []
        only1 = []
        only2 = []
        db1_tables = self.db1.get_table_list()
        db2_tables = self.db2.get_table_list()

        # convert each table name to a tuple of ( canonicalized name, original name)
        if self.canonicalize is None:
            db1_tables = [ (table, table) for table in db1_tables ]
            db2_tables = [ (table, table) for table in db2_tables ]
        else:
            db1_tables = [ (self.canonicalize(table), table) for table in db1_tables ]
            db2_tables = [ (self.canonicalize(table), table) for table in db2_tables ]
            
        for canontable,origtable in db1_tables:
            tablefound = False
            for canon2,orig2 in db2_tables:
                if canontable == canon2:
                    both.append((origtable,orig2))
                    tablefound = True
                    break
            if not tablefound:
                only1.append((canontable,origtable))

        for canon2,orig2 in db2_tables:
            tablefound = False
            for canon1,orig1 in db1_tables:
                if canon1 == canon2:
                    tablefound = True
                    break
            if not tablefound:
                only2.append((canon2,orig2))
        return both, only1, only2

    def diff_procedure_list(self) -> Tuple[List[str], List[str], List[str]]:
        both = []
        only1 = []
        only2 = []
        db1_procs = self.db1.get_procedure_list()
        db2_procs = self.db2.get_procedure_list()

        for procname in db1_procs:
            if procname in db2_procs:
                both.append(procname)
            else:
                only1.append(procname)
        for procname in db2_procs:
            if procname not in db1_procs:
                only2.append(procname)
        return both, only1, only2

# Technically, this is a reverse topological sort since
# a table A with a foreign key link to table B would
# be modeled as a graph edge from A to B.
# reference: https://en.wikipedia.org/wiki/Topological_sorting
class TopoSort:
    @classmethod
    def sort(self, tables:List[Table]) -> List[Table]:
        # get list of tables with no dependencies
        sorted = []
        visited = {}
        nodelist = []

        # step 1: find tables with no foreign key dependencies
        for table in tables:
            hasfk = False
            for constraint in table.constraints:
                if constraint.is_foreign_key():
                    hasfk = True
                    break
            if not hasfk:
                sorted.append(table)
                visited[table.name] = table
            else:
                nodelist.append(table)

        # step 2: scan nodelist, adding nodes to sorted in topo order
        while len(nodelist) > 0:
            #print('Sorted:' + str(len(sorted_dict)) + ' Work:' + str(len(fklist)))
            for table in nodelist:
                all_deps_visited = True
                # check if table dependencies are already visited
                for constr in table.constraints:
                    if constr.is_foreign_key():
                        #print(table.name + '->' + constr.reference_table)
                        reftable = constr.reference_table
                        # note: must detect and ignore self-referential fk links
                        #   since this is a dependency loop that will break the algorithm
                        if reftable not in visited and reftable != table.name:
                            #print(reftable + ' not visited')
                            all_deps_visited = False
                            break
                if all_deps_visited:
                    # all dependencies of table are in the sorted list
                    sorted.append(table)
                    visited[table.name] = table
                    nodelist.remove(table)
        return sorted
