import json
import RDBDataTable
import RDBQueryProcessor

# Make some tables
connect_info = {"localhost": "localhost", "dbuser": "root", "dbpassword": "pickles1", "db": "lahman2017"}
people = RDBDataTable.RDBDataTable("People", "People.csv", ["playerID"], connect_info)
batting = RDBDataTable.RDBDataTable("Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"], connect_info)
appearances = RDBDataTable.RDBDataTable("Appearances", "Appearances.csv", ['playerID', 'yearID', 'teamID'], connect_info)

tables = { "People": people, "Batting": batting, "Appearances": appearances}

# A constraint
c1 = { "target_table": "People", "target_attribute": "playerID", "source_table": "Batting",
       "source_attribute": "playerID"}

processor = RDBQueryProcessor.RDBQueryProcessor(tables, [c1])

try:
    tmp = { "table_name": "People", "operation_name": "find_by_template", "template": {"nameLast" : "Williams", "nameFirst": "Ted"}, \
            "fields": ['playerID', 'nameLast', 'nameFirst', 'birthCity']}
    rr = processor.find_by_query(tmp)
    print("Query = ", tmp)
    print("Result = \n", json.dumps(rr, indent=2))
except Exception as e:
    print("Got exception = ", str(e))
    
    
try:
    tmp = { "table_name": "Batting", "template": {"playerID" : "willite01"}, 
            "fields": ['G', 'AB', 'H', 'yearID']}
    rr = processor.find_by_query(tmp)
    print("Query = ", tmp)
    print("Result = \n", json.dumps(rr, indent=2))
except Exception as e:
    print("Got exception = ", str(e))