import json
import SCSVDataTable
import CSVQueryProcessor

# Make some tables
people = SCSVDataTable.SCSVDataTable("People1", "People1.csv", ["playerID"])
batting = SCSVDataTable.SCSVDataTable("Batting", "Batting.csv", ["playerID", "yearID", "teamID", "stint"])
appearances = SCSVDataTable.SCSVDataTable("Appearances", "Appearances.csv", ['playerID', 'yearID', 'teamID'])

tables = { "People1": people, "Batting": batting, "Appearances": appearances}

# A constraint
c1 = { "target_table": "People1", "target_attribute": "playerID", "source_table": "Batting",
       "source_attribute": "playerID"}

processor = CSVQueryProcessor.CSVQueryProcessor(tables, [c1])
try:
    tmp = { "table_name": "People1", "operation_name": "find_by_template", "template": {"nameLast" : "Ferguson", "nameFirst": "Donald"}, \
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