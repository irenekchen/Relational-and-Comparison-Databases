import json
import copy
import shutil
import os
import SCSVDataTable

people_csvt = SCSVDataTable.SCSVDataTable("People1", "People1.csv", ["playerID"])
people_csvt.load()

try:
    t1 = { "nameFirst": "Ted", "nameLast": "Williams"}
    print("Testing template ", t1, " on table", "People")
    result = people_csvt.find_by_template(t1)
    print("Query result is ")
    print(json.dumps(result, indent=2))
except Exception as e:
    print("Got exception = ", str(e))
    
try:
    t2 = { "nameLast": "Williams", "throws": "R"}
    fields2 = ['nameLast', 'nameFirst', 'birthCountry', 'throws', 'bats']
    print("Testing template ", t1, " on table", "People")
    print("With field list = ", fields2)
    result = people_csvt.find_by_template(t2, fields2)
    print("Query result is ")
    print(json.dumps(result, indent=2))
except Exception as e:
    print("Got exception = ", str(e))
    
people_csvt.insert({'playerID': 'dff1', 'nameLast': 'Ferguson', 'nameFirst': 'Donald'})
people_csvt.save()
people_csvt.delete({'playerID': 'dff1', 'nameLast': 'Ferguson', 'nameFirst': 'Donald'})