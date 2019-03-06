import RDBQueryProcessor

t = {"nameLast": "Williams", "nameFirst": "Ted"}
query = "SELECT * FROM country WHERE "
template = ""
vals = []
for fieldname in t.keys():
    query = query + "{}, "
    template = template + fieldname + " = " + "{}, "
    vals.append(t[fieldname])
query = query[:-4]

template = template[:-2].format(*["'"+vals[i]+"'" for i in range(len(vals))])
template_list = template.split(', ')
query = query.format(*[template_list[i] for i in range(len(template_list))])
print(query)
print(template_list)
print(template)


connect_info = {"localhost": "localhost", "dbuser": "root", "dbpassword": "pickles1", "db": "lahman2017"}
rdbt = RDBDataTable.RDBQueryProcessor("People", "People.csv", ["playerID"], connect_info)
t1 = {"nameLast": "Aaron"} #must find a way to append multiple queries, not AND
print("Testing template ", t1, " on table", "People")
result = rdbt.find_by_template(t1)
print(len(result))
print("Query result is ")
print(json.dumps(result, indent=2))
#playerID = input("Enter a player's ID: ")
#print("The player is \n", json.dumps(rdbt.find_by_template(playerID), indent=3))


rdbt.insert({'playerID': 'dff2', 'nameLast': 'Ferguson', 'nameFirst': 'Donald'})


rdbt.delete({'playerID': 'dff2'})
