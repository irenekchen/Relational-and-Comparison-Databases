import pymysql.cursors
import pandas as pd
import json

class RDBDataTable:

# The database server is running somewhere in the network.
# I must specify the IP address (HW server) and port number
# (connection that SW server is listening on)
# Also, I do not want to allow anyone to access the database
# and different people have different permissions. So, the
# client must log on.


# Connect to the database over the network. Use the connection
# to send commands to the DB.
    def __init__(self, t_name, t_file, key_columns, connect_info):
        self.t_name = t_name
        self.t_file = t_file
        self.key_columns = key_columns
        self.modified = False
        self.columns = None
        self.rows = None
        self.cnx = pymysql.connect(host=connect_info['localhost'], user=connect_info['dbuser'], 
                                   password=connect_info['dbpassword'], db=connect_info['db'], 
                                   charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        

    def load(self):
        pass

    def save(self):
        pass
    
    
    def find_by_template(self, t, fields=None):
        cursor = self.cnx.cursor()
        r = None
        if fields == None:
            query = "SELECT * FROM " + self.t_name +" WHERE "
            template = ""
            vals = []
            for fieldname in t.keys():
                query = query + "{} AND "
                template = template + fieldname + " = " + "{}, "
                vals.append(t[fieldname])
            query = query[:-4]
            template = template[:-2].format(*["'"+vals[i]+"'" for i in range(len(vals))])
            template_list = template.split(', ')
            query = query.format(*[template_list[i] for i in range(len(template_list))])
            cursor.execute(query)
            print(query)
            r = cursor.fetchall()
            return r
        else:
            field_list = ""
            for field in fields:
                field_list = field_list + field + ", "
            field_list = field_list[:-2]
            query = "SELECT " + field_list +" FROM " + self.t_name +" WHERE "
            print(query)
            template = ""
            vals = []
            for fieldname in t.keys():
                query = query + "{} AND "
                template = template + fieldname + " = " + "{}, "
                vals.append(t[fieldname])
            query = query[:-4]

            template = template[:-2].format(*["'"+vals[i]+"'" for i in range(len(vals))])
            template_list = template.split(', ')
            query = query.format(*[template_list[i] for i in range(len(template_list))])
            cursor.execute(query)
            print(query)
            r = cursor.fetchall()
            return r
        return r
    
   
    def insert(self, r):
        cursor = self.cnx.cursor()
        
        keys = []
        values = []
        sqlQuery1 = "INSERT INTO " + self.t_name + "("
        sqlQuery2 = " SELECT * FROM (SELECT "
        for key in r.keys():
            keys.append(key)
            values.append(r[key])
            sqlQuery1 = sqlQuery1 + "{}, "
            sqlQuery2 = sqlQuery2 + "{}, "
        print(keys,values)
        print(*[keys[i] for i in range(len(keys))])
        print(*[values[i] for i in range(len(values))])
        sqlQuery1 = sqlQuery1[:-2] + ")"
        sqlQuery2 = sqlQuery2[:-2] + ") AS tmp "
        sqlQuery1 = sqlQuery1.format(*[keys[i] for i in range(len(keys))])
        sqlQuery2 = sqlQuery2.format(*["'"+values[i]+"'" for i in range(len(values))])
        
        for key_column in self.key_columns:
                if (key_column not in r.keys()):
                    raise NameError("Missing key value for entry!")
        t_keys = []
        t_values = []
        query = "WHERE NOT EXISTS ( SELECT * FROM " + self.t_name + " WHERE "
        template = ""
        for key_column in self.key_columns:
            t_keys.append(key_column)
            t_values.append(r[key_column])
            query = query + "{} AND "
            template = template + key_column + " = " + "{}, "
        query = query[:-4]
        template = template[:-2].format(*["'"+t_values[i]+"'" for i in range(len(t_values))])
        template_list = template.split(', ')
        query3 = query.format(*[template_list[i] for i in range(len(template_list))]) + ")"
        

        query = sqlQuery1 + sqlQuery2 + query3
        print(query)
        cursor.execute(query)
        self.cnx.commit()
    
    def delete(self, t):
        
        cursor = self.cnx.cursor()
        
        query = "DELETE FROM " + self.t_name +" WHERE "
        template = ""
        vals = []
        for fieldname in t.keys():
            query = query + "{} AND "
            template = template + fieldname + " = " + "{}, "
            vals.append(t[fieldname])
        query = query[:-4]
        template = template[:-2].format(*["'"+vals[i]+"'" for i in range(len(vals))])
        template_list = template.split(', ')
        query = query.format(*[template_list[i] for i in range(len(template_list))])

        cursor.execute(query)
        self.cnx.commit()
        