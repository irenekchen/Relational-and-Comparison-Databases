import csv

class CSVQueryProcessor:

    # TableName: CSVDataTable object
    tables = {}
    # {target table, target attribute, source table, source attribute}
    """
    constraint = {"target_table_name": "People", "target_attribute_name": "playerID", "source_table_name:" "Batting", "source_attribute_name": "playerID"}
    I can only insert a row into Batting with "playerID" = "dff2" if there is a corresponding entry on People with the same value for playerID.
    I cannot delete the row from People of there is a row in Batting with the playerID that would be deleted.
    """
    
    constraints = {}
    
    def __init__(self, tables, constraints=None):
        self.tables = tables
        self.constraints = constraints
        pass

    # Query if of the form:
    # { "table_name": "some name", "template": {...}, "fields": [] }
    def find_by_query(self, q):
        # table
        self.tables[q["table_name"]].load()
        return self.tables[q["table_name"]].find_by_template(q["template"], q["fields"])
        pass

    # t_name is table name
    # Row is row to insert into table.
    def insert(self, q):
        t_name = self.tables[q["table_name"]]
        source_table = self.constraint["source_table_name"]
        self.tables[source_table].load()
        target_table = self.constraint["target_table_name"]
        self.tables[target_table].load()
        source_attribute = self.constraint["source_attribute_name"]
        target_attribute = self.constraint["target_atribute_name"]
        row = q["Row"]
        template = {}
        template[source_attribute] = row[source_attribute]
        #if row[source_attribute] in self.tables[target_table].find_by_template(template) #returns a list of dicts'
        # then , can insert row into source_table
        #template[source_attribute] = self.tables[source_table[source_attribute]]
        if (t_name == source_table):
            if len(self.tables[target_table].find_by_template(template) != 0):
            #if self.tables[source_table[source_attribute]].find_by_template(template)
            #if source_table[source_attribute] == target_table[target_attribute]:
                self.tables[q["table_name"]].insert(q["Row"])
        
        pass

    # Same for delete.
    # tmpl is the template to delete.
    def delete(self, q):
        t_name = self.tables[q["table_name"]]
        source_table = self.constraint["source_table_name"]
        self.tables[source_table].load()
        target_table = self.constraint["target_table_name"]
        self.tables[target_table].load()
        source_attribute = self.constraint["source_attribute_name"]
        target_attribute = self.constraint["target_atribute_name"]
        tmp1 = q["tmp1"]
        template = {}
        template[target_attribute] = tmp1[source_attribute]
        #if row[source_attribute] in self.tables[target_table].find_by_template(template) #returns a list of dicts'
        # then , can insert row into source_table
        #template[source_attribute] = self.tables[source_table[source_attribute]]
        if (t_name == target_table):
            if len(self.tables[source_table].find_by_template(template) == 0):
                self.tables[q["table_name"]].delete(q["Row"])
        else:
            pass
