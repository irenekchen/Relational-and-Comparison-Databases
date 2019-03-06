import csv

class SCSVDataTable:

    data_dir = "/Users/irene/Downloads/Data/"
    t_name = ""
    t_file = ""
    key_columns = ""
    csv_data = []
    temp_file = "temp.csv"
    del_file = "del.csv"
    #temp_file = NamedTemporaryFile(delete=False)
    #del_file = NamedTemporaryFile(delete=False)
    fieldnames = []
    
    # t_name: The "Name" of the collection.
    # t_file: The name of the CSV file. The class looks in the data_dir for the file.
    def __init__(self, t_name, t_file, key_columns):
        # Your code goes here
        self.t_name = t_name
        self.t_file = t_file
        self.key_columns = key_columns

    
    # Pretty print the CSVTable and its attributes.
    def __str__(self):
        # Your code goes here.
        # Optional
        with open(self.data_dir + self.t_file) as csv_file:
            csv_reader = csv.reader(csv_file)
            for line in csv_reader:
                print(line)
        return ""        

    # loads the data from the file into the class instance data.
    # You decide how to store and represent the rows from the file.
    def load(self):
        # Your code goes here
        with open(self.data_dir + self.t_file) as csv_file:
            csv_reader = csv.DictReader(csv_file)
            self.fieldnames = next(csv_reader)
        with open(self.data_dir + self.t_file, "r") as csv_file:
            with open(self.data_dir + self.temp_file, "w") as temp_file:
                csv_reader = csv.DictReader(csv_file)
                csv_writer = csv.DictWriter(temp_file, fieldnames = self.fieldnames)
                csv_writer.writeheader()
                for line in csv_reader:
                    self.csv_data.append(line)
                    csv_writer.writerow(line)
        
    # Obvious
    def save(self):
        shutil.move(self.data_dir + self.temp_file, self.data_dir + self.t_file)    

        
    # The input is:
    # t: The template to match. The result is a list of rows
    # whose attribute/value pairs exactly match the template.
    # fields: A subset of the fields to include for each result.
    # Raises an exception if the template or list of fields contains
    # a column/attribute name not in the file.
    def find_by_template(self, t, fields=None):
        # Your code goes here
        matches = self.csv_data
        for fieldname in t.keys():
            matches = [x for x in matches if (x[fieldname] == t[fieldname])]
        if (fields != None):
            filtered_matches = []
            for match in matches:
                filtered_match = {}
                for field in match.keys():
                    if (field in fields):
                        filtered_match[field] = match[field]
                filtered_matches.append(filtered_match)
            return filtered_matches
        return matches
    
    # Inserts the row into the table. 
    # Raises on duplicate key or invalid columns.
    def insert(self, r):
        # Your code goes here
        with open(self.data_dir + self.temp_file, "a") as temp_file:
            csv_writer = csv.DictWriter(temp_file, fieldnames = self.fieldnames, restval = None)
            for key_column in self.key_columns:
                if (key_column not in r.keys()):
                    raise NameError("Missing key value for entry!")
            for entry in self.csv_data:
                entry_exists = 0
                for key_column in self.key_columns:
                    if (r[key_column] == entry[key_column]):
                        entry_exists += 1
                if (entry_exists == len(self.key_columns)):
                    raise NameError("Key value already exists for entry!")
            csv_writer.writerow(r)
        for field in self.fieldnames:
            if field not in r.keys():
                r[field] = None
        self.csv_data.append(r)

    # t: A template.
    # Deletes all rows matching the template.
    def delete(self, t):
        # Your code goes here.
        with open(self.data_dir + self.temp_file, "r") as temp_file:
            with open(self.data_dir + self.del_file, "w") as del_file:
                csv_reader = csv.DictReader(temp_file)
                csv_writer = csv.DictWriter(del_file, fieldnames = self.fieldnames)
                csv_writer.writeheader()
                matches = self.csv_data
                for fieldname in t.keys():
                    matches = [x for x in matches if (x[fieldname] == t[fieldname])]
                for line in csv_reader:
                    if (line not in matches):
                        csv_writer.writerow(line)
        shutil.move(self.data_dir + self.del_file, self.data_dir + self.temp_file)
        #os.rename(self.data_dir + self.del_file, self.data_dir + self.temp_file)
