#PandasSQL tutorial
------

PandasSQL is a light and convenient tools to load data from csv file and save to database.

###Define database

	from angora.PandasSQL.sqlite3blackhole import CSVFile, Sqlite3BlackHole
	bh = Sqlite3BlackHole(":memory:") # define database

###Define csv file
	
	### you can also use for loop to easily define mutiple files
    # define first file
    advertisement = CSVFile(r"test_data/advertisement.txt", # path
                            table_name="advertisement", # table_name
                            sep=",", # seperator
                            dtype={"id": "TEXT", "hour": "DATETIME"}, # datatype
                            usecols=[0,1,2,3,4,5,6,7,8,9], # use cols
                            primary_key_columns=["id"]) # primary_key_columns
    # define second file
    employee = CSVFile(r"test_data/employee.txt",
                       table_name="employee",
                       sep=",",
                       dtype={"employee_id": "TEXT", "start_date": "DATE"},
                       primary_key_columns=["employee_id"])

###Read data

    bh.add(advertisement) # add file to pipeline
    bh.add(employee)

	bh.devour() # this will insert everything into database
    ## bh.update() # this will execute insert & update process
	
	### DONE!!!

Then it's already in your database