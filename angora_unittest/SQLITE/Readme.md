#SQLITE tutorial
------
SQLite is an amazing library that gets embedded inside the application that makes use of. Unlike MSSQL, OracleDB, MySQL, Postgres and other database system, SQLite is a self-contained, file-based database, SQLite offers an amazing set of tools to handle all sorts of data with much less constraint and ease compared to hosted, process based (server) relational databases.

When an application uses SQLite, the integration works with functional and direct calls made to a file holding the data (i.e. SQLite database) instead of communicating through an interface of sorts (i.e. ports, sockets). This makes SQLite extremely fast and efficient, and also powerful thanks to the library's underlying technology.

##Why I create a SQLITE wrapper extension
SqlAlchemy is an amazing extension makes manipulating all RDBMS so simple. But Sqlite is a special one. Because it's a file-based and single-writer concurrence database, although it is not good for writing intensive environment, but this brings outstanding I/O performance better than other RDBMS. Which means it is an extremely good solution for testing, embedded system and inter-system. But SqlAlchemy doesn't make any optimization for sqlite.

##Features of SQLITE wrapper

- use human language to define table/column, to execute INSERT, DELETE, UPDATE, SELECT. 
- no SQL language needed, your workload will be minimized.
- seemlessly compatible to any Python object.
- highly optimized ultra-performance.
- autocommit or non-autocommit mode.

###Define a engine

	engine = Sqlite3Engine("test.db") # use Sqlite3Engine(":memory:") for memory mode
	metadata = MetaData()
	datatype = DataType() 

Sqlite3Engine() defines the database you are playing with. 

MetaData() is a metadata class to store engine, tables, columns metadata.

DataType() is a base class for all datatype. For example, you can use datatype.integer to access a integer type

###Define a table
	
	col_movie_id = Column("movie_id", datatype.text, primary_key=True)
	col_title = Column("title", datatype.text, default="unknown_title")
	col_length = Column("length", datatype.integer, default=-1)
	col_rate = Column("rate", datatype.real, default=0.01)
	col_release_date = Column("release_date", datatype.date, default="0000-01-01")
	col_genres = Column("genres", datatype.pickletype, default=set())
	movie = Table("movie", metadata, col_movie_id, col_title, col_length, col_rate, col_release_date, col_genres)

All parameters to define a Column

	Column(column_name, data_type, primary_key=False, nullable=True, default=None)

All parameters to define a Table
	
	Table(table_name, metadata, column1, column2, ...)

###Create all tables in database
	
	metadata.create_all(engine) # create all table for database

###Insert

There are two type of data can be insert to database.

1. record, an immutable tuple represents all columns.
	
		record = ("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"})
	    records = [("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
	               ("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"}),
	               ("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"}),
	               ("m0004", "12 Angry Men", 96, 8.9, date(1957, 4, 11), {"Drama"}),
	               ("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"}),]

2. row, a OrderedDict like object. values can be accessed via row.title or row["title"].

		row = Row( 
			("movie_id", "title", "length", "rate", "release_date", "genres"),
			("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"}),
			)

	    rows = [
			Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
				("m0001", "The Shawshank Redemption", 142, 9.2, date(1994, 10, 14), {"Drama", "Crime"})),
			Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
				("m0002", "The Godfather", 175, 9.2, date(1972, 3, 24), {"Crime", "Drama"})),
			Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
				("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"})),
			Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
				("m0003", "The Dark Knight", 152, 8.9, date(2008, 7, 18), {"Action", "Crime", "Drama"})),
			Row(("movie_id", "title", "length", "rate", "release_date", "genres"),
				("m0005", "Schindler's List", 195, 8.9, date(1994, 2, 4), {"Biography", "Drama", "History"})),
			]

There are two type of insert method to database.

1. insert, insert one commit one.
2. insert many, insert all and commit at the end. **Support [generator object](https://docs.python.org/2/library/stdtypes.html#generator-types)**, which means the memory usage is only one row of data in the whole insert process.

**bonus:** insert and update, try insert, if failed, then update the corresponding row. This feature has to work together with Update.

####Define a insert object

	ins = movie.insert()

####Four insert method

	engine.insert_record(ins, record) # insert one record
	engine.insert_many_records(ins, records) # insert many records
	engine.insert_row(ins, row) # insert one row
	engine.insert_many_rows(ins, rows) # insert many rows
	