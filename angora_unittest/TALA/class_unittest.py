##encoding=utf8

from __future__ import print_function
from angora.TALA.tala import FieldType, Field, Schema, SearchEngine

class MyClass():
    """a user customized class stored with document
    """
    def __init__(self, value):
        self.value = value

    def __repr__(self):
        return "MyClass(%s)" % repr(self.value)
    
if __name__ == "__main__":
    fieldtype = FieldType()
    _movie_id = Field("movie_id", fieldtype.Searchable_UUID, primary_key=True)
    _title = Field("title", fieldtype.Searchable_TEXT)
    _year = Field("year", fieldtype.Searchable_INTEGER)
    _length = Field("length", fieldtype.Searchable_INTEGER)
    _rating = Field("rating", fieldtype.Searchable_REAL)
    _votes = Field("votes", fieldtype.Searchable_INTEGER)
    _genres = Field("genres", fieldtype.Searchable_KEYWORD)
    _other = Field("other", fieldtype.Unsearchable_OBJECT)
    
    movie_schema = Schema("movie",
        _movie_id,
        _title,
        _year,
        _length,
        _rating,
        _votes,
        _genres,
        _other,
        )
    
    engine = SearchEngine(movie_schema)
    
    def FieldType_unittest():
        print(fieldtype.Searchable_ID)
        print(repr(fieldtype.Searchable_ID))
        print(fieldtype.Searchable_KEYWORD)
        print(repr(fieldtype.Searchable_KEYWORD))
        print(fieldtype.Unsearchable_OBJECT)
        print(repr(fieldtype.Unsearchable_OBJECT))
        
#     FieldType_unittest()
    
    def Field_unittest():
        print(_movie_id.field_name)
        print(repr(_movie_id))
        print(_genres.field_name)
        print(repr(_genres))
        print(_other.field_name)
        print(repr(_other))
        
#     Field_unittest()

    def Schema_unittest():
        print(movie_schema)
        print(repr(movie_schema))
        movie_schema.prettyprint()
        
#     Schema_unittest()

    def Engine_add_one_unittest():
        documents = [
         {"movie_id": "m001", "title": "The Shawshank Redemption", "year": 1994, "length": 142, 
          "rating": 9.2, "votes": 12994883, "genres": {"Drama", "Crime"}, "other": MyClass(1000)},
         {"movie_id": "m003", "title": "The Dark Knight", "year": 2008, "length": 152, 
          "rating": 8.9, "votes": 5873621, "genres": {"Action", "Crime", "Drama"}, "other": MyClass(2000)},
         ]
        
        for document in documents:
            engine.add_one(document)
            
        engine.engine.prt_all(engine.get_table("movie"))
        engine.engine.prt_all(engine.get_table("genres"))
        
#     Engine_add_one_unittest()

    def Engine_add_all_unittest():
        documents = [
         {"movie_id": "m001", "title": "The Shawshank Redemption", "year": 1994, "length": 142, 
          "rating": 9.2, "votes": 12994883, "genres": {"Drama", "Crime"}, "other": MyClass(1000)},
         {"movie_id": "m003", "title": "The Dark Knight", "year": 2008, "length": 152, 
          "rating": 8.9, "votes": 5873621, "genres": {"Action", "Crime", "Drama"}, "other": MyClass(2000)},
         ]
        
        engine.clone_from_data_stream(documents)
        
        engine.engine.prt_all(engine.get_table("movie"))
        engine.engine.prt_all(engine.get_table("genres"))
        
#     Engine_add_all_unittest()
    
    def Query_unittest():
        query = engine.create_query()
        query.renew_with(query.query_between("rating", 0.0, 10.0))
        query.order_by(["year"], ["DESC"])
        main_sqlcmd, main_sqlcmd_select_all, keyword_sqlcmd_list = query.create_sql()
        print(main_sqlcmd)
        
    Query_unittest()