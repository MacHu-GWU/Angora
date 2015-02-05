##encoding=utf8

from __future__ import print_function
from angora.TALA.tala import FieldType, Field, Schema, SearchEngine
import pandas as pd

def document_generator():
    """extract documents from raw data, and add documents to engine
    """
    df = pd.read_csv("movies.tab", sep="\t")
    for index, row in df.iterrows():
        document = dict()
        document["movie_id"] = str(index)
        document["title"] = row["title"]
        document["year"] = row["year"]
        document["length"] = row["length"]
        document["votes"] = row["votes"]
        genres = set()
        if row["Action"]:
            genres.add("Action")
        if row["Animation"]:
            genres.add("Animation")
        if row["Comedy"]:
            genres.add("Comedy")
        if row["Drama"]:
            genres.add("Drama")
        if row["Documentary"]:
            genres.add("Documentary")
        if row["Romance"]:
            genres.add("Romance")
        if row["Short"]:
            genres.add("Short")
        document["genres"] = genres
        yield document
        
fieldtype = FieldType()
movie_schema = Schema("movie",
    Field("movie_id", fieldtype.Searchable_UUID, primary_key=True),
    Field("title", fieldtype.Searchable_TEXT),
    Field("year", fieldtype.Searchable_INTEGER),
    Field("length", fieldtype.Searchable_INTEGER),
    Field("rating", fieldtype.Searchable_REAL),
    Field("votes", fieldtype.Searchable_INTEGER),
    Field("genres", fieldtype.Searchable_KEYWORD),
    )

engine = SearchEngine(movie_schema) # 13.27秒
# engine.clone_from_data_stream(document_generator())
# engine.engine.commit()

query = engine.create_query()
query.add(query.query_like("title", "walking"))
query.add(query.query_between("year", 1993, 2000)) # year between 1993, 2000
query.add(query.query_contains("genres", "Drama", "Romance")) # genres contains Drama

results = engine.search_document(query)
for document in results:
    print(document)
