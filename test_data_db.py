import chromadb
import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE

chroma_client = chromadb.HttpClient(host='localhost', port=8000)
chroma_collection = chroma_client.get_collection("recipes")
postgres_client = psycopg2.connect(
  dbname='mcp_tutorial_data',
  user='mcp_tutorial_data_db_user', 
  host='localhost',
  password='mcp_tutorial_data_db_password'
)
postgres_client.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
postgres_cursor = postgres_client.cursor()

# find the some data as test in the chroma DB
results = chroma_collection.query(
  query_texts=["give me a fast mexican tacco recipe"],
  n_results=3
  # where={"metadata_field": "is_equal_to_this"}, # optional filter
  # where_document={"$contains":"search_string"}  # optional filter
)
print("first 3 entries for search fast mexican tacco recipes in chroma DB")
print(results)


# find the some data as test in the postgres DB
select_command = "SELECT * FROM RECIPES;"
postgres_cursor.execute(sql.SQL(select_command))
row = postgres_cursor.fetchone()
print("first entry for select in postgrest DB")
for index, val in enumerate(row):
  print(f"column {index} value {val} of type {type(val)}")
print(row)
