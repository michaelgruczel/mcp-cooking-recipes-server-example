import chromadb
import csv
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT # <-- ADD THIS LINE

# create chroma db client
chroma_client = chromadb.HttpClient(host='localhost', port=8000)
chroma_client.heartbeat()
# we ensure that a collection will be created and that it is empty
try:
  chroma_client.get_collection("recipes")
  chroma_client.delete_collection("recipes")
except Exception:
  # Collection does not exist
  print("Collection did not exist, that's fine")
chroma_client.create_collection("recipes")
chroma_collection = chroma_client.get_collection("recipes")

# create the postgres client
postgres_client = psycopg2.connect(
  dbname='mcp_tutorial_data',
  user='mcp_tutorial_data_db_user', 
  host='localhost',
  password='mcp_tutorial_data_db_password'
)
postgres_client.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) # <-- ADD THIS LINE
postgres_cursor = postgres_client.cursor()
# we ensure that a postgres table will be created and that it is empty
# name,id,minutes,contributor_id,submitted,tags,nutrition,n_steps,steps,description,ingredients,n_ingredients
# 0 name, 1 id, 2 minutes, 3 contributor_id, 4 submitted,
# 5 tags, 6 nutrition, 7 n_steps, 8 steps,
# 9 description, 10 ingredients, 11 n_ingredients
postgres_cursor.execute(sql.SQL("DROP TABLE IF EXISTS RECIPES;"))
postgres_cursor.execute(sql.SQL("CREATE TABLE RECIPES (id TEXT not null, name TEXT not null, minutes NUMERIC, n_steps NUMERIC, steps TEXT, ingredients TEXT, n_ingredients NUMERIC);"))


# the database uses l2 as default difference calculation, we can override that if needed e.g.
# collection = client.create_collection(name="recipes", metadata={"hnsw:space": "cosine"})

# add data from csv with recipes
with open('RAW_recipes.csv', newline='') as csv_file:
  csv_reader = csv.reader(csv_file, delimiter=',')
  line_count = 0
  for row in csv_reader:
    if line_count == 0:
      print(f'Column names are id{", ".join(row)}')
      line_count += 1
    else:
      # name,id,minutes,contributor_id,submitted,tags,nutrition,n_steps,steps,description,ingredients,n_ingredients
      # 0 name, 1 id, 2 minutes, 3 contributor_id, 4 submitted,
      # 5 tags, 6 nutrition, 7 n_steps, 8 steps,
      # 9 description, 10 ingredients, 11 n_ingredients


      n_steps = row[7]
      complexity = "simple"
      if n_steps:
        if int(n_steps) > 5 and  int(n_steps) < 10:
          complexity = "middle"
        elif int(n_steps) >= 10:
          complexity = "complex"

      minutes = row[2]
      duration = "normal"
      if n_steps:
        if int(minutes) > 15 and  int(n_steps) < 10:
          duration = "fast"

      #print("import recipe " + str(row[1]) + " with title '" + str(row[0]) + "' with complexity '" + complexity + "' and duration '" + duration + "'")

      text_to_index = str(row[0]) + str(row[8]) + str(row[9])
      chroma_collection.add(
        documents=[text_to_index],
        ids=[row[1]],
        metadatas=[{"complexity": complexity, "duration": duration}],
      )
      steps_text = row[8]
      steps_text = steps_text[1:-1]
      steps_text = steps_text.replace("'", "")
      ingedients_text = row[10]
      ingedients_text = ingedients_text[1:-1]
      ingedients_text = ingedients_text.replace("'", "")
      # print(steps_text)
      insert_text = "INSERT INTO RECIPES (id, name, minutes, n_steps, steps, ingredients, n_ingredients) values (" + str(row[1]) + ",'" + str(row[0]) + "'," + str(row[2]) + "," + str(row[7]) + ",'" + steps_text + "','" + ingedients_text + "'," + str(row[11]) + ");"
      #print(insert_test)
      postgres_cursor.execute(sql.SQL(insert_text))
      line_count += 1
      if (line_count % 100 == 0):
        print(str(line_count) + " processed...")

  print("...Data imported into vector DB and postres")

# find the some data as test in the chroma DB
results = chroma_collection.query(
  query_texts=["give me a fast mexican tacco recipe"],
  n_results=3
  # where={"metadata_field": "is_equal_to_this"}, # optional filter
  # where_document={"$contains":"search_string"}  # optional filter
)

print(results)
