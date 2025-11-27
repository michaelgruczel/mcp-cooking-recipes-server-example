import asyncio
import chromadb
from mcp.server.fastmcp import FastMCP, Context
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT 
import logging
from mcp.server.fastmcp.utilities.logging import get_logger

mcp = FastMCP("recipe_tools")

to_client_logger = get_logger(name="fastmcp.server.context.to_client")
to_client_logger.setLevel(level=logging.INFO)

# create chroma db client
chroma_client = chromadb.HttpClient(host='localhost', port=8000)
chroma_collection = chroma_client.get_collection("recipes")
# create the postgres client
postgres_client = psycopg2.connect(
  dbname='mcp_tutorial_data',
  user='mcp_tutorial_data_db_user', 
  host='localhost',
  password='mcp_tutorial_data_db_password'
)
postgres_client.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT) 
postgres_cursor = postgres_client.cursor()

@mcp.tool()
async def search_for_recipes(search_phrase: str, ctx: Context) -> list[str]:
    """Get recipe IDs by fitting to the search phrase"""
    await asyncio.sleep(1)
    # https://docs.trychroma.com/docs/querying-collections/query-and-get
    results = chroma_collection.query(
        query_texts=[search_phrase],
        n_results=1
        # where={"metadata_field": "is_equal_to_this"}, # optional filter
        # where_document={"$contains":"search_string"}  # optional filter
    )
    await ctx.info(f"search complete: {results}")
    search_result = [str(results["ids"][0][0])]
    return search_result

@mcp.tool()
async def get_recipe_by_id(
    recipe_id: str,
    ctx: Context
) -> str:
    """Get recipe by recipe ID"""
    await asyncio.sleep(1)
    select_command = "SELECT * FROM RECIPES WHERE id = '" + recipe_id + "' limit 1"
    await ctx.info(f"execute against DB {select_command}")
    postgres_cursor.execute(sql.SQL(select_command))
    row = postgres_cursor.fetchone()
    #for index, val in enumerate(row):
    #    await ctx.info(f"column {index} value {val} of type {type(val)}")
    #id, name, minutes, n_steps, steps, ingredients, n_ingredients
    await ctx.info(f"found {str(row)}")
    response = row[1] + " takes " + str(row[2]) + " minutes, follow this steps:" + row[4] + ", you need " + row[5]
    await ctx.info(f"response {response}")
    return response

if __name__ == "__main__":
    mcp.run(transport="stdio")
    #mcp.run(transport="http", host="127.0.0.1", port=8000)