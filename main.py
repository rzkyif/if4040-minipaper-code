import argparse
import psycopg2
from os import getenv as env
from dotenv import load_dotenv
from py2neo import Graph
from generator import generate
from time import perf_counter

# ---------------------------------------------------------------------------- #
#                                Available Tasks                               #
# ---------------------------------------------------------------------------- #

# -------------------------------- PostgreSQL -------------------------------- #

def g_p(conn, data):
  """Generate and insert dummy data for PostgreSQL
  """
  t = perf_counter()

  # insertion code here

  print(f"Data insertion complete! ({perf_counter()-t}s)")

def t_p_1(conn):
  """Test query 1 for PostgreSQL
  """
  t = perf_counter()

  # query 1 code here

  print(f"PostgreSQL query 1 complete! ({perf_counter()-t}s)")


def t_p_2(conn):
  """Test query 2 for PostgreSQL
  """
  t = perf_counter()

  # query 2 code here

  print(f"PostgreSQL query 2 complete! ({perf_counter()-t}s)")

def t_p_3(conn):
  """Test query 3 for PostgreSQL
  """
  t = perf_counter()

  # query 3 code here

  print(f"PostgreSQL query 3 complete! ({perf_counter()-t}s)")

def t_p(conn):
  """Test all query for PostgreSQL
  """
  t = perf_counter()

  t_p_1(conn)
  t_p_2(conn)
  t_p_3(conn)

  print(f"PostgreSQL query 1, 2, and 3 complete! ({perf_counter()-t}s)")

# ----------------------------------- Neo4J ---------------------------------- #

def g_n(graph, data):
  """Generate and insert dummy data for Neo4j
  """
  t = perf_counter()

  # insertion code here

  print(f"Data insertion complete! ({perf_counter()-t}s)")

def t_n_1(graph):
  """Test query 1 for Neo4j
  """
  t = perf_counter()

  # query 1 code here

  print(f"Neo4J query 1 complete! ({perf_counter()-t}s)")

def t_n_2(graph):
  """Test query 2 for Neo4j
  """
  t = perf_counter()

  # query 2 code here

  print(f"Neo4J query 2 complete! ({perf_counter()-t}s)")

def t_n_3(graph):
  """Test query 3 for Neo4j
  """
  t = perf_counter()

  # query 3 code here

  print(f"Neo4J query 3 complete! ({perf_counter()-t}s)")

def t_n(graph):
  """Test all query for Neo4j
  """
  t = perf_counter()

  t_n_1(conn)
  t_n_2(conn)
  t_n_3(conn)

  print(f"Neo4J query 1, 2, and 3 complete! ({perf_counter()-t}s)")

# ---------------------------------------------------------------------------- #
#                                 Main Function                                #
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
  parser = argparse.ArgumentParser()
  parser.add_argument(
    'task', metavar='task',
    help="What task to do. See available functions in main.py."
  )
  args = parser.parse_args()

  load_dotenv()

  conn = None;
  if "_p" in args.task:
    conn = psycopg2.connect(f"user='{env('POSTGRE_USER')}' password='{env('POSTGRE_PASS')}' host='{env('POSTGRE_HOST')}' port='5432'")
  elif "_n" in args.task:
    conn = Graph(f"bolt://{env('NEO4J_HOST')}", auth=(env('NEO4J_USER'), env('NEO4J_PASS')))

  data = None;
  if "g" in args.task:
    data = generate()
    
    if args.task in globals():
      globals()[args.task](conn, data)
    else:
      print('Task not available!')
  elif "t" in args.task:
    if args.task in globals():
      globals()[args.task](conn)
    else:
      print('Task not available!')
