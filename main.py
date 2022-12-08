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
  """Insert dummy data for PostgreSQL
  """
  t = perf_counter()

  if input("Are you sure you want to rebuild the PostgreSQL database? (y/n): ") != 'y': return

  c = conn.cursor()

  print('Starting data insertion for PostgreSQL!')
  print('  Deleting previous data...')
  c.execute('''
    DO
    $do$
    DECLARE
      _tbl text;
    BEGIN
    FOR _tbl  IN
        SELECT quote_ident(table_schema) || '.'
            || quote_ident(table_name)
        FROM   information_schema.tables
        WHERE  table_name LIKE 'test_' || '%'
        AND    table_schema NOT LIKE 'pg\_%'
    LOOP
      EXECUTE
      'DROP TABLE ' || _tbl || ' CASCADE';
    END LOOP;
    END
    $do$;
  ''')
  print(f'  Populating user table ({len(data["users"])-1})...')
  c.execute('''
    CREATE TABLE test_user (
      user_id SERIAL PRIMARY KEY,
      name TEXT,
      email TEXT,
      phone_number TEXT,
      birth_date DATE
    )
  ''')
  c.executemany('''
    INSERT INTO test_user VALUES (%s, %s, %s, %s, %s)
  ''', [(i, user['name'], user['email'], user['phone_number'], user['birth_date']) for i, user in enumerate(data['users']) if user is not None])
  print(f'  Populating company table ({len(data["companies"])-1})...')
  c.execute('''
    CREATE TABLE test_company (
      company_id SERIAL PRIMARY KEY,
      name TEXT
    )
  ''')
  c.executemany('''
    INSERT INTO test_company VALUES (%s, %s)
  ''', [(i, company['name']) for i, company in enumerate(data['companies']) if company is not None])
  print(f'  Populating institution table ({len(data["institutions"])-1})...')
  c.execute('''
    CREATE TABLE test_institution (
      institution_id SERIAL PRIMARY KEY,
      name TEXT
    )
  ''')
  c.executemany('''
    INSERT INTO test_institution VALUES (%s, %s)
  ''', [(i, institution['name']) for i, institution in enumerate(data['institutions']) if institution is not None])
  print(f'  Populating connection table ({len(data["connections"])})...')
  c.execute('''
    CREATE TABLE test_connection (
      user_id_a INT REFERENCES test_user(user_id),
      user_id_b INT REFERENCES test_user(user_id),
      date_start DATE,
      PRIMARY KEY (user_id_a, user_id_b)
    )
  ''')
  c.executemany('''
    INSERT INTO test_connection VALUES (%s, %s, %s)
  ''', [(x['user_id_a'], x['user_id_b'], x['start_date']) for x in data['connections']])
  print(f'  Populating employment table ({len(data["employments"])})...')
  c.execute('''
    CREATE TABLE test_employment (
      user_id INT REFERENCES test_user(user_id),
      company_id INT REFERENCES test_company(company_id),
      date_start DATE,
      date_end DATE,
      role TEXT,
      PRIMARY KEY (user_id, company_id, date_start, date_end, role)
    )
  ''')
  c.executemany('''
    INSERT INTO test_employment VALUES (%s, %s, %s, %s, %s)
  ''', [(x['user_id'], x['company_id'], x['start_date'], x['end_date'], x['role']) for x in data['employments']])
  print(f'  Populating education table ({len(data["educations"])})...')
  c.execute('''
    CREATE TABLE test_education (
      user_id INT REFERENCES test_user(user_id),
      institution_id INT REFERENCES test_institution(institution_id),
      date_start DATE,
      date_end DATE,
      degree TEXT,
      PRIMARY KEY (user_id, institution_id, date_start, date_end, degree)
    )
  ''')
  c.executemany('''
    INSERT INTO test_education VALUES (%s, %s, %s, %s, %s)
  ''', [(x['user_id'], x['institution_id'], x['start_date'], x['end_date'], x['degree']) for x in data['educations']])
  conn.commit()

  print(f"Data insertion complete! ({perf_counter()-t}s)")

def t_p_1(conn):
  """Test query 1 for PostgreSQL
  """
  t = perf_counter()
  c = conn.cursor()

  COUNT = 1000
  print(f"Starting test 1 for PostgreSQL! ({COUNT})")
  
  c.execute('''
    SELECT name FROM test_user WHERE user_id = 1
  ''')
  (initial_name,) = c.fetchone()
  print(f"  Initial name: {initial_name}")
  print(f"  Running {COUNT} flip-flop queries...")
  for _ in range(COUNT):
    c.execute('''
      SELECT name FROM test_user WHERE user_id = 1
    ''')
    (name,) = c.fetchone()
    if name == 'TEST':
      c.execute('''
        UPDATE test_user SET name = %s WHERE user_id = 1
      ''', (initial_name,))
    else:
      c.execute('''
        UPDATE test_user SET name = 'TEST' WHERE user_id = 1
      ''')
    conn.commit()
  
  c.execute('''
    SELECT name FROM test_user WHERE user_id = 1
  ''')
  (name,) = c.fetchone()
  print(f"  Final name: {name}")

  print(f"PostgreSQL query 1 complete! ({perf_counter()-t}s)")


def t_p_2(conn):
  """Test query 2 for PostgreSQL
  """
  t = perf_counter()
  c = conn.cursor()

  COUNT = 1000
  print(f"Starting test 2 for PostgreSQL! ({COUNT})")

  print(f"  Running {COUNT} profile queries...")
  for _ in range(COUNT):
    c.execute('''
      SELECT * FROM test_user WHERE user_id = 1
    ''')
    c.execute('''
      SELECT * FROM test_connection, test_user 
      WHERE (
        test_connection.user_id_a = 1
        AND test_connection.user_id_b = test_user.user_id
      ) OR (
        test_connection.user_id_b = 1
        AND test_connection.user_id_a = test_user.user_id
      )
    ''')
    c.execute('''
      SELECT * FROM test_employment, test_company 
      WHERE test_employment.user_id = 1
      AND test_employment.company_id = test_company.company_id
    ''')
    c.execute('''
      SELECT * FROM test_education, test_institution 
      WHERE test_education.user_id = 1
      AND test_education.institution_id = test_institution.institution_id
    ''')

  print(f"PostgreSQL query 2 complete! ({perf_counter()-t}s)")

def t_p_3(conn):
  """Test query 3 for PostgreSQL
  """
  t = perf_counter()
  c = conn.cursor()

  COUNT = 1000
  print(f"Starting test 3 for PostgreSQL! ({COUNT}))")

  print(f"  Running {COUNT} complex quer{'y' if COUNT == 1 else 'ies'}...")
  for id in range(1,COUNT+1):
    c.execute('''
      (
        SELECT cb.user_id_b 
          FROM 
            test_connection AS cb
            INNER JOIN test_connection AS ca ON ca.user_id_b = cb.user_id_a
          WHERE
            ca.user_id_a = %(id)s
        UNION
        SELECT ca.user_id_a
          FROM 
            test_connection AS ca
            INNER JOIN test_connection AS cb ON cb.user_id_a = ca.user_id_b
          WHERE
            cb.user_id_b = %(id)s
        UNION
        SELECT cb.user_id_a 
          FROM 
            test_connection AS cb
            INNER JOIN test_connection AS ca ON ca.user_id_b = cb.user_id_b
          WHERE
            ca.user_id_a = %(id)s
        UNION
        SELECT ca.user_id_b
          FROM 
            test_connection AS ca
            INNER JOIN test_connection AS cb ON cb.user_id_a = ca.user_id_a
          WHERE
            cb.user_id_b = %(id)s
      )
      EXCEPT
      (
        SELECT user_id_b FROM test_connection WHERE user_id_a = %(id)s
        UNION
        SELECT user_id_a FROM test_connection WHERE user_id_b = %(id)s
      )
    ''', {'id': id})

  print(f"PostgreSQL query 3 complete! ({perf_counter()-t}s)")

def t_p(conn):
  """Test all query for PostgreSQL
  """
  t = perf_counter()

  for _ in range(10):
    t_p_1(conn)
  for _ in range(10):
    t_p_2(conn)
  for _ in range(10):
    t_p_3(conn)

  print(f"PostgreSQL query 1, 2, and 3 complete! ({perf_counter()-t}s)")

# ----------------------------------- Neo4J ---------------------------------- #

def g_n(graph: Graph, data):
  """Insert dummy data for Neo4j
  """
  t = perf_counter()

  if input("Are you sure you want to rebuild the Neo4j database? (y/n): ") != 'y': return

  print('Starting data insertion for PostgreSQL!')
  print('  Deleting previous data...')
  graph.delete_all()
  print(f'  Populating user nodes ({len(data["users"])-1})...')
  for id, user in enumerate(data["users"]):
    if user is not None:
      graph.update("CREATE (u:User {user_id: $user_id, name: $name, email: $email, phone_number: $phone_number, birth_date: $birth_date})", {"user_id": id, **user})
  print(f'  Creating user_id index ({len(data["users"])-1})...')
  graph.update("CREATE INDEX user_id FOR (u:User) ON (u.user_id)")
  print(f'  Populating company nodes ({len(data["companies"])-1})...')
  for id, company in enumerate(data["companies"]):
    if company is not None:
      graph.update("CREATE (c:Company {company_id: $company_id, name: $name})", {"company_id": id, **company})
  print(f'  Creating company_id index ({len(data["companies"])-1})...')
  graph.update("CREATE INDEX company_id FOR (c:Company) ON (c.company_id)")
  print(f'  Populating institution nodes ({len(data["institutions"])-1})...')
  for id, institution in enumerate(data["institutions"]):
    if institution is not None:
      graph.update("CREATE (i:Institution {institution_id: $institution_id, name: $name})", {"institution_id": id, **institution})
  print(f'  Creating institution_id index ({len(data["institutions"])-1})...')
  graph.update("CREATE INDEX institution_id FOR (i:Institution) ON (i.institution_id)")
  print(f'  Populating connection relationships ({len(data["connections"])})...')
  for x in data["connections"]:
    graph.update("MATCH (a:User), (b:User) WHERE a.user_id = $user_id_a AND b.user_id = $user_id_b CREATE (a)-[:CONNECTION {date_start: $start_date}]->(b)", x)
  print(f'  Populating employment relationships ({len(data["employments"])})...')
  for x in data["employments"]:
    graph.update("MATCH (u:User), (c:Company) WHERE u.user_id = $user_id AND c.company_id = $company_id CREATE (u)-[:EMPLOYMENT {date_start: $start_date, date_end: $end_date, role: $role}]->(c)", x)
  print(f'  Populating education relationships ({len(data["educations"])})...')
  for x in data["educations"]:
    graph.update("MATCH (u:User), (i:Institution) WHERE u.user_id = $user_id AND i.institution_id = $institution_id CREATE (u)-[:EDUCATION {date_start: $start_date, date_end: $end_date, degree: $degree}]->(i)", x)

  print(f"Data insertion complete! ({perf_counter()-t}s)")

def t_n_1(graph: Graph):
  """Test query 1 for Neo4j
  """
  t = perf_counter()

  COUNT = 1000
  print(f"Starting test 1 for Neo4j! ({COUNT})")
  
  initial_name = graph.evaluate("MATCH (u:User {user_id: 1}) RETURN u.name")
  print(f"  Initial name: {initial_name}")
  print(f"  Running {COUNT} flip-flop queries...")
  for _ in range(COUNT):
    name = graph.evaluate("MATCH (u:User {user_id: 1}) RETURN u.name")
    if name == 'TEST':
      graph.update('''
        MATCH (u:User {user_id: 1}) SET u.name = $name
      ''', {"name": initial_name})
    else:
      graph.update('''
        MATCH (u:User {user_id: 1}) SET u.name = $name
      ''', {"name": "TEST"})
  
  final_name = graph.evaluate("MATCH (u:User {user_id: 1}) RETURN u.name")
  print(f"  Final name: {final_name}")

  print(f"Neo4J query 1 complete! ({perf_counter()-t}s)")

def t_n_2(graph: Graph):
  """Test query 2 for Neo4j
  """
  t = perf_counter()

  COUNT = 1000
  print(f"Starting test 2 for Neo4j! ({COUNT})")

  print(f"  Running {COUNT} profile queries...")
  for _ in range(COUNT):
    graph.run('''
      MATCH (u:User {user_id: 1})-[co:CONNECTION]-(u2:User)
      RETURN u.name, co.date_start, u2.name
    ''')
    graph.run('''
      MATCH (u:User {user_id: 1})-[em:EMPLOYMENT]-(c:Company)
      RETURN u.name, em.date_start, em.date_end, em.role, c.name
    ''')
    graph.run('''
      MATCH (u:User {user_id: 1})-[ed:EDUCATION]-(i:Institution)
      RETURN u.name, ed.date_start, ed.date_end, ed.degree, i.name
    ''')

  print(f"Neo4J query 2 complete! ({perf_counter()-t}s)")

def t_n_3(graph: Graph):
  """Test query 3 for Neo4j
  """
  t = perf_counter()

  COUNT = 1000
  print(f"Starting test 3 for Neo4j! ({COUNT}))")

  print(f"  Running {COUNT} complex quer{'y' if COUNT == 1 else 'ies'}...")
  for id in range(1,COUNT+1):
    graph.run('''
      MATCH (u:User {user_id: $user_id})-[:CONNECTION*2]-(u2:User)
      WHERE NOT ((u)-[:CONNECTION]-(u2))
      RETURN u2.user_id
    ''', {"user_id": id})

  print(f"Neo4J query 3 complete! ({perf_counter()-t}s)")

def t_n(graph: Graph):
  """Test all query for Neo4j
  """
  t = perf_counter()

  for _ in range(10):
    t_n_1(graph)
  for _ in range(10):
    t_n_2(graph)
  for _ in range(10):
    t_n_3(graph)


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

  conn = None
  if "_p" in args.task:
    conn = psycopg2.connect(f"user='{env('POSTGRE_USER')}' password='{env('POSTGRE_PASS')}' host='{env('POSTGRE_HOST')}' port='5432'")
  elif "_n" in args.task:
    conn = Graph(f"bolt://{env('NEO4J_HOST')}", auth=(env('NEO4J_USER'), env('NEO4J_PASS')))

  data = None
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
