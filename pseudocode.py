# pyright: reportUndefinedVariable=false

# ---------------------------------------------------------------------------- #
#                           PSEUDOCODE FOR THE PAPER                           #
# ---------------------------------------------------------------------------- #

# Query 1

initial_name = get_name_by_id(1)

for i in range(1000):
  name = get_name_by_id(1)
  if name == 'TEST':
    set_name_by_id(1, initial_name)
  else:
    set_name_by_id(1, 'TEST')

final_name = get_name_by_id(1) 

# Query 2

for i in range(1000):
  get_user_by_id(1)
  get_connections_by_id(1)
  get_employments_by_id(1)
  get_education_by_id(1)

# Query 3

for i in range(1,1001):
  get_2nd_order_connections_by_id(i)