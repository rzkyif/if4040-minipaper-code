import faker, random, faker_education, time, os, pickle

# ---------------------------------------------------------------------------- #
#                              Global Faker Object                             #
# ---------------------------------------------------------------------------- #

FAKE = faker.Faker()
FAKE.add_provider(faker_education.SchoolProvider)
RANDOM = random.Random()

# ---------------------------------------------------------------------------- #
#                                Data Structure                                #
# ---------------------------------------------------------------------------- #

class Data:
  def __generate_users__(self):
    generated_names = set()
    for i in range(self.scale):
      name = FAKE.name()
      while name in generated_names:
        name = FAKE.name()
      generated_names.add(name)
      self.users.append({
        "name": name,
        "email": FAKE.email(),
        "phone_number": FAKE.phone_number(),
        "birth_date": FAKE.date_of_birth()
      })

  def __generate_companies__(self):
    generated_names = set()
    for i in range(self.scale):
      name = FAKE.company()
      while name in generated_names:
        name = FAKE.company()
      generated_names.add(name)
      self.companies.append({
        "name": name
      })

  def __generate_institutions__(self):
    generated_names = set()
    for i in range(self.scale):
      name = FAKE.school_name()
      while name in generated_names:
        name = FAKE.school_name()
      generated_names.add(name)
      self.institutions.append({
        "name": name
      })

  def __generate_connections__(self):
    connection_count = [0 for i in range(self.scale+1)]
    connection_target = [self.avg_connection+RANDOM.randint(-self.pm_connection,self.pm_connection) for i in range(self.scale+1)]
    connection_set = set()
    for a in range(1,self.scale+1):
      while connection_count[a] < connection_target[a]:
        b = random.randint(1,self.scale)
        while (a,b) in connection_set or (b,a) in connection_set:
          b = random.randint(1,self.scale)
        self.connections.append({
          "user_id_a": a,
          "user_id_b": b,
          "start_date": FAKE.date()
        })
        connection_set.add((a,b))
        connection_count[a] += 1
        connection_count[b] += 1

  def __generate_employments__(self):
    employment_count = [0 for i in range(self.scale+1)]
    employment_target = [self.avg_employment+RANDOM.randint(-self.pm_employment,self.pm_employment) for i in range(self.scale+1)]
    employment_set = set()
    for a in range(1,self.scale+1):
      while employment_count[a] < employment_target[a]:
        b = random.randint(1,self.scale)
        while (a,b) in employment_set:
          b = random.randint(1,self.scale)
        sd = FAKE.date()
        ed = FAKE.date()
        if ed < sd:
          (sd, ed) = (ed, sd)
        self.employments.append({
          "user_id": a,
          "company_id": b,
          "start_date": sd,
          "end_date": ed,
          "role": FAKE.job()
        })
        employment_set.add((a,b))
        employment_count[a] += 1

  def __generate_educations__(self):
    education_count = [0 for i in range(self.scale+1)]
    education_target = [self.avg_education+RANDOM.randint(-self.pm_education,self.pm_education) for i in range(self.scale+1)]
    education_set = set()
    for a in range(1,self.scale+1):
      while education_count[a] < education_target[a]:
        b = random.randint(1,self.scale)
        while (a,b) in education_set:
          b = random.randint(1,self.scale)
        sd = FAKE.date()
        ed = FAKE.date()
        if ed < sd:
          (sd, ed) = (ed, sd)
        self.educations.append({
          "user_id": a,
          "institution_id": b,
          "start_date": sd,
          "end_date": ed,
          "degree": FAKE.school_type()
        })
        education_set.add((a,b))
        education_count[a] += 1

  def __init__(self, scale=100, avg_connection=20, pm_connection=15, avg_employment=3, pm_employment=2, avg_education=3, pm_education=2) -> None:
    self.users = [None]
    self.connections = []
    self.companies = [None]
    self.employments = []
    self.institutions = [None]
    self.educations = []

    self.configuration = {
      "scale": scale, 
      "avg_connection": avg_connection, 
      "pm_connection": pm_connection, 
      "avg_employment": avg_employment, 
      "pm_employment": pm_employment, 
      "avg_education": avg_education, 
      "pm_education": pm_education
    }
    self.scale = scale
    self.avg_connection = avg_connection
    self.pm_connection = pm_connection
    self.avg_employment = avg_employment
    self.pm_employment = pm_employment
    self.avg_education = avg_education
    self.pm_education = pm_education

    self.__generate_users__()
    self.__generate_companies__()
    self.__generate_institutions__()
    self.__generate_connections__()
    self.__generate_employments__()
    self.__generate_educations__()

  def __str__(self) -> str:
    res = ""
    res += "Users:\n"
    for u in self.users:
      if u is None: continue
      res += f"  [{u['name']}, {u['email']}, {u['phone_number']}, {u['birth_date']}]\n"
    res += "Companies:\n"
    for c in self.companies:
      if c is None: continue
      res += f"  [{c['name']}]\n"
    res += "Institutions:\n"
    for i in self.institutions:
      if i is None: continue
      res += f"  [{i['name']}]\n"
    res += "Connections:\n"
    for c in self.connections:
      res += f"  [{self.users[c['user_id_a']]['name']} <-> {self.users[c['user_id_b']]['name']}]\n"
    res += "Employments:\n"
    for e in self.employments:
      res += f"  [{self.users[e['user_id']]['name']} --> {self.companies[e['company_id']]['name']} ({e['role']})]\n"
    res += "Educations:\n"
    for e in self.educations:
      res += f"  [{self.users[e['user_id']]['name']} --> {self.institutions[e['institution_id']]['name']} ({e['degree']})]\n"
    return res

  def as_dict(self):
    return {
      'configuration': self.configuration,
      'users': self.users,
      'companies': self.companies,
      'institutions': self.institutions,
      'connections': self.connections,
      'employments': self.employments,
      'educations': self.educations
    }

# ---------------------------------------------------------------------------- #
#                        Cached Data Generator Function                        #
# ---------------------------------------------------------------------------- #

def generate(scale=10000, avg_connection=5, pm_connection=2, avg_employment=5, pm_employment=3, avg_education=3, pm_education=2):
  if os.path.exists("data.pickle"):
    configuration = {
      "scale": scale, 
      "avg_connection": avg_connection, 
      "pm_connection": pm_connection, 
      "avg_employment": avg_employment, 
      "pm_employment": pm_employment, 
      "avg_education": avg_education, 
      "pm_education": pm_education
    }

    valid = True

    stored_data = {}
    with open("data.pickle", 'rb') as f:
      stored_data = pickle.load(f)
      for key in configuration:
        if stored_data['configuration'][key] != configuration[key]:
          valid = False
          break
  
    if valid:
      return stored_data

  generated_data = Data(scale, avg_connection, pm_connection, avg_employment, pm_employment, avg_education, pm_education).as_dict()
  
  with open("data.pickle", "wb") as f:
    pickle.dump(generated_data, f)

  return generated_data

# ---------------------------------------------------------------------------- #
#                              Test Main Function                              #
# ---------------------------------------------------------------------------- #

if __name__ == "__main__":
  start = time.perf_counter()

  data = generate()

  print(f"Done generating data! ({time.perf_counter()-start}s)")
