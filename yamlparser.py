import json
import yaml
from yaml import SafeLoader
yaml_string="""employee:
  name: John Doe
  age: 35
  job:
    title: Software Engineer
    department: IT
    years_of_experience: 10
  address:
    street:
      meetzzz: derp
      array:
        - a
        - b
        - c

    city: San Francisco
    state: CA
    zip: 94102
    colorsa:
      - red
      - blue
      - green
"""
print("The YAML string is:")
print(yaml_string)
python_dict=yaml.load(yaml_string, Loader=SafeLoader)
json_string=json.dumps(python_dict)
print("The JSON string is:")
print(json_string)

json_elegant = json.loads(json_string)
emp = json_elegant['employee']['name']
print(emp)