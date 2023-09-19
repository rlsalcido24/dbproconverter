# Databricks notebook source
### this cell instantiates the widgets. The dynamic sql notebooks/files get written to dbtroot or notebook root/notebookdir. In MTJ mode jobname controls the MTJ job name. yamlfile controls the input yml (derived from a sproc) and integrationtest runs tests based on known test yml files. 

dbutils.widgets.text("notebookroot", "/Users/roberto.salcido@databricks.com/StoredProcedureTasks")
dbutils.widgets.text("notebookdir", "mixtres")
dbutils.widgets.text("jobname", "procjob")
dbutils.widgets.dropdown("yamlfile", "mix", ["mix", "true", "false"])
dbutils.widgets.dropdown("mode", "mtj", ["mtj", "dbt"] )
dbutils.widgets.text("dbtroot", "dbtroot")
dbutils.widgets.text("integrationtest", "false")


# COMMAND ----------

jobname = dbutils.widgets.get("jobname")
notebookroot = dbutils.widgets.get("notebookroot")
nbpath = dbutils.widgets.get("notebookdir")
yamlfile = dbutils.widgets.get("yamlfile")
mode = dbutils.widgets.get("mode")
dbtroot = dbutils.widgets.get("dbtroot")
integrationtest = dbutils.widgets.get("integrationtest")


# COMMAND ----------

### this cell imports relevant libraries and loads in the input yml file as a dictionary, which is then leveraged for downstream functions.

import json
import yaml
from yaml import SafeLoader
import base64
import requests
import pandas as pd
from requests.auth import HTTPBasicAuth

rstest = open('/Workspace/Repos/roberto.salcido@databricks.com/dbproconverter/{}.yml'.format(yamlfile), 'r')
python_dict=yaml.load(rstest, Loader=SafeLoader)
json_string=json.dumps(python_dict)
#print("The JSON string is:")
#print(json_string)
json = json.loads(json_string)
testdict = json
print(testdict)

# COMMAND ----------

def createdbxnotebooks(arraydicts):
  resultarray = []
  for (dicts) in (arraydicts):
    subset = dicts.get('sql')
    if subset['skip'] == True:
      results = dicts.get('name')
      resultarray.append(results)
    elif subset['skip'] == False:
      nbcontent = gencontent(subset, 'no')
      nbcreationresults = callnotebookapi(nbcontent, dicts['name'])
      resultarray.append(nbcreationresults)
    elif subset['skip'] == 'maybe':
      nbcontent = gencontent(subset, 'maybe')
      nbcreationresults = callnotebookapi(nbcontent, dicts['name'])
      resultarray.append(nbcreationresults)
  return resultarray

# COMMAND ----------

def createdbtfiles(arraydicts):
  resultarray = []
  for (dicts) in (arraydicts):
    subset = dicts.get('sql')
    if subset['skip'] == True:
      results = dicts.get('name')
      resultarray.append(results)
    elif subset['skip'] == False:
      nbcontent = gencontentdbt(subset, 'no')
      nbcreationresults = callfilesapi(nbcontent, dicts['name'])
      resultarray.append(nbcreationresults)
    elif subset['skip'] == 'maybe':
      nbcontent = gencontentdbt(subset, 'maybe')
      nbcreationresults = callfilesapi(nbcontent, dicts['name'])
      resultarray.append(nbcreationresults)
  return resultarray      

# COMMAND ----------

def gencontent(subset, string):
  bull = string
  if string == 'no':
    nbstring = subset['noconditions']["text"]
    nbstringsource = "-- Databricks notebook source \n " + nbstring
    nbytes = nbstringsource.encode('ascii')
    nbcontent = base64.b64encode(nbytes)
    return nbcontent
  elif string == 'maybe':
    dynamicnbcontent = valconditionsql(subset['conditions'])
    nbstringsource = "-- Databricks notebook source \n " + dynamicnbcontent
    nbytes = nbstringsource.encode('ascii')
    nbcontent = base64.b64encode(nbytes)
    return nbcontent
  else:  
    print ('please enter valid params into the function')  



# COMMAND ----------

def gencontentdbt(subset, string):
  bull = string
  if string == 'no':
    nbstring = subset['noconditions']["text"]
    nbytes = nbstring.encode('ascii')
    nbcontent = base64.b64encode(nbytes)
    return nbcontent
  elif string == 'maybe':
    nbstring = valconditionsqldbt(subset['conditions'])
    nbytes = nbstring.encode('ascii')
    nbcontent = base64.b64encode(nbytes)
    return nbcontent
  else:  
    print ('please enter valid params into the function')

# COMMAND ----------

def valconditionsql(subsetconditions):
  getllaves = subsetconditions.keys()
  getkey = list(getllaves)[0]
  nestcondition = subsetconditions[getkey]
  if getkey == 'bool':
    sparkresult = spark.sql(nestcondition["text"])
    pdresult = sparkresult.toPandas()
    result = pdresult.iloc[0, 0]
    if result == True:
      trueval = nestcondition["true"]
      return trueval
    else:
      falseval = nestcondition["false"]
      return falseval
  elif getkey == 'notexist':
    try:
      result = spark.sql(nestcondition["text"])
    except:
      noexistval = nestcondition["true"]
      return noexistval
    else: 
       existval = nestcondition["false"]
       return existval
  elif getkey == 'tree':
    for count, treeconditions in enumerate(nestcondition):
      sparkresult = spark.sql(treeconditions["treedict"]["text"])
      pdresult = sparkresult.toPandas()
      result = pdresult.iloc[0, 0]
      if result == True:
        treeval = treeconditions["treedict"]["true"]
        return treeval
    treeval = subsetconditions["catchall"]["text"]
    return treeval
  else:
    print(getkey)
    print('Please fix your YAML file according to the spec')

# COMMAND ----------

def valconditionsqldbt(subsetconditions):
  getllaves = subsetconditions.keys()
  getkey = list(getllaves)[0]
  nestcondition = subsetconditions[getkey]
  if getkey == 'bool':
    initarray = []
    conditionalstring = nestcondition["text"]
    initstring = '{% if execute %} \n'
    setstring = "{{% set results = run_query('{}') %}} \n".format(conditionalstring)
    truestring = '{% if results == True %} \n'
    trueval = nestcondition["true"] 
    elstring = '\n{% else %} \n'
    falseval = nestcondition["false"]
    endifstring = '\n{% endif %}'
    initarray.append(initstring + setstring + truestring + trueval + elstring + falseval + endifstring + endifstring)
    finalstring = "\n".join(initarray)
    return finalstring
   
  elif getkey == 'notexist':
    initarray = []
    conditionalstring = nestcondition["text"]
    initstring = '{% if execute %} \n'
    setstring = "{{% set results = run_query('{}') %}} \n".format(conditionalstring)
    existstring = '{% if results|length > 0 %} \n'
    existval = nestcondition["false"] 
    elstring = '\n{% else %} \n'
    noexistval = nestcondition["true"]
    endifstring = '\n{% endif %}'
    initarray.append(initstring + setstring + existstring + existval + elstring + noexistval + endifstring + endifstring)
    finalstring = "\n".join(initarray)
    return finalstring
       
  elif getkey == 'tree':
    initarray = []
    initstring = '{% if execute %} \n'
    endifstring = '{% endif %} \n'
    initarray.append(initstring)
    for count, treeconditions in enumerate(nestcondition):
      conditionalstring = treeconditions["treedict"]["text"]
      setstring = "{{% set results = run_query('{}') %}} \n".format(conditionalstring)
      truestring = '{% if results == True %} \n'
      trueval = treeconditions["treedict"]["true"] 
      endifstring = '\n{% endif %}'
      initarray.append(setstring + truestring + trueval + endifstring )
    
    initarray.append(endifstring)
    joinstring = "\n".join(initarray)
    return joinstring

  else:
    print(getkey)
    print('Please fix your YAML file according to the spec')    

# COMMAND ----------

### parameterize intitialcheck
def buildflow(arraydicts):
  initarray = []
  subset = [x for x in arraydicts if x['name'] == "initialcheck"]
  initarray.append(subset[0])
  proximo = valconditionflow(subset[0]['next'])
  while proximo != 'fin':
    nextnode = [x for x in arraydicts if x['name'] == proximo ] 
    initarray.append(nextnode[0])
    proximo = valconditionflow(nextnode[0]['next'])
  return initarray  


# COMMAND ----------

def valconditionflow(subset):
  getllaves = subset.keys()
  getkey = list(getllaves)[0]
  if getkey == 'noconditions':
    nextask = subset[getkey]["text"]
    return nextask
  elif getkey == 'conditions':
    nextask = valconditionsql(subset[getkey])
    return nextask 
  else:
    print('please fix your yaml')

# COMMAND ----------

### parameterize secret scopes
def callnotebookapi(nbcontent, dictname):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/import'
  un = 'roberto.salcido@databricks.com'
  pw = dbutils.secrets.get(scope="robertocreds", key="password")
  url = host
  pathname = dictname
  payload = {
"path": "{}/{}/{}".format(notebookroot, nbpath, pathname),
"format": "SOURCE",
"language": "SQL",
"content": nbcontent,
"overwrite": "true"
}
  skipdecode = base64.b64decode(nbcontent)
  skipcheck = skipdecode.decode('ascii')
  #print(skipcheck)
  if skipcheck == 'skip':
    results = 'no notebook created'
    return results
  else: 
    r = requests.post(url, auth=HTTPBasicAuth(un, pw), json=payload)
    results = r.json()
    return results

# COMMAND ----------

def callfilesapi(nbcontent, dictname):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/import'
  un = 'roberto.salcido@databricks.com'
  pw = dbutils.secrets.get(scope="robertocreds", key="password")
  url = host
  pathname = dictname
  payload = {
"path": "{}/{}/{}".format(notebookroot, nbpath, pathname),
"format": "AUTO",
"content": nbcontent,
"overwrite": "true"
}
  r = requests.post(url, auth=HTTPBasicAuth(un, pw), json=payload)
  results = r.json()
  return results    

# COMMAND ----------

def getsqlnb(path):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/get-status?path={}/{}/{}'.format(notebookroot, nbpath, path)
  un = 'roberto.salcido@databricks.com'
  pw = dbutils.secrets.get(scope="robertocreds", key="password")
  url = host
  r = requests.get(url, auth=HTTPBasicAuth(un, pw))
  results = r.json()
  return results

# COMMAND ----------

def gentaskpayload(flowarraydicts):
  tasks = []
  for flowdict in flowarraydicts:
    getnb = getsqlnb(flowdict['name'])
    try: 
      validpath = getnb["path"] 
    except:
      cow = 'moo' 
    else:
      taskdict = {   #"task_key": "create table {}".format(name),
               "task_key": "{}".format(flowdict['name']),
                "notebook_task": {
                    "notebook_path": "{}/{}/{}".format(notebookroot, nbpath, flowdict['name']),
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
       }
      tasks.append(taskdict)
    
  
  return tasks

# COMMAND ----------

def genselectoryml(flowarraydicts):
  uniontasks = []
  for flowdict in flowarraydicts:
    getnb = getsqlnb(flowdict['name'])
    try: 
      validpath = getnb["path"] 
    except:
      cow = 'moo' 
    else:
    
      taskdict = { 
               "method": "fqn",
               "value": "{}".format(flowdict['name']),
                      
       }
      uniontasks.append(taskdict)

  uniondict = {'union': uniontasks } 
  selectorarray = [{"name": "dynamicproc", 'defintion': uniondict}]
  selectorydictfinal = {"selectors": selectorarray}
  validyaml=yaml.dump(selectorydictfinal)
  selectoryaml = open('{}/selectors.yml'.format(dbtroot), 'w')
  selectoryaml.write(validyaml)
  selectoryaml.close()
  return uniontasks
  
   

# COMMAND ----------

def createmtj(jsonblob):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.1/jobs/create'
  un = 'roberto.salcido@databricks.com'
  pw = dbutils.secrets.get(scope="robertocreds", key="password")
  url = host
  payload = jsonblob
  r = requests.post(url, auth=HTTPBasicAuth(un, pw), json=payload)
  results = r.json()
  return(results)

# COMMAND ----------

def genfulljsonblob(payload):
  fulljsonblob = {
    "name": "{}".format(jobname),
    "email_notifications": {
            "no_alert_for_skipped_runs": "false"
        },
    "timeout_seconds": 0,
    "schedule": {
            "quartz_cron_expression": "58 45 14 * * ?",
            "timezone_id": "America/Los_Angeles",
            "pause_status": "UNPAUSED"
        },
    "max_concurrent_runs": 1,
    "creator_user_name": "roberto.salcido@databricks.com",
    "run_as_user_name": "roberto.salcido@databricks.com",
    "run_as_owner": "true",
    "tasks": payload,
    "job_clusters": [
            {
                "job_cluster_key": "Job_cluster",
                "new_cluster": {
                    "cluster_name": "",
                    "spark_version": "10.4.x-scala2.12",
                    "aws_attributes": {
                        "first_on_demand": 1,
                        "availability": "SPOT_WITH_FALLBACK",
                        "zone_id": "us-west-2a",
                        "spot_bid_price_percent": 100,
                        "ebs_volume_count": 0
                    },
                    "node_type_id": "i3.2xlarge",
                    "spark_env_vars": {
                        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
                    },
                    "enable_elastic_disk": "false",
                    "data_security_mode": "NONE",
                    "runtime_engine": "PHOTON",
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    }
                }
            }
        ],
        "format": "MULTI_TASK"
  }
  return fulljsonblob  

# COMMAND ----------

if mode == 'dbt':
  sqlnb = createdbtfiles(testdict)
  nodetree = buildflow(testdict)
  taskpayload = genselectoryml(nodetree)
  print(taskpayload)


else:
  sqlnb = createdbxnotebooks(testdict)
  nodetree = buildflow(testdict)
  taskpayload = gentaskpayload(nodetree)
  fulljsonblob = genfulljsonblob(taskpayload)
  genjob = createmtj(fulljsonblob)
  print(genjob)

# COMMAND ----------

### investigate assert
tasklength = len(taskpayload)
if integrationtest == 'true':
  if yamlfile == 'true':
      #assert tasklength == 13
    if tasklength == 5:
      sparkresult = spark.sql('select 1')
    else: 
      sparkresult = spark.sql('select zzz')
  elif yamlfile == 'mix':
      #assert tasklength == 5
    if tasklength == 12:
      sparkresult = spark.sql('select 1')
    else: 
      sparkresult = spark.sql('select zzz')
  elif yamlfile == 'false':
      #assert tasklength == 1
    if tasklength == 1:
      sparkresult = spark.sql('select 1')
    else: 
      sparkresult = spark.sql('select zzz')
  else: 
    sparkresult = spark.sql('select 1')        

