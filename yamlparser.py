# Databricks notebook source
@StartTime = getdate()    
@unknown_CLIENT_ID      int,
@unknown_PRODUCT_ID     int,
@unknown_RATE_ID        int,
@unknown_RATE_ID_CAD    int,
@unknown_RATE_ID_USD    int,
@unknown_SALESPERSON_ID int,
@WarningCode 		    int,
@WarningMessage 		varchar(255)

@creation_dt datetime
@MAX_LAST_UPDATE_DATE datetime  
@id int
@id_max int 
@records int 
@rowcount int

@STRUC_RATE_PRODUCT_ID int -- v.2.0
@APPLICATION varchar(10), 
@APPLICATION_USE varchar(10)
@START_DT_V2 as date
@START_DT_V2 = '2015-03-15'
@ErrorCode = 0
@ErrorMessage = ''
@WithWarning = 0 
@WarningCode = 0,
@WarningMessage = ''
@EXTRACT_ID = ( 
    SELECT MAX(EXTRACT_ID) 
    FROM dbo.ETL_CONTROL_TABLE
    WHERE SOURCE_SYSTEM_CODE = @SOURCE_SYSTEM_CODE )

# COMMAND ----------

dbutils.widgets.text("notebookpath", "nbpath")
dbutils.widgets.text("jobname", "procjob")
dbutils.widgets.text("yamlfile", "mix")

# COMMAND ----------

nbpath = dbutils.widgets.get("notebookpath")
jobname = dbutils.widgets.get("jobname")
yamlfile = dbutils.widgets.get("yamlfile")

# COMMAND ----------

import json
import yaml
from yaml import SafeLoader
import base64
rstest = open('/Workspace/Repos/roberto.salcido@databricks.com/dbproconverter/{}.yml'.format(yamlfile), 'r')
python_dict=yaml.load(rstest, Loader=SafeLoader)
json_string=json.dumps(python_dict)
#print("The JSON string is:")
#print(json_string)
json = json.loads(json_string)
testdict = json
print(testdict)

# COMMAND ----------

def createsqlnotebooks(arraydicts):
  resultarray = []
  for (dicts) in (arraydicts):
    subset = dicts.get('sql')
    if subset['skip'] == True:
      results = dicts.get('name')
      resultarray.append(results)
    elif subset['skip'] == False:
      nbcontent = gencontent(subset, 'no')
      nbcreationresults = createsqlnb(nbcontent, dicts['name'])
      resultarray.append(nbcreationresults)
    elif subset['skip'] == 'maybe':
      nbcontent = gencontent(subset, 'maybe')
      nbcreationresults = createsqlnb(nbcontent, dicts['name'])
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

def createsqlnb(nbcontent, dictname):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/import'
  un = 'roberto.salcido@databricks.com'
  pw = dbutils.secrets.get(scope="robertocreds", key="password")
  url = host
  pathname = dictname
  payload = {
"path": "/Users/roberto.salcido@databricks.com/StoredProcedureTasks/{}/{}".format(nbpath, pathname),
"format": "SOURCE",
"language": "SQL",
"content": nbcontent,
"overwrite": "true"
}
  skipdecode = base64.b64decode(nbcontent)
  skipcheck = skipdecode.decode('ascii')
  print(skipcheck)
  if skipcheck == 'skip':
    results = 'no notebook created'
    return results
  else: 
    r = requests.post(url, auth=HTTPBasicAuth(un, pw), json=payload)
    results = r.json()
    return results

# COMMAND ----------

def getsqlnb(path):
  host = 'https://e2-demo-field-eng.cloud.databricks.com/api/2.0/workspace/get-status?path=/Users/roberto.salcido@databricks.com/StoredProcedureTasks/{}/{}'.format(nbpath, path)
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
                    "notebook_path": "/Users/roberto.salcido@databricks.com/StoredProcedureTasks/{}/{}".format(nbpath, flowdict['name']),
                    "source": "WORKSPACE"
                },
                "job_cluster_key": "Job_cluster",
                "timeout_seconds": 0,
                "email_notifications": {}
       }
      tasks.append(taskdict)
    
  
  return tasks

# COMMAND ----------

import json
import requests
import pandas as pd
import base64

from requests.auth import HTTPBasicAuth

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

sqlnb = createsqlnotebooks(testdict)
nodetree = buildflow(testdict)
taskpayload = gentaskpayload(nodetree)

# COMMAND ----------

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
    "tasks": taskpayload,
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

# COMMAND ----------

genjob = createmtj(fulljsonblob)
print(genjob)
