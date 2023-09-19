# dbproconverter

This repo is a v1 of a framework for potentially migrating stored procedures to Databricks. This is a yaml based approach where a customer enters key/value pairs into a yaml file and then that yaml is read in as a dictionary and then parsed to translate the flow and sql operations to abstractions that can be leveraged for dbt + databricks or databricks workflows. There are a handful of foundational sproc compenents that we seek to support in this V1. <br>

The highest level keys in the dictionary are sql and next-- these keys will always be present. The first key within SQL that will be parsed is the skip key-- this is because certain operations in the sproc are just meant to direct flow and are not meant to actually execute sql logic. The range of expected values for this key are yes, no, maybe. (please feel free to reference the template.yml) <br>

If key val is no, this is treated as a situation where it is 100% clear cut that a single sql string will be run, and the path of that string is noconditions.text. If the key val is maybe, than this is treated as a situation where there is ambiguity in terms of which sql query will be run, or if a sql query will be run at all. In this case an additional set of conditions will have to be evaluated to make this determination. There are three types of conditions <br>

There are boolean conditions, not exist conditions, and tree conditions. Within these conditions there are three nested keys, true/false/text. For boolean, the text must be translateable to a boolean condition that returns true/false. Thus the first step is to evaluate the condition, and if its true generate the true text and if its false generate the false text. Notexist is similar, except if the condition does not exist than generate true text and if it does exist generate false text. Lastly, there are tree conditions, which is a dynamic array of x amount of els/if booleans. If none of the booleans in the array evaluates to true than the generated logic will depend on the catchall.text value. <br>

The next dictionary is similar to the sql dictionary-- the main difference is that there is no skip key as the next dictionary will always be used to determine the next node in the node tree. Besides that everything else is the same-- if there are no conditions the logic will parse noconditions.text whereas if there are conditions they will be parsed using the same function mentioned above. <br>

In the dbt implementation, the yamlparser will create sql files (optional) then build a dynamic node tree which will then be used to overwrite the selectors.yml. Thus when a user executes dbt run --selector dynamicproc the dbt run will only run the appropriate subset of models. Another noteworthy aspect is that in the dbt implementation any conditional sql logic is evaluated at compilation time/runtime using Jinja whereas in the multi-task job (mtj) implementation the conditional sql logic is evaluated prior to writing out the notebooks.  

In the MTJ implmenetation, the yamlparser will create sql notebook tasks (optional) and build the node tree. The last step is to translate the node tree to an array of dictionaries that can then be used as a payload to create a MTJ. This MTJ is ultimately meant to achive the same thing as the original SPROC-- dynamic pipeline exeuction that builds a warehouse according to a precise sequence of steps. However this would be a much more modern interpretation of a SPROC-- modular, merge compatible, data quality/lineage/discovery/quality tests with version control and fault tolerance, all on modern scalable + elastic cloud hardware-- no more running sprocs on-prem on fri evening and crossing your fingers hoping it finishes before analysts come into work mon morning!!

### Next steps <br>

This repo is still very much a WIP-- that being said if you import this repo into a databricks environment you can run the yamlparser reading in any valid yml file. Here is a 'hello world' CUJ to try out <br>

i) Import this repo into Databricks using repo feature <br>
ii) Try out MTJ mode or DBT mode with different yamlfiles (you can reference the contents of these yml files in the root directory). Observe the notebooks or sql files that get written to the notebookroot. Further obeserver the MTJ or selectors.yml file that gets generated <br>
iii) Run the logic with integrationtest set to true-- observe the conditional logic and watch as the tests succeed (or fail)

We gladly welcome contributions-- as long as the tests for mix.yml, true.yml, and false.yml pass just describe the nature of the changes that you've implemented and happy to review and merge the PR! In future versions we will seek to automate intergration testing and merging!  

### Here are the medium/long term next steps:

i) Support while looping <br>
ii) Replace sql text so that dynamic database variables execute as expected against the DB. <br>

FWIW this will all be much easier once we natively support sql database vars and looping within workflows :)
