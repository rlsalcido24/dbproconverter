# dbproconverter

This repo is a v1 of a framework for potentially migrating stored procedures to databricks workflows. This is a yaml based approach where a customer enters key/value pairs into a yaml file and then that yaml is read in as a dictionary and then parsed to translate the flow and sql operations to json that can be fed into a MTJ. There are a handful of foundational components sproc compenents that we seek to support in this V1. <br>

The highest level keys in the dictionary are sql and next-- these keys will always be present. The first key within SQL that will be parsed is the skip key-- this is because certain operations in the sproc are just meant to direct flow and are not meant to actually execute sql logic. The range of expected values for this key are yes, no, maybe. <br>

If key val is no, this is treated as a situation where it is 100% clear cut that a single sql string will be run, and the path of that string is noconditions.text. If the key val is maybe, than this is treated as a situation where there is ambiguity in terms of which sql query will be run, or if a sql query will be run at all. In this case an additional set of conditions will have to be evaluated to make this determination. There are three types of conditions <br>

There are boolean conditions, not exist conditions, and tree conditions. Within these conditions there are three nested keys, true/false/text. For boolean, the text must be translateable to a boolean condition that returns true/false. Thus the first step is to evaluate the condition, and if its true generate the true text and if its false generate the false text. Notexist is similar, except if the condition does not exist than generate true text and if it does exist generate false text. Lastly, there are tree conditions, which is a dynamic array of x amount of els/if booleans. If none of the booleans in the array evaluates to true than the generated logic will depend on the catchall.text value. <br>

Next has similarities-- there is no skip however, just noconditions and conditions. conditions logic is essentially the same

the main body has three main functions-- sql read in, tasks generated, node tree, translate to dict that mtj can read, gen mtj. try it out today!!
