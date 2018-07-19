
"""
Copyright Google Inc. 2018
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

import argparse
import logging
import datetime, os
import apache_beam as beam
import math

'''

This is a dataflow pipeline that demonstrates Python use of side inputs. The pipeline finds Java packages
on Github that are (a) popular and (b) need help. Popularity is use of the package in a lot of other 
projects, and is determined by counting the number of times the package appears in import statements.
Needing help is determined by counting the number of times the package contains the words FIXME or TODO
in its source.

@author tomstern
based on original work by vlakshmanan

python JavaProjectsThatNeedHelp.py --project <PROJECT> --bucket <BUCKET> --DirectRunner or --DataFlowRunner

'''

# Global values
TOPN=1000


# Command line arguments
parser = argparse.ArgumentParser(description='Demonstrate side inputs')
parser.add_argument('--bucket', required=True, help='Specify Cloud Storage bucket for output')
parser.add_argument('--project',required=True, help='Specify Google Cloud project')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--DirectRunner',action='store_true')
group.add_argument('--DataFlowRunner',action='store_true')
      
opts = parser.parse_args()

if opts.DirectRunner:
  runner='DirectRunner'
if opts.DataFlowRunner:
  runner='DataFlowRunner'

bucket = opts.bucket
project = opts.project


limit_records=''
if runner == 'DirectRunner':
   limit_records='LIMIT 3000'     
get_java_query='SELECT content FROM [fh-bigquery:github_extracts.contents_java_2016] {0}'.format(limit_records)



# ### Functions used for both main and side inputs

def splitIntoLines(input):
   acc=[]
   if input is None:
     return
   lines=input.split('\n')
   for line in lines:
      acc.append(line)
   yield acc

def splitPackageName(packageName):
   """e.g. given com.example.appname.library.widgetname
           returns com
	           com.example
                   com.example.appname
      etc.
   """
   result = []
   end = packageName.find('.')
   while end > 0:
      result.append(packageName[0:end])
      end = packageName.find('.', end+1)
   result.append(packageName)
   return result

def getPackages(line, keyword):
   start = line.find(keyword) + len(keyword)
   end = line.find(';', start)
   if start < end:
      packageName = line[start:end].strip()
      return splitPackageName(packageName)
   return []
   
def packageUse(line, keyword):
   packages = getPackages(line, keyword)
   for p in packages:
      yield (p, 1)

def by_value(kv1, kv2):
   key1, value1 = kv1
   key2, value2 = kv2
   return value1 < value2


# Returns the package line (full name) and the count of FIXME and TODO as a tuple
#
def packagesThatAreImported(record):
   count=0
   import_name= u'EMPTY'
   for line in record:
      if line.startswith('import'):
         import_name=line
   return import_name

# Finds lines that start with 'import'
# Break out the package name into separate components  
# Count the number of times each is used and combine, order, and limit them     
 
def is_popular(input_data):
  keyword='import'
  return (input_data 
      | 'GetImports' >> beam.Map(lambda line: packagesThatAreImported(line) )
      | 'PackageUse' >> beam.FlatMap(lambda line: packageUse(line, keyword))
      | 'TotalUse' >> beam.CombinePerKey(sum)
      | 'Top_NNN' >> beam.transforms.combiners.Top.Of(TOPN, by_value) )
        

# ### Functions used to determine packages that need help

# Returns the package line (full name) and the count of FIXME and TODO as a tuple
#
def packagesThatNeedHelp(record):
   count=0
   package_name= u'EMPTY'
   for line in record:
      if line is None:
         continue
      if line.startswith('package'):
         package_name=line
      if 'FIXME' in line or 'TODO' in line:
         count+=1
   yield (package_name,count)

# Drop all entries with zero help value 
class FilterHelp(beam.DoFn):
  def process(self, element):
     if element[1] > 0:
       return [(element[0],element[1])]

# Separate the package name, and associate it with the help count
#
def splitHelpPackageName(packageName,count):
   """e.g. given com.example.appname.library.widgetname
           returns (com,count)
	           (com.example,count)
                   (com.example.appname,count)
      etc.
   """
   result = []
   end = packageName.find('.')
   while end > 0:
      result.append((packageName[0:end],count))
      end = packageName.find('.', end+1)
   result.append((packageName,count))
   return result

# Strip the leading and trailing text from the package line
#
def getHelpPackages(line,count,keyword):
   start = line.find(keyword) + len(keyword)
   end = line.find(';', start)
   if start < end:
      packageName = line[start:end].strip()
      return splitHelpPackageName(packageName,count)
   return []


# Identify tuples with package name and a count of help items 'FIXME' and 'TODO' 
# Split the package names into components and accumulate the help count   
#
def identify_needs_help(input_data):
  keyword='package'
  return(input_data 
      | 'GetPackages' >> beam.FlatMap(lambda record: packagesThatNeedHelp(record) ) 
      | 'PackageNamesHelp' >> beam.FlatMap(lambda line: getHelpPackages(line[0], line[1],keyword))
      | 'TotalHelp' >> beam.CombinePerKey(sum) ) 


# Calculate the final composite score 
# 
#    For each package that needs help, get the help count
#    If the package is in the popularity dictionary, retrieve the popularity cound  
#    Multiply to get compositescore
#      - Using log() because these measures are subject to tournament effects
#
def compositeScore(help, popular):
   acc=[]
   for element in help:
     helpCount = element[1]
     popCount = popular.get(element[0])
     if popular.get(element[0]):
       compositescore = math.log(popCount) * math.log(helpCount)
       if compositescore > 0:
         acc.append((element[0],compositescore))
   return acc  


# ### main 

# Define pipeline runner (lazy execution)
def run():
  argv = [
    '--project={0}'.format(project),
    '--job_name=sideinputsjob',
    '--save_main_session',
    '--staging_location=gs://{0}/staging/'.format(bucket),
    '--temp_location=gs://{0}/staging/'.format(bucket),
    '--runner={0}'.format(runner)
    ]

  p = beam.Pipeline(argv=argv)
   

  # Read the table rows into a PCollection (a Python Dictionary)
  #    Limit records if running local, or full data if running on the cloud
  # 
  bigqcollection = p | 'ReadFromBQ' >> beam.io.Read(beam.io.BigQuerySource(project=project,query=get_java_query))
 
  # Parse the dictionary and return a PCollection of simple lines
  cleancollection = bigqcollection | 'ToLines' >> beam.FlatMap(lambda rowdict: splitIntoLines(rowdict['content']))
    
  # Identify the popular packages; a list of tuples for calculation; this will be the main input
  popular_packages = is_popular(cleancollection)       
   
  # Idenitfy the packages that need help (have "FIXME" or "TODO" in the text); this will be the side input 
  help_packages = identify_needs_help(cleancollection)
   
  # The following uses side inputs to view the help_packages as a dictionary
  #   Uses both main and side to calculate the composite score
  results = popular_packages | 'SideInputs' >> beam.Map(lambda element, the_dict: compositeScore(element,the_dict), beam.pvalue.AsDict(help_packages))

  # Write out the composite scores and packages to an unsharded csv file
  output_results = 'gs://{0}/javahelp/Results'.format(bucket)
  results | 'WriteToStorage' >> beam.io.WriteToText(output_results,file_name_suffix='.csv',shard_name_template='')
    
  # Run the pipeline (all operations are deferred until run() is called).


  if runner == 'DataFlowRunner':
     p.run()
  else:
     p.run().wait_until_finish()
  logging.getLogger().setLevel(logging.INFO)


if __name__ == '__main__':
  run()

