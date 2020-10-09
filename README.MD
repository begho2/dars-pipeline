# Summary
## MODULES
* Rootdir - python env setup (pipenv and pytest)
    * airflow_src - contains dags and python functions for those dags
    * src - place holder for any python code not executed from airflow
    * tests - python pytests to validate any python code or dag imports
    * dars-ingest - scala code for ingesting hes zips and transforming to parq
    * docker-spark-airflow - scala code for ingesting hes zips and transforming to parq


#Setup
change directory to your local sandbox and git clone the project there
cd into the project and setup up the python virtual environment

 pipenv install 
  
  or if you don't have python 3.7 in path, you can specify and exact version
 
 pipenv install python /usr/local/opt/python@3.7/binpython3.7

This reads the Pipfile and creates a pipfile.lock, and installs everything in the Pipfile
It is slow the first time you run it and will create a new virtual python environment hidden away somewhere. 
You do not need a requirements.txt as well (although it is easy to interchange between the two and use tools to keep them in sync)

Now you can run:
 
  pipenv shell
   
to create a shell session with appropriate python and pip versions. 
or you can run 'pipenv graph', or other commands to pipenv commands with correct env

## Setup IDE
..tbd.. pycharm, interpreter chose, mark src/ tests/ airflow/src
In pycharm, go to preferences, python interpreter. Setup a new interpreter,
using Pip environment option. Two choices from here: 1) create new one pointing
at the Pipfile (i think under the hood it will resolve to create new pipenv sandbox 
in the same place the command line setup did) 2) from command line do pipenv --venv, and
navigate to that environment from gui

## Ingest
### Manual Testing
open spark shell with suitable memory, then copy paste code from dars-ingest repo
spark-shell --driver-memory 3g --conf spark.memory.storageFraction=0.1|tee -a ~/spark1.out
3g is enough for ae. still need to try for apc

### Build Run from maven and/or IDE
cd into dars-ingest and run 'mvn package'
it should compile the scala code and package it up into a nice jar for you
import the dars-ingest folder into an ide like intellij.
A good IDE will read the mvn .pom.xml and update its project setup to match maven. 
After this, it should compile and you can run test directly from the IDE


## Setting up Github actions
The .github folder contains config to force run python tests using pytest
pytest is relying on the pytest.ini file to define where the source code is

## Set up tests

