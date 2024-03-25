**Data Processing Script**

A script for processing data files and loading into a database.

**Installation**

Before installing the Data Processing Script, please make sure you have setuptools installed in your Python environment. You can install it using pip:

**Please use following command to run setup python setup.py install at root directory.**

**Project Structure:**
project_root/ │ ├── / │ ├── init.py │ ├──fund_analysis.py │ │ ├──modules.py │└── │ └── TestScriptFunctions.py


**Under directory : DE_Assessment**
fund_analysis.py is the main script. 

-modules.py has utility modules using in fund analysis and test cases.

-setup.py is to setup the env for executing the entire analysis.

-config.yaml is the configuration yaml file. 

-TestScriptFunctions.py : This is the script which has the test cases written to test the analyis



**under directory : DE_Assessment/view_scripts**
-Contains two view creation statements for recon and best fund analysis. 

-v_best_perf_equities_report.sql

-v_equities_bond_price_recon.sql

**Under directory : DE_Assessment/Report**
-Contains the reports created by the analysis scripts for recon and best fund of the month. 

-best_fund_report.csv

-recon_report.csv

**How to run testcases?**

Go to root directory and run python TestScriptFunctions.py 
