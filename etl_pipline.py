# Interacting with the Python runtime environment.
import sys
# Interacting with the operating system, such as file paths and directories.
import os

### Specify the path where function files are stored
wd = os.getcwd()
srcpath = os.path.join(wd,"scripts")

### Add the path to the system path
sys.path.append(srcpath)

### Loading the custom libraries
from functions_etl_pipeline import extract_data, transform_data, load_data

### Running the ETL pipeline.
print("⏳ Executing the ETL pipeline...")

print(" ⏳ Extract...")
raw_movies, raw_casts, raw_crews, last_processed_batch = extract_data()
print(" ✅ Extract complete!")
 
print(" ⏳ Transform...")
list_movies, list_casts, list_crews = transform_data(raw_movies, raw_casts, raw_crews)
print(" ✅ Transform complete!")
    
print(" ⏳ Load...")
load_data(list_movies, list_casts, list_crews, last_processed_batch)
print(" ✅ Load complete!")
    
print("✅ Completed the ETL pipeline...")
