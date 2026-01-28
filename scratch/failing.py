import os
import sys

import opteryx

sys.path.insert(1, os.path.join(sys.path[0], ".."))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../opteryx/opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx"))
sys.path.insert(1, os.path.join(sys.path[0], "../../opteryx/opteryx"))


# Define the SQL statement to query the data
sql_statement = """
SELECT COUNT(*) as Missions, Company 
  FROM $missions
 GROUP BY Company
 ORDER BY Missions DESC;
"""
# This SQL statement counts the number of missions for each company, grouping 
# the results by the company name and ordering them in descending order based 
# on the count of missions.

# Execute the SQL Query and store the results
results = opteryx.query(sql_statement)

# Step Two of Three: Prepare the Data for Visualization

print(results.column_names)

# Prepare data for the pie chart by converting the query results into lists.
missions = list(results["Missions"])  # List of mission counts
companies = list(results["Company"])  # List of corresponding company names
