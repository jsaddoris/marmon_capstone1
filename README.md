# MFT Viper Machine Capstone

This repository conatins the workflow for Marmon Capstone Group 1

Inlcuded is all the necessary code, data, documentation used to in our visualzations, cleaning/modeling, and raw data

# data folder
data_to_R:  
This file contains the R script we used to read in our raw data parquet files to R for analysis
Since Github doesn't support the parquet file type they aren't included in the Repo but will be included in the Process Folder in final submission

Agenda & Documentation:
This file contains various additional documentation such as data understanding and clarifying definitions we recieved from Marmon over the course of this project

# modeling_cleaning folder
1_filter_devices:
This file contains the R script we used to cut down the totals data to observe only relevant devices given to us by Marmon

2_messages_cleaning:
This file contains the R script we used to clean the messages data file, all major transformations/cleaning is captured in this document in the correct order necessary for replication

3_totals_cleaning:
This file contains the R script we used for both cleaning and modeling in the totals file, all major steps are captured in this document in the correct order necessary for replication

occupancy_calculation:
This excel file contains a breakdown of our occupancy calculations

# visuals folder
Contains screenshots of visuals from report along with service used to create
