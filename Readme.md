Here is the instructions for the article entitled "_A new hybrid method to quantify the ambiguity degree of human movement networks between weekdays and weekends_". If you have any questions, please contact [spx0310@tongji.edu.cn](mailto:spx0310@tongji.edu.cn) . We provide the illustrate for the origin-destination (OD) data and the instructions for the script.

# Software requirements
The OD data is managed through the PostgreSQL, thus, you need to download and install the PostgreSQL and the PostGIS to store and operate the OD data.

# Data declaration
We provide the New York City (NYC) OD data, Chicago City (CGC) OD data, and the zone maps for reproducing our experiment results. The OD data is stored in the "Data" folder, except for the 2017 taxi data and 2018 taxi data in NYC. However, we provide the script named "Tripdatadownload" to help you obtain these data. For detailed information, please refer to the "Tripdatadownload.py" file in the "Script/Data2postgre" folder. Additionally, the OD data in Shanghai City (SHC) cannot be shared because of the data privacy. But, we can provide the experiment results of the SHC OD data.

# Script declaration
We provide the scripts for reproducing our experiment results in the "Script" folder, expect for the sensitivity analysis part. It results from code security problem raised by the key method of my next research. However, we provide the essential data for this parts in the "Data/NYC/2016ACS" folder. They are used for rearranging the OD matrix based on the semantic information in each zone.

# Script instructions
## OD data to PostgreSQL
Once you installed the PostgreSQL and PostGIS, you can use the script named "Tripdata2postgre" in the "Script/Data2postgre" folder to load the OD data to the database. To load the OD data, you have to create a table in the database first. In this script, we provide three function to create the OD table for storing the OD data. They corresponds to create the table for NYC and CGC taxi data, NYC bike data, and CGC bike data. They are named as:
- 1. create_bike_od_table_points()
- 2. create_bike_od_table()
- 3. create_taxi_od_table()

For the detailed usage, please refer to the "Tripdata2postgre" script. We have well annotated the function. Once you created the table, you have to use the "putinoddata()" function to insert the OD data to the table. The "putinoddata()" function is used to multithread process the inserting operation. In terms of inserting operation, we create three function to insert the OD data provided in "Data" folder. They are named as:
- 1. write2databasebike_points()
- 2. write2databasebike()
- 3. write2databasetaxi()

Except for inserting the data to the table in the database, the three function can also filter the data and map the OD points or the station points to the zone ids through the defined function. you can deep into the annotation about the three functions in the  "Tripdata2postgre" script.

Finally, you have to create the index for faster querying by using the "create_index()" function.
## OD data standardization
After you loaded the OD data to the database, you can use the script named "ODmatrix_standardization" in "Script/Data_standardization" folder to standardize the OD data and output the results in the local folders. We create two function named "adjacent_matrix_bike()" and "adjacent_matrix_taxi()" to output the standardized results. They are shown in the "Results" folder. For detailed usage, please refer the annotation in the "ODmatrix_standardization" script.
## Fuzzy C-Means clustering
Once you standardized the OD data and outputted the corresponding OD matrices, the "Clustering" script in the "Script/Fuzzy C-Means clustering" folder can help you cluster the OD matrix and obtain the membership matrix and clustering centers. To obtain the experiment results, you have to run the "graph_cmeans()" function. It will output two files corresponding to the membership matrix and clustering centers. They are shown in the "Results" folder
## Visualization
To obtain the Figure 3 - 10 in the manuscript, you can run and adjust the "membership_plot ()"and "scatter_plot()" in the "Visulization" script.