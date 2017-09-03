# command to put data in hdfs from local dir
# put command can also be used

hdfs dfs -copyFromLocal /home/akash/analytics/input/aadhaar_data.csv /analytics/data/


# command to check data in hdfs
hdfs dfs -ls /analytics/data/
