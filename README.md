Hi! This is readme file to setup and run spark jobs.

Export environent variable :
export BIG_DATA=/home/${user.home}/aaadhaar_analytics/


**directory structure.**

${BIG_DATA}/checkpoints : contains source code for tasks.
${BIG_DATA}/input : Contains input data files(csv).
${BIG_DATA}/output : Contains screenshots and output file generated from checkpoints.

###############################################################################

Steps to run the jobs:

**Note: env variable BIG_DATA should be exported and clean output directory before running big data jobs.**

Format : pyspark src_code_file.py <partition> <input-file>

1.) Checkpoint 1:
	pyspark ${BIG_DATA}/checkpoints/checkpoint_1/spark/dataframe.py 4 ${BIG_DATA}/input/aadhaar_data.csv > ${BIG_DATA}/output/checkpoint_1/result.out


2.) Checkpoint 2:
	pyspark ${BIG_DATA}/checkpoints/checkpoint_2/checkpoint_2.py 4 ${BIG_DATA}/input/aadhaar_data.csv > ${BIG_DATA}/output/checkpoint_2/result.out


3.) Checkpoint 3:
	pyspark ${BIG_DATA}/checkpoints/checkpoint_3/checkpoint_3.py 4 ${BIG_DATA}/input/aadhaar_data.csv > ${BIG_DATA}/output/checkpoint_3/result.out


4.) Checkpoint 4:
	pyspark ${BIG_DATA}/checkpoints/checkpoint_4/checkpoint_4.py 4 ${BIG_DATA}/input/aadhaar_data.csv > ${BIG_DATA}/output/checkpoint_4/result.out


5.) Checkpoint 5:
	pyspark ${BIG_DATA}/checkpoints/checkpoint_5/checkpoint_5.py 4 ${BIG_DATA}/input/aadhaar_data.csv > ${BIG_DATA}/output/checkpoint_5/result.out


Output : ${BIG_DATA}/output/
###############################################################################
