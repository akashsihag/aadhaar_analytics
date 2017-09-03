#!/usr/bin/python

import os
import sys, getopt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SQLContext, Row
import pandas as pd
import math
import matplotlib.pyplot as plt


"""
	author - Akash
	Program - checkpoint-2
"""


# main function
def main(argv):
	conf = SparkConf().setAppName("checkpoint-2")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	partition = int(argv[0])
	input_file = argv[1]

	#Print info to the console
	print '\nCreating dataframe:\n'
	print 'No. of partitions : ', partition
	print 'input_file : ', input_file

	## create rdd from input file and apply map operation to split strings
	rdd = sc.textFile(input_file, partition)
	mapped_rdd = rdd.map(lambda l: l.split(","))

	## Converting fields into appropriate data types
	aadhaar = mapped_rdd.map(lambda l: \
	Row(date=(datetime.strptime(l[0].strip(), '%Y%m%d').date()), \
	        registrar=(l[1].strip()), \
	        private_agency=(l[2].strip()), \
	        state=(l[3].strip()), \
	        district=(l[4].strip()), \
	        sub_district=(l[5].strip()), \
	        pincode=(l[6].strip()), \
	        gender=(l[7].strip()), \
	        age=int(l[8].strip()), \
	        aadhaar_generated=int(l[9].strip()), \
	        rejected=int(l[10].strip()), \
	        mobile_number=int(l[11].strip()), \
	        email_id=int(l[12].strip())))

	# Applying schema to RDD and create Dataframe
	aadhaar_df = sqlContext.createDataFrame(aadhaar)
	aadhaar_df.persist()

	aadhaar_df.registerTempTable('aadhaar_table')
	sqlContext.cacheTable('aadhaar_table')

	# 7. Describe the schema
	print('7 - Schema : ')
	print('\n')
	aadhaar_df.printSchema()
	print('\n\n')

	# 8. count and names of registrars in the table
	print('8 - the count and names of registrars in the table ')
	print('\n')
	count_registrars = aadhaar_df.select("registrar").distinct().count()
	print("8.1 - count of registrars : \n", count_registrars)

	print("names of registrars: \n")
	print('\n')
	if aadhaar_df.isLocal:
		aadhaar_df.select("registrar").distinct().show(count_registrars, False)
	else:
		aadhaar_df.select("registrar").distinct().show(False)
	print('\n\n')

	# 9. number of states, districts in each state and sub-districts in each district
	count_states = aadhaar_df.select("state").distinct().count()
	print(" 9 - number of states : ", count_states)
	print('\n')
	query = 'select state, count(distinct district) as district_count from aadhaar_table group by state order by state'
	district_per_state = sqlContext.sql(query)
	print('\n\n')

	print(" 9 - number of districts in each state : ")
	print('\n')
	district_per_state.show(district_per_state.count(), False)

	query = 'select state, district, count(distinct sub_district) as sub_district_count from aadhaar_table group by state, district order by state, district'
	subdist_per_dist = sqlContext.sql(query)

	print(" 9 -number of sub_district in each district : ")
	print('\n')
	subdist_per_dist.show(subdist_per_dist.count(), False)
	print('\n\n')

	# 10. number of males and females in each state from the table and display a suitable plot
	query = 'select state, \
	 sum(case when gender = \'M\' then 1 else 0 end) as male,\
	 sum(case when gender = \'F\' then 1 else 0 end) as female \
	 from aadhaar_table \
	 group by state \
	 order by state'
	
	mf_each_state = sqlContext.sql(query)
	print(" 10 - number of male and female in each state are: ")
	print('\n')
	mf_each_state.show(mf_each_state.count(), False)
	print('\n\n')

	# plot the graph using Matplotlib        
	df = mf_each_state.toPandas()
	df = df.set_index('state')
	gf = df[['male','female']]
	gf.plot.barh(stacked=True)
	plt.title('Number of males and females in each state')
	plt.xlabel('Number of males and females')
	plt.ylabel('States') 
	plt.show()

	# 11. names of private agencies for each state
	query = 'select distinct state, private_agency from aadhaar_table group by state, private_agency'
	prvt_agencies_each_state = sqlContext.sql(query)
	print(" 11 - names of private_agencies in each state are : ")
	print('\n')
	prvt_agencies_each_state.show(prvt_agencies_each_state.count(), False)
	print('\n\n')

	# 12. number of private agencies for each state.
	query = 'select state, count(distinct private_agency) as private_agencies from aadhaar_table group by state order by state'
	prvt_agencies_each_state = sqlContext.sql(query)
	print(" 12 - Number of private_agencies in each state as follow: ")
	print('\n')
	prvt_agencies_each_state.show(prvt_agencies_each_state.count(), False)
	print('\n\n')
	
	# plot the graph using Matplotlib
	df = prvt_agencies_each_state.toPandas()
	df = df.set_index('state')
	gf = df['private_agencies']
	gf.plot.barh(stacked=True)
	plt.title('Number of private agencies for each state')
	plt.xlabel('Number of private agencies')
	plt.ylabel('States')
	plt.show()
	
	# remove cached elements from memory
	sqlContext.uncacheTable('aadhaar_table')
	aadhaar_df.unpersist()

if __name__ == "__main__":
	main(sys.argv[1:])
