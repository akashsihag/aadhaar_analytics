#!/usr/bin/python

import os
import heapq
import sys, getopt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SQLContext, Row
from pyspark.sql.functions import sum

"""
	author - Akash
	Program - checkpoint-4
"""

# main function
def main(argv):
	conf = SparkConf().setAppName("checkpoint-4")
	sc = SparkContext(conf=conf)

	sqlContext = SQLContext(sc)

	partition = int(argv[0])
	input_file = argv[1]

	#Print info to the console
	print '\nConfig :\n'
	print 'No. of partitions : ', partition
	print 'input_file : ', input_file

	# create rdd from input file and apply map operation to split strings
	rdd = sc.textFile(input_file, partition)
	mapped_rdd = rdd.map(lambda l: l.split(","))

	# Converting fields into appropriate data types
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

	# Applying schema to RDD and creatig Dataframe
	aadhaar_df = sqlContext.createDataFrame(aadhaar)
	aadhaar_df.persist()

	# 18. dataframe creation and its summary.
	print("\n18 - Summary :")
	print("\n")
	aadhaar_df.describe().show(5, False)
	print("\n\n")

	# 19. correlation between age and mobile_number
	print("19 - correlation between age and mobile_number")
	print("\n")
	correlation = aadhaar_df.filter("mobile_number > 0").stat.corr('age', 'mobile_number')
	print(correlation)
	print("\n\n")

	# 20 number of unique pincodes
	print("20 - Unique pincodes : ")
	print("\n")
	pincodes = aadhaar_df.select('pincode').distinct()
	print(pincodes.count())
	print("\n\n")

	## 21 number of Aadhaar registrations rejected in UP and Maharashtra
	print("21 - number of Aadhaar registrations rejected in UP and Maharashtra")
	print("\n")
	registrations_rejected = aadhaar_df.filter("state = 'Uttar Pradesh' OR state = 'Maharashtra'") \
	.groupBy("state").agg(sum("rejected").alias("rejected"))
	registrations_rejected.show(registrations_rejected.count(), False)
	print("\n\n")

if __name__ == "__main__":
	main(sys.argv[1:])
