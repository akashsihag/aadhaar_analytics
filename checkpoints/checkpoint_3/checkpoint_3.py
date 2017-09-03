#!/usr/bin/python

import os
import sys, getopt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SQLContext, Row
import math
from pyspark.sql.functions import sum


"""
	author - Akash
	Program - checkpoint-2
"""


# main function
def main(argv):
	conf = SparkConf().setAppName("checkpoint-3")
	sc = SparkContext(conf=conf)
	sqlContext = SQLContext(sc)

	partition = int(argv[0])
	input_file = argv[1]

	#Print info to the console
	print '\Start:\n'
	print 'No. of partitions : ', partition
	print 'input_file : ', input_file
	print('\n\n')

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


	# (13. Find top 3 states generating most number of Aadhaar cards)
	print '13 - top 3 states with most number of Aadhar cards : '
	print('\n')

	df = aadhaar_df.select('state', 'aadhaar_generated').groupBy('state').sum('aadhaar_generated') \
	.withColumnRenamed('sum(aadhaar_generated)','aadhaar_generated').orderBy('aadhaar_generated',ascending=False)
	print(df.head(3))
	print('\n\n')

	# (14. Find top 3 private agencies generating the most number of Aadhar cards)
	print '14 - top 3 private agencies with most number of Aadhar cards : '
	print('\n')

	df = aadhaar_df.select('private_agency','aadhaar_generated').groupBy('private_agency').sum('aadhaar_generated') \
	.withColumnRenamed('sum(aadhaar_generated)','aadhaar_generated').orderBy('aadhaar_generated', ascending=False)
	print(df.head(3))
	print('\n\n')


	# (15. Find the number of residents providing email, mobile number? (Hint: consider non-zero values))
	print '15 - number of residents provided email and mobile number : '
	print('\n')

	df = aadhaar_df.select('email_id','mobile_number').agg(sum('email_id').alias('email_id'), sum('mobile_number').alias('mobile_number'))
	df.show()
	print('\n\n')
	# .withColumnRenamed('sum(email_id)','email_id').withColumnRenamed('sum(mobile_number)','mobile_number')

	# (16. Find top 3 districts where enrolment numbers are maximum)
	print '16 - top 3 districts where enrolment numbers are maximum : '
	print('\n')

	df = aadhaar_df.select(aadhaar_df['district'], aadhaar_df['aadhaar_generated'] + aadhaar_df['rejected']) \
	.withColumnRenamed('(aadhaar_generated + rejected)','enrolments').orderBy('enrolments', ascending=False)
	print(df.head(3))
	print('\n\n')
	

	# (17. Find the no. of Aadhaar cards generated in each state)
	print '17 - no. of Aadhaar cards generated in each state : '
	print('\n')

	df_count = aadhaar_df.select('state','aadhaar_generated').groupBy('state').sum('aadhaar_generated') \
	.withColumnRenamed('sum(aadhaar_generated)','aadhaar_generated').count()

	df = aadhaar_df.select('state','aadhaar_generated').groupBy('state').sum('aadhaar_generated') \
	.withColumnRenamed('sum(aadhaar_generated)','aadhaar_generated')
	df.show(df_count)
	print('\n\n')

if __name__ == "__main__":
	main(sys.argv[1:])
