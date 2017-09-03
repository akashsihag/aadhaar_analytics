#!/usr/bin/python

import os
import sys, getopt
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SQLContext, Row

"""
	author - Akash
	Program - checkpoint-1
"""


# main function
def main(argv):
	conf = SparkConf().setAppName("checkpoint-1")
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
        
        ## Applying schema to RDD and creatig Dataframe
        aadhaar_df = sqlContext.createDataFrame(aadhaar)
	aadhaar_df.show(25);

if __name__ == "__main__":
   main(sys.argv[1:])
        
