#!/usr/bin/python

import os
import sys
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as func
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SQLContext, Row
import math

"""
	author - Akash
	Program - checkpoint 5
"""


# main function
def main(argv):
	conf = SparkConf().setAppName("checkpoint-5")
	sc = SparkContext(conf=conf)

	sqlContext = SQLContext(sc)

	partition = int(argv[0])
	input_file = argv[1]

	# Print info to the console
	print '\nCreating dataframe:\n'
	print 'No. of partitions : ', partition
	print 'input_file : ', input_file

	## Create rdd from input file and apply map operation to split strings
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

	# Apply schema to RDD and create Dataframe
	aadhaar_df = sqlContext.createDataFrame(aadhaar)
	aadhaar_df.registerTempTable('aadhaar_card_table')
	sqlContext.cacheTable('aadhaar_card_table')

	# male and female percentage in each state query
	aadhaar_male_female_query = 'select state,\
					sum(case when gender = \'F\' then aadhaar_generated ELSE 0 END) as aadhaar_generated_female, \
					sum(case when gender = \'M\' then aadhaar_generated ELSE 0 END) as aadhaar_generated_male, \
					sum(aadhaar_generated) as aadhaar_generated from aadhaar_card_table group by state'

	aadhaar_male_female_dataframe = sqlContext.sql(aadhaar_male_female_query)
	aadhaar_male_female_dataframe.registerTempTable('aadhaar_male_female_table')
	sqlContext.cacheTable('aadhaar_male_female_table')

	aadhaar_male_female_prctnge_query = 'select state, \
			(aadhaar_generated_male * 100.0)/aadhaar_generated as aadhaar_generated_male_percentage, \
			(aadhaar_generated_female * 100.0)/aadhaar_generated as aadhaar_generated_female_percentage \
			from aadhaar_male_female_table'

	aadhaar_male_female_prctnge_dataframe = sqlContext.sql(aadhaar_male_female_prctnge_query)
	aadhaar_male_female_prctnge_dataframe.registerTempTable('aadhaar_male_female_prctnge_table')
	sqlContext.cacheTable('aadhaar_male_female_prctnge_table')


	# 22. The top 3 states where the percentage of Aadhaar cards being generated for males is the highest.
	print('\n')
	print('The top 3 states where the percentage of Aadhaar cards being generated for males is the highest : ')
	query = 'select state from ( select * from aadhaar_male_female_prctnge_table \
	order by aadhaar_generated_male_percentage DESC limit 3)temp'
	
	top_3_males_dataframe = sqlContext.sql(query)
	top_3_males_dataframe.persist()
	top_3_males_dataframe.registerTempTable('top_3_males_table')
	top_3_males_dataframe.show(top_3_males_dataframe.count(), truncate=False)
	print('\n\n')


	# 23. The top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.
	print('\n')
	print('The top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest')
	query = 'select top_3_males_table.state, act.district, act.gender, act.rejected \
					from top_3_males_table LEFT OUTER JOIN aadhaar_card_table act\
					ON(top_3_males_table.state = act.state)'

	top_3_males_info_dataframe = sqlContext.sql(query)
	top_3_males_info_dataframe.registerTempTable('top_male_states_info_table')

	query = 'select district from ( select district, \
				(case when rejected = 0 then 0 ELSE (aadhaar_rejected_female * 100.0) / rejected END) as aadhaar_rejected_female_percentage \
				from ( select district, \
						sum(case when gender = \'F\' then rejected ELSE 0 END) as aadhaar_rejected_female, \
						sum(rejected) as rejected \
						from top_male_states_info_table \
						group by district)temp1 \
					order by aadhaar_rejected_female_percentage DESC limit 3)temp2'
	
	top_3_female_rejected_aadhar_df = sqlContext.sql(query)
	top_3_female_rejected_aadhar_df.show(top_3_female_rejected_aadhar_df.count(), truncate=False)
	print('\n\n')


	# 24. The top 3 states where the percentage of Aadhaar cards being generated for females is the highest.
	print('\n')
	print('The top 3 states where the percentage of Aadhaar cards being generated for females is the highest.')   
	query = 'select state from ( select * from aadhaar_male_female_prctnge_table order by aadhaar_generated_female_percentage DESC limit 3)A'
	top_3_females_dataframe = sqlContext.sql(query)
	top_3_females_dataframe.persist()
	top_3_females_dataframe.registerTempTable('top_3_females_table')
	top_3_females_dataframe.show(top_3_females_dataframe.count(), truncate=False)
	print('\n\n')


	# 25. The top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest.
	print('\n')
	print('The top 3 districts where the percentage of Aadhaar cards being rejected for males is the highest')
	query = 'select top_3_females_table.state, act.district, act.gender, act.rejected \
					from top_3_females_table LEFT OUTER JOIN aadhaar_card_table act \
					ON(top_3_females_table.state = act.state)'
	top_female_states_info_dataframe = sqlContext.sql(query)
	top_female_states_info_dataframe.registerTempTable('top_female_states_info_table')


	query = 'select district from ( select district, \
					(case when rejected = 0 then 0 \
						ELSE (aadhaar_rejected_male * 100.0)/rejected END) as aadhaar_rejected_male_percentage \
				from ( select district, \
						sum(case when gender = \'M\' then rejected ELSE 0 END) as aadhaar_rejected_male, \
						sum(rejected) as rejected \
					from top_female_states_info_table \
					group by district)temp1 \
					order by aadhaar_rejected_male_percentage DESC limit 3)temp2'

	top_3_male_rejected_aadhar_df = sqlContext.sql(query)
	top_3_female_rejected_aadhar_df.show(top_3_female_rejected_aadhar_df.count(), truncate=False)
	print('\n\n')
	

	# 26. The summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.
	age_df = aadhaar_df.select("age")
	
	upper_limit = age_df.rdd.max()[0]
	lower_limit = age_df.rdd.min()[0]

	diff_max_min = float(upper_limit - lower_limit)
	bucket_size = int(math.ceil(diff_max_min/10))

	query1 = 'select (case ' \
				+ 'when age <= ' + str(lower_limit + 1*bucket_size) + \
				' then \'' + str(lower_limit) + '_' + str(lower_limit + bucket_size) + '\'' \
				+ 'when age > ' + str(lower_limit + 1*bucket_size) + ' and age <= ' + str(lower_limit + 2*bucket_size) + \
				' then \'' + str(lower_limit + 1*bucket_size) + '_' + str(lower_limit + 2*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 2*bucket_size) + ' and age <= ' + str(lower_limit + 3*bucket_size) + \
				' then \'' + str(lower_limit + 2*bucket_size) + '_' + str(lower_limit + 3*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 3*bucket_size) + ' and age <= ' + str(lower_limit + 4*bucket_size) + \
				' then \'' + str(lower_limit + 3*bucket_size) + '_' + str(lower_limit + 4*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 4*bucket_size) + ' and age <= ' + str(lower_limit + 5*bucket_size) + \
				' then \'' + str(lower_limit + 4*bucket_size) + '_' + str(lower_limit + 5*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 5*bucket_size) + ' and age <= ' + str(lower_limit + 6*bucket_size) + \
				' then \'' + str(lower_limit + 5*bucket_size) + '_' + str(lower_limit + 6*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 6*bucket_size) + ' and age <= ' + str(lower_limit + 7*bucket_size) + \
				' then \'' + str(lower_limit + 6*bucket_size) + '_' + str(lower_limit + 7*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 7*bucket_size) + ' and age <= ' + str(lower_limit + 8*bucket_size) + \
				' then \'' + str(lower_limit + 7*bucket_size) + '_' + str(lower_limit + 8*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 8*bucket_size) + ' and age <= ' + str(lower_limit + 9*bucket_size) + \
				' then \'' + str(lower_limit + 8*bucket_size) + '_' + str(lower_limit + 9*bucket_size) + '\' '  \
				+ 'when age > ' + str(lower_limit + 9*bucket_size) + ' and age <= ' + str(lower_limit + 10*bucket_size) + \
				' then \'' + str(lower_limit +9*bucket_size) + '_' + str(lower_limit + 10*bucket_size) + '\' '  \
				+ 'ELSE \'more_than_' + str(upper_limit) + '\' end) as age_group, ' + 'aadhaar_generated as selected, ' \
				+ 'rejected ' + 'from aadhaar_card_table'

	query_final = 'select age_group, (100 * sum(selected))/(sum(selected) + sum(rejected)) as percentage_summary ' \
				+ 'from ('+ query1 +')A group by age_group order by length(age_group), age_group'

	summary_dataframe = sqlContext.sql(query_final)
	summary_dataframe.show(summary_dataframe.count())

	## unpersist elements from memory (optional)
	top_3_females_dataframe.unpersist()
	top_3_males_dataframe.unpersist()
	sqlContext.uncacheTable('aadhaar_card_table')
	sqlContext.uncacheTable('aadhaar_male_female_table')
	sqlContext.uncacheTable('aadhaar_male_female_prctnge_table')

if __name__ == "__main__":
    main(sys.argv[1:])