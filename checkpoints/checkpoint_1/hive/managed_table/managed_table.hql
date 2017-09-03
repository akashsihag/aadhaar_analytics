-- Hive Managed table

-- HiveQL commands to create mng_aadhaar_card managed table
-- Data storage is managed by hive internally

CREATE TABLE IF NOT EXISTS aadhaardb.mng_aadhaar_card
( 
  REG_DATE STRING,
  REGISTRAR STRING,
  PRIVATE_AGENCY STRING,
  STATE STRING,
  DISTRICT STRING,
  SUB_DISTRICT STRING,
  PINCODE STRING,
  GENDER VARCHAR(1),
  AGE INT,
  AADHAAR_GENERATED BIGINT,
  REJECTED BIGINT,
  MOBILE_NUMBER BIGINT,
  EMAIL_ID BIGINT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive/warehouse/analytics/mng_aadhaar_card/' 
TBLPROPERTIES("SKIP.HEADER.LINE.COUNT"="0"); 



-- Hive LOAD script to load managed aadhaar_card table from local file system.
LOAD DATA LOCAL INPATH '/home/akash/analytics/big_data/input/aadhaar_data.csv' OVERWRITE INTO TABLE aadhaardb.mng_aadhaar_card;

-- Show top 25 records from loaded table
SELECT * FROM aadhaardb.mng_aadhaar_card LIMIT 25;



