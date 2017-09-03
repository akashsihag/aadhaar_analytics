-- commands to create hive external table
-- data is saved /user/hive/warehouse/analytics/ext_aadhaar_card/'

-- ddl command
CREATE EXTERNAL TABLE IF NOT EXISTS aadhaardb.aadhaar_card_external
( REG_DATE STRING,
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
COMMENT "External aadhaar table stored in text format"
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '/user/hive/warehouse/analytics/ext_aadhaar_card/' 
TBLPROPERTIES("SKIP.HEADER.LINE.COUNT"="0"); 


-- dml command
LOAD DATA LOCAL INPATH '/home/akash/analytics/big_data/input/aadhaar_data.csv' OVERWRITE INTO TABLE aadhaardb.aadhaar_card_external;


-- To view top 25 records from the table
SELECT * FROM aadhaardb.aadhaar_card_external LIMIT 25;
