-- commands to drop and create database in hive.

-- drop database if exists
DROP DATABASE IF EXISTS aadhaardb;

-- create database aadhaardb
CREATE DATABASE IF NOT EXISTS aadhaardb
LOCATION '/user/hive/warehouse/analytics';
