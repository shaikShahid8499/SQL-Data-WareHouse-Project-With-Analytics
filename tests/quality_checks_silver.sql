/*
==========================================================
Quality Checks
==========================================================
Script Purpose:
  This script performs various quality checks for data consistency, accuracy,
  and standardization across the 'silver' schema. It includes checks for:
  -Null or Duplicate primary keys.
  -unwanted spaces in string fields.
  -Data standardization and consistency.
  -Invalid date ranges and orders.
  -Data consistency between related fields.
Usage Notes:
  -Run the checks after data loading silver layer.
  -Investigate and resolve any discrepancies found during the checks.
=========================================================
*/

--Testing if the primary key have unique values
SELECT
cst_id,
count(*)
FROM Silver.crm_cust_info
GROUP BY cst_id
HAVING count(*)>1 OR cst_id IS NULL;

--checking for the distinct values for further transformation
SELECT
DISTINCT cst_marital_status
from Silver.crm_cust_info;

--checking for the head and tail spaces for the values
SELECT
cst_lastname
FROM Silver.crm_cust_info
where TRIM(cst_lastname)!=cst_lastname;


--checking if the start date is bigger then end date
SELECT
prd_id,
prd_start_dt,
prd_end_dt
from Silver.crm_prd_info
where prd_start_dt>prd_end_dt
