-- pig_example.pig
-- Author: Sanika
-- Objective: Perform Load & Store, Aggregation, Filtering, and Joining operations in Apache Pig

---------------------------------------------------------------------------
-- STEP 1: LOAD DATA FROM HDFS
---------------------------------------------------------------------------
employees = LOAD '/user/hadoopuser/pig_input/employees.csv'
            USING PigStorage(',')
            AS (id:int, name:chararray, dept:int, salary:double);

departments = LOAD '/user/hadoopuser/pig_input/departments.csv'
              USING PigStorage(',')
              AS (dept_id:int, dept_name:chararray);

---------------------------------------------------------------------------
-- STEP 2: FILTERING OPERATION
-- Objective: Select employees earning more than 50,000
---------------------------------------------------------------------------
high_paid = FILTER employees BY salary > 50000;

-- View filtered data (uncomment next line to test)
-- DUMP high_paid;

---------------------------------------------------------------------------
-- STEP 3: AGGREGATION OPERATION
-- Objective: Find department-wise statistics
-- Operations: COUNT, SUM, AVG, MAX, MIN
---------------------------------------------------------------------------
grp = GROUP employees BY dept;

dept_stats = FOREACH grp GENERATE
                group AS dept,
                COUNT(employees) AS emp_count,
                SUM(employees.salary) AS total_salary,
                AVG(employees.salary) AS avg_salary,
                MAX(employees.salary) AS max_salary,
                MIN(employees.salary) AS min_salary;

-- View aggregation result (uncomment next line to test)
-- DUMP dept_stats;

---------------------------------------------------------------------------
-- STEP 4: JOIN OPERATION
-- Objective: Combine employee details with department names
---------------------------------------------------------------------------
emp_dept = JOIN employees BY dept, departments BY dept_id;

emp_with_deptname = FOREACH emp_dept GENERATE
                        employees::id AS id,
                        employees::name AS name,
                        departments::dept_name AS dept_name,
                        employees::salary AS salary;

-- View join result (uncomment next line to test)
-- DUMP emp_with_deptname;

---------------------------------------------------------------------------
-- STEP 5: STORE RESULTS INTO HDFS
-- Output will be stored in new folders under /user/hadoopuser/pig_output
---------------------------------------------------------------------------
STORE high_paid INTO '/user/hadoopuser/pig_output/high_paid' USING PigStorage('\t');
STORE dept_stats INTO '/user/hadoopuser/pig_output/dept_stats' USING PigStorage('\t');
STORE emp_with_deptname INTO '/user/hadoopuser/pig_output/emp_with_dept' USING PigStorage('\t');

---------------------------------------------------------------------------
-- END OF SCRIPT
---------------------------------------------------------------------------

