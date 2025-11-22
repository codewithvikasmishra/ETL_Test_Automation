src_sql = """SELECT Employee_ID,
CASE
	WHEN Department IN ('Market Dept','Marketing','MKT')
	THEN 'Marketing'
	WHEN Department IN ('Hr Dept','HR','Human Resources', 'H.R.')
	THEN 'Human Resources'
	WHEN Department IN ('IT','I.T.','Information Technology')
	THEN 'Information Technology'
	WHEN Department IN ('OPR','Ops','Operations')
	THEN 'Operations'
	WHEN Department IN ('FIN','Finance Dept','Finance')
	THEN 'Finance'
END AS Department,
CASE
	WHEN Employment_Status = 'Active'
	THEN DATEDIFF(day, Join_Date, GETDATE())
	WHEN Employment_Status IN ('Terminated', 'Resigned')
	THEN DATEDIFF(Day, Join_Date, Leave_Date)
END AS Working_Days,
CASE
	WHEN Employment_Status = 'Active'
	THEN '1'
	ELSE '0'
END AS 'Current_Employee_Flag',
CASE
	WHEN Employment_Status = 'Active'
	AND Performance_Score >= 80
	AND Experience_Years >= 3
	THEN 'Eligible'
	ELSE 'Not Eligible'
END AS Promotion_Eligilibility,
(
        SELECT '' + SUBSTRING(Salary, v.n, 1)
        FROM (
            -- generate numbers 1..LEN(Salary)
            SELECT TOP (CASE WHEN LEN(Salary) IS NULL THEN 0 ELSE LEN(Salary) END)
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
            FROM sys.all_objects
        ) v
        WHERE UNICODE(SUBSTRING(Salary, v.n, 1)) BETWEEN 48 AND 57
        ORDER BY v.n
        FOR XML PATH(''), TYPE
    ).value('.', 'VARCHAR(50)') AS Salary_Numeric,
CASE
	WHEN Salary LIKE '%$%' 
	OR Salary LIKE '%USD %' 
	OR Salary LIKE '% USD%' 
	THEN 'USD'
    WHEN Salary LIKE '%â‚¹%' 
	OR Salary LIKE '%INR%' 
	OR Salary LIKE '% INR%' 
	THEN 'INR'
	ELSE 'UNKNOWN'
END AS Salary_Currency,
CASE
	WHEN Leave_Date IS NOT NULL
	AND Employment_Status = 'Active'
	THEN 'Employee Left but Status Active, Fix Required'
	WHEN Leave_Date IS NULL
	AND Employment_Status <> 'Active'
	THEN 'Employee is Active but leave_date mentioned, Fix Required'
	WHEN Leave_Date>Join_Date
	THEN 'Wrong Entry Flag'
END AS 'Action_Items'
FROM
dbo.test_employee"""