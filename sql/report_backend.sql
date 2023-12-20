/*
Backend job for a power bi report
*/
create table [master] (
ID varchar(50) PRIMARY KEY,
ORDERS int,
Started_date DATETIME,
TotalAmount DECIMAL(10,2),
);

DECLARE @ID VARCHAR(50)
DECLARE @ORDERS INT
DECLARE @Started_date DATETIME
DECLARE @TotalAmount DECIMAL(10,2)

-- Loop to insert random data
DECLARE @Counter INT = 1
WHILE @Counter <= 120000 
BEGIN

    SET @ID = NEWID()
    SET @ORDERS =  FLOOR(RAND() * 10) + 1
    SET @Started_date = DATEADD(DAY, -RAND() * 365, GETDATE()) 
    SET @TotalAmount = RAND() * 10000 

    -- Insert into the table
    INSERT INTO master (ID, ORDERS,Started_date, TotalAmount)
    VALUES (@ID,@ORDERS, @Started_date, @TotalAmount)

    -- Increment counter
    SET @Counter = @Counter + 1
END;

select 
ID,
ORDERS,
year(cast(Started_date as Date)) Year_Started_date,
TotalAmount,
sum(TotalAmount) OVER(PARTITION BY ORDERS,year(cast(Started_date as Date))) sum_orders
from [master];
