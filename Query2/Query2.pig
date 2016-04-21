trans = LOAD '/user/hadoop/cs561/dataset/Transaction.txt' USING PigStorage(',') AS (transID:int, custID:int, transTotal:double, transNumItems:int, transDesc: chararray);
A = FOREACH trans generate custID, transTotal, transNumItems;
B = group A by custID;
C = foreach B generate group, COUNT(A.transTotal) as NumOfTransaction, SUM(A.transTotal) as TotalSum, MIN(A.transNumItems) as MinItems;
cust = LOAD '/user/hadoop/cs561/dataset/Customer.txt' USING PigStorage(',') AS (ID:int, name:chararray, age:int, CountryCode:int, salary: double);
alpha = FOREACH cust generate ID, name, salary;
D = join C by group, alpha by ID;
E = foreach D generate ID, name as Name, salary as Salary, NumOfTransaction, TotalSum, MinItems;
store E into '/user/hadoop/cs561/Project2/output/query2' USING PigStorage(',');


