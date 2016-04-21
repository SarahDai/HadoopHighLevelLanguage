trans = LOAD '/user/hadoop/cs561/dataset/Transaction.txt' USING PigStorage(',') AS (transID:int, custID:int, transTotal:double, transNumItems:int, transDesc: chararray);
A = FOREACH trans generate custID, transTotal;
cust = LOAD '/user/hadoop/cs561/dataset/Customer.txt' USING PigStorage(',') AS (ID:int, name:chararray, age:int, CountryCode:int, salary: double);
alpha = FOREACH cust generate ID, CountryCode;
B = join A by custID, alpha by ID;
C = group B by CountryCode;
D = foreach C {E = DISTINCT B.ID; transtotal = B.transTotal; generate group, COUNT(E) as NumberOfCustomers, MIN(B.transTotal) as MinTotal, MAX(B.transTotal) as MaxTotal;};
store D into '/user/hadoop/cs561/Project2/output/query3' USING PigStorage(',');
