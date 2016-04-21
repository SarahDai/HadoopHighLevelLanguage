trans = LOAD '/user/hadoop/cs561/dataset/Transaction.txt' USING PigStorage(',') AS (transID:int, custID:int, transTotal:double, transNumItems:int, transDesc: chararray);
A = GROUP trans by custID parallel 10;
B = FOREACH A generate group, COUNT(trans.transTotal) as NumTransactions, SUM(trans.transTotal) as TotalSum;
store B into '/user/hadoop/cs561/Project2/output/query1' USING PigStorage(',');
