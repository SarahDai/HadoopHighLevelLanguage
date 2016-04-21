cust = load '/user/hadoop/cs561/dataset/Customer.txt' using PigStorage(',') as (id,name,age,countrycode,salary);
trans = load '/user/hadoop/cs561/dataset/Transaction.txt' using PigStorage(',') as (transid,custid,transtotal,transnumitems,transdesc);

custTrim = foreach cust generate id,name;
transTrim = foreach trans generate custid;

g = group transTrim by custid;

transCount = foreach g generate group, COUNT(transTrim.custid) as cnt;

r = RANK transCount BY cnt asc;

o = FILTER r BY (rank_transCount == 1);

ot = foreach o generate $1 as cid, $2 as cn;

jcot = join ot by cid, custTrim by id;

oj = foreach jcot generate $3,$1; 

store oj into '/user/hadoop/cs561/output/project2_4' using PigStorage(',');

