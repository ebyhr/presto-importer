presto-io is a command to import a local file to specific table. 

Create sample table.
```
CREATE TABLE schema.catalog.table (
 c1 boolean,
 c2 tinyint,
 c3 smallint,
 c4 integer,
 c5 bigint,
 c6 real,
 c7 double,
 d8 decimal,
 c9 varchar,
 c10 char,
 c11 date
)
;
```

Save csv file in local.
```
true,1,-1,10,100,-1.1,1.1,1,Presto,a,2020-01-01
true,2,-2,20,200,-2.2,2.2,2,High performance,b,2020-01-02
true,3,-3,30,300,-3.3,3.3,3,Versatile,c,2020-01-03
true,4,-4,40,400,-4.4,4.4,4,In-place analysis,d,2020-01-04
true,5,-5,50,500,-5.5,5.5,5,Query federation,e,2020-01-05
false,6,-6,60,600,-6.6,6.6,6,Works with existing BI tools,f,2020-01-06
false,7,-7,70,700,-7.7,7.7,7,Trusted,g,2020-01-07
false,8,-8,80,800,-8.8,8.8,8,Runs everywhere,h,2020-01-08
false,9,-9,90,900,-9.9,9.9,9,Scalable,i,2020-01-09
false,10,-10,100,1000,-10.0,10,10,Open,j,2020-01-10
```

Run presto-io.jar
```
$ ./presto-io.jar --server localhost:8080 --table schema.catalog.table -f sample.csv
```

This command uses PREPARED STATEMENT internally. If you gets below error,
```
Error fetching next at http://hostname:8080/v1/statement/xxx returned an invalid response
```

You may need to decrease --batch-size option or increase these values in config.properties of Presto server 
```
http-server.max-request-header-size=64kB
http-server.max-response-header-size=64kB
```
