#!/bin/sh

./filler.py -sname=S.csv -pname=P.csv -spname=SP.csv
mysql --user='root' --password='finncrisporiginal' < company1.sql
mysqlimport --fields-optionally-enclosed-by="\"" --fields-terminated-by="," --lines-terminated-by="\n" --local --user='root' --password='finncrisporiginal' company1 ./S.csv ./P.csv ./SP.csv

./filler.py -sname=S.csv -pname=P.csv -spname=SP.csv
mysql --user='root' --password='finncrisporiginal' < company2.sql
mysqlimport --fields-optionally-enclosed-by="\"" --fields-terminated-by="," --lines-terminated-by="\n" --local --user='root' --password='finncrisporiginal' company2 ./S.csv ./P.csv ./SP.csv

mysql --user='root' --password='finncrisporiginal' < warehouse.sql

ruby ./etl.rb