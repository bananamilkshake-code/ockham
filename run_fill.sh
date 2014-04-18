#!/bin/sh

./filler.py -sname=S.csv -pname=P.csv -spname=SP.csv
mysql --user='root' --password='finncrisporiginal' < company_scheme.sql
mysqlimport --fields-optionally-enclosed-by="\"" --fields-terminated-by="," --lines-terminated-by="\n" --local --user='root' --password='finncrisporiginal' company ./S.csv ./P.csv ./SP.csv


./filler.py -sname=S.csv -pname=P.csv -spname=SP.csv

mysql --user='root' --password='finncrisporiginal' < warehouse.sql