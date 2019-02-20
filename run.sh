#!/bin/bash

#python 20161093.py

#python 20161093.py yo
#echo ""

python 20161093.py "Select B,C from table1,table2 where B = 10 AND C = 20;"
echo ""

python 20161093.py "Select * from table2"
echo ""

python 20161093.py "Select B,C from table1, table2  B = 10 AND C = 20;"
echo ""

python 20161093.py "Select * from table1;"
echo ""

python 20161093.py "Select * from table1, table2;"
echo ""

python 20161093.py "Select max(B), avg(B) from table1;"
echo ""

python 20161093.py "Select max(B) from table1;"
echo ""

python 20161093.py "Select distinct B, C from table1, table2;"
echo""

python 20161093.py "Select distinct B, C from table1;"
echo""

python 20161093.py "Select max(A), avg(A) from table1;"
echo ""

python 20161093.py "Select distinct B, col2 from table2;"
echo ""

python 20161093.py "Select B,C from table1,table2 where B = 10 AND C = 20;"
echo ""
