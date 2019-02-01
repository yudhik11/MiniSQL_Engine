#!/bin/bash

#python sql_engine.py

#python sql_engine.py yo
#echo ""

python sql_engine.py "Select B,C from table1,table2 where B = 10 AND C = 20;"
echo ""

python sql_engine.py "Select * from table2"
echo ""

python sql_engine.py "Select B,C from table1, table2  B = 10 AND C = 20;"
echo ""

python sql_engine.py "Select * from table1;"
echo ""

python sql_engine.py "Select * from table1, table2;"
echo ""

python sql_engine.py "Select max(B), avg(B) from table1;"
echo ""

python sql_engine.py "Select max(B) from table1;"
echo ""

python sql_engine.py "Select distinct B, C from table1, table2;"
echo""

python sql_engine.py "Select distinct B, C from table1;"
echo""

python sql_engine.py "Select max(A), avg(A) from table1;"
echo ""

python sql_engine.py "Select distinct B, col2 from table2;"
echo ""

python sql_engine.py "Select B,C from table1,table2 where B = 10 AND C = 20;"
echo ""
