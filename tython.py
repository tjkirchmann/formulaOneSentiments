import pandas as pd #Dataframe manipulation
import numpy as np #Matrix-wise operations
import praw #Reddit API Wrapper
from psaw import PushshiftAPI #Reddit API Wrapper has the ability to select reddit objects based on time, more functionality too
import datetime as dt #used for encoding datetimes
from psycopg2 import connect, sql #postgreSQL functions
from bs4 import BeautifulSoup # BeautifulSoupWeb


### STRINGIFY ROW ### this function takes a list and returns a string with the elements separated by a comma, useful for SQL INSERT Statements
def stringify_row(rowList, current, idx):
    print(current)
    print(idx)
    if idx < 0:
        return current
    if idx == len(rowList):
        current = str(rowList[idx-1])
        idx -= 2
        return stringify_row(rowList, current, idx)
    else:
        current = str(rowList[idx]) + ", " + current
        idx -= 1
        return stringify_row(rowList, current, idx)

### INSERT ROW ### this function formats a SQL Insert statement assuming that you are submitting all columns, takes a list version of a 
# row of data and the name of the table to insert
def insert_row(postRow, table):
    statement = "INSERT INTO"+table+"VALUES(" + stringify_row(postRow, "", len(postRow)) + ");"
    execute_postgresql(None, statement)

### EXECUTE POSTGRESQL ### this functions executes a SQL statement
# REQUIREMENTS: import psycopg2 import sql
def execute_postgresql(ident=None, statement=""):
    print("POSTGRES SQL: Executing...\n"+str(statement))

    if statement[-1] != ";":
        print("POSTGRES SQL: NO SEMI-COLON")
    else:
        try:
            sql_obj = sql.SQL(statement).format(sql.Identifier(ident))
            Cursor.execute( sql_obj )
            print("POSTGRES SQL: Finished")
        except Exception as error:
            print("POSTGRES SQL: ERROR...\n", error)