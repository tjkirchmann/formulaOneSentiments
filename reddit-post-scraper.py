import praw
from psaw import PushshiftAPI
import datetime as dt
import pandas as pd
from psycopg2 import connect, sql

#PRAW Reddit Instance
reddit = praw.Reddit(
    client_id = "39xb4_RQG875f_WdFaU6nQ",
    client_secret="73AgSa_7TmG7Hy5BfgKw6J5E8uqY0Q",
    user_agent="OrlandoSucks",
)
#SQL Connect
connection = connect(
    dbname="f1sentiments",
    user="postgres",
    host="localhost",
    password="@stonM@rtin2021"
)

Cursor = connection.cursor()

#PSAW API Instance
api = PushshiftAPI()

# Environment Variables
    #Year Range
yearRange = {"start": 2014, "end": 2021}
    #DIR
dirPath = './sentiment-data/reddit/'




def log_progress(update):
    print(update)
    return

def write_year_submissions(year):
    submission_dict = {
        "id": [],
        "author": [],
        "time_created": [],
        "edited":[],
        "is_original_content": [],
        "is_self": [],
        "locked": [],
        "name": [],
        "num_comments": [],
        "over_18": [],
        "score": [],
        "selftext": [],
        "spoiler": [],
        "title": [],
        "upvote_ratio": [],
        "year": [],
    }
    #Defining the year window
    ts_after = int(dt.datetime(year, 1, 1).timestamp())
    ts_before = int(dt.datetime(year+1, 1, 1).timestamp())

    gen = api.search_submissions(
        after=ts_after,
        before=ts_before,
        filter=['id'],
        subreddit='formula1',
        limit=1 #for testing purposes
    )
    
    for submission in gen:
        sub_id = submission.d_['id']
        submission_data = reddit.submission(id=sub_id)

        #Data Collection
        submission_dict['id'].append(submission_data.id)
        submission_dict['author'].append(submission_data.author)
        submission_dict['time_created'].append(submission_data.created_utc)
        submission_dict['edited'].append(submission_data.edited)
        submission_dict['is_original_content'].append(submission_data.is_original_content)
        submission_dict['is_self'].append(submission_data.is_self)
        submission_dict['locked'].append(submission_data.locked)
        submission_dict['name'].append(submission_data.name)
        submission_dict['num_comments'].append(submission_data.num_comments)
        submission_dict['over_18'].append(submission_data.over_18)
        submission_dict['score'].append(submission_data.score)
        submission_dict['selftext'].append(submission_data.selftext)
        submission_dict['spoiler'].append(submission_data.spoiler)
        submission_dict['title'].append(submission_data.title)
        submission_dict['upvote_ratio'].append(submission_data.upvote_ratio)
        submission_dict['year'].append(year)
        #end Data Collection
    dataFrame = pd.DataFrame(submission_dict)
    dataFrame.to_csv(str(year)+"-submissions.csv", index=False)

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


def insert_row(postRow, table):
    statement = "INSERT INTO"+table+"VALUES(" + stringify_row(postRow, "", len(postRow)) + ");"
    execute_postgresql(None, statement)

def query_posts_by_day(year, month, day, subreddit):
    ts_after = int(dt.datetime(year, month, day).timestamp())
    ts_before = int(dt.datetime(year, month, day+1).timestamp())

    gen = api.search_submissions(
        after=ts_after,
        before=ts_before,
        filter=['id'],
        subreddit=subreddit,
        limit=100 #for testing purposes
    )
    return gen

def listify(dataDict):
    cols = dataDict.keys()
    output = []
    for col in cols:
        output.append(dataDict[col])
    return output
# def main():
#     for year in range(yearRange['start'], yearRange['end']):
#         write_year_submissions(year)

#main()
#testlist = [1,2,3,4,5,"boo"]

testdict = {
    'one':1,
    'two':2,
    'three':"three"
}
print(listify(testdict))

#print(testlist)
#string = stringify_row(testlist, "", len(testlist))
#print(string)