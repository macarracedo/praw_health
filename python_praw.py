import praw 
import json
import mysql.connector as mysql
from praw.reddit import Submission
from praw.models import MoreComments

credentials = 'client_secrets.json'

enable_commit = True
enable_submission_insert = False
enable_comment_insert = False
enable_redditor_insert = False
post_limit=1000
# comment_limit=2

with open(credentials) as f:
    creds = json.load(f)

# Config Reddit connection
reddit = praw.Reddit(client_id=creds['client_id'],
                     client_secret=creds['client_secret'],
                     user_agent=creds['user_agent'],
                     redirect_uri=creds['redirect_uri'],
                     refresh_token=creds['refresh_token'])
reddit.validate_on_submit=True

# Config local DB connection
db =  mysql.connect(
    host = "localhost",
    user = "praw_user",
    passwd = "reddit",
    database = "test"
)
mycursor = db.cursor()

mycursor.execute("SHOW DATABASES")

for x in mycursor:
    print(x)

subreddit = reddit.subreddit("cancer")
hot_subreddit = subreddit.hot(limit=post_limit)

query = ""
for submission in hot_subreddit:
    # lo siguiente descarta mensajes fijados
    if not submission.stickied:
        print(40*'-')

        print("title : {}, submission.id: {}, author: {}, author.id: {}, num_comments: {}".format(
                                                    submission.title,
                                                    submission.id,
                                                    submission.author.name,
                                                    submission.author.id,
                                                    submission.num_comments))
        # print("Text: \n" + submission.selftext)
        

        # Compruebo autor del submission
        query = "SELECT pk_id_redditor FROM Redditor WHERE pk_id_redditor = %s"
        valores = (submission.author.id, )
        mycursor.execute(query, valores)
        resultado = mycursor.fetchone() #devuelve uno

        if(resultado is not None): # conocemos al redditor
            print(resultado)
            print(mycursor.rowcount, "NO insertado el \"Redditor\" con id: " + submission.author.id)
        else:   # no tenemos este submission -> lo anhadimos
            query = "INSERT INTO Redditor (pk_id_redditor, name) VALUES (%s, %s)"
            valores = (submission.author.id, submission.author.name)
            if enable_redditor_insert:
                mycursor.execute(query, valores)
                print(mycursor.rowcount, "Insertado el \"Redditor\" con id: " + submission.author.id)
        

        # Compruebo el submission
        query = "SELECT pk_id_submission FROM Submission WHERE pk_id_submission = %s"
        valores = (submission.id, )
        mycursor.execute(query, valores)
        resultado = mycursor.fetchone()

        if(resultado is not None): # ya tenemos el submission
            print(resultado)
            print(mycursor.rowcount, "NO insertado el \"Submission\" con id: " + submission.id)
        else:   # no tenemos este submission -> lo anhadimos
            query = "INSERT INTO Submission (pk_id_submission, title, selftext, id_author, num_comments) VALUES (%s, %s, %s, %s, %s)"
            valores = (submission.id, submission.title, submission.selftext, submission.author.id, submission.num_comments)
            if enable_submission_insert:
                mycursor.execute(query, valores)
                print(mycursor.rowcount, "Insertado el \"Submission\" con id: " + submission.id)
        

        
        # Recorremos los comentario de las publicaciones (todos)

        comments = submission.comments
        # comments = submission.comments.replace_more(limit=comment_limit)
        # Check possible improvement using replace_more() method from .CommentForest
        # Requires network conecction each time it is called
        for comment in comments:
            print(20*'-')
            #print(comment.body)
            print("author: {}, author.id: {}, comment.id: {}, parent.id: {}".format(
                                                        comment.author.name,
                                                        comment.author.id,
                                                        comment.id,
                                                        comment.parent_id))
            # Compruebo autor del comentario
            query = "SELECT pk_id_redditor FROM Redditor WHERE pk_id_redditor = %s"
            valores = (comment.author.id, )
            mycursor.execute(query, valores)
            resultado = mycursor.fetchone()

            if(resultado is not None):
                print(resultado)
                print(mycursor.rowcount, "NO insertado el \"Redditor\" con id: " + comment.author.id)
            else:   # no conocemos al autor -> lo anhadimos
                query = "INSERT INTO Redditor (pk_id_redditor, name) VALUES (%s, %s)"
                valores = (comment.author.id, comment.author.name)
                if enable_redditor_insert:
                    mycursor.execute(query, valores)
                    print(mycursor.rowcount, "Insertado el \"Redditor\" con id: "+ comment.author.id)

            # Compruebo comentario 
            query = "SELECT pk_id_comment FROM Comment WHERE pk_id_comment = %s"
            valores = (comment.id, )
            mycursor.execute(query, valores)
            resultado = mycursor.fetchone()
            if(resultado is not None):
                print(resultado)
                print(mycursor.rowcount, "NO insertado el \"Comment\" con id: " + comment.id)
            else:   # no tenemos este submission -> lo anhadimos
                query = "INSERT INTO Comment (pk_id_comment, id_parentComment, body, id_author, id_submission) VALUES (%s, %s, %s, %s, %s)"
                valores = (comment.id, comment.parent_id, comment.body, comment.author.id, comment.submission.id)
                if enable_comment_insert:
                    mycursor.execute(query, valores)
                    print(mycursor.rowcount, "Insertado el \"Comment\" con id: "+comment.id)
        
        # Flag para controlar las modificaciones de la BBDD
        if(enable_commit):
            db.commit()
            print("Se ha actualizado BBDD.")            