import pandas as pd
import praw
import json
from praw.models import MoreComments

client_secrets = open('client_secrets.json',)

data = json.load(client_secrets)

reddit = praw.Reddit(user_agent="Comment Estraction (by u/macarracedo)",
                                client_id=data['client_id'],client_secret=data['client_secret'])

url = "https://www.reddit.com/r/cancer/comments/ov5ozy/so_today_is_my_last_chemo_treatment/"

submission = reddit.submission(url=url)

posts = []
for top_level_comment in submission.comments[1:]:
    if isinstance(top_level_comment, MoreComments):
        continue
    posts.append(top_level_comment.body)
posts = pd.DataFrame(posts,columns=["body"])

indexNames = posts[(posts.body == '[removed]') | (posts.body == '[deleted]')].index
posts.drop(indexNames, inplace=True)
print(posts)