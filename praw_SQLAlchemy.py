try:
    import praw
    import json
    import mysql.connector as mysql
    from sqlalchemy import create_engine, select, Column, Integer, Float, String, Boolean, ForeignKey
    from sqlalchemy.orm import Session, sessionmaker, relationship
    from sqlalchemy.ext.declarative import declarative_base
    from praw.reddit import Submission
    from praw.models import MoreComments

    print("All modules loaded")
except:
    print("No modules found")

credentials = 'client_secrets.json'

enable_commit = True
enable_submission_insert = True
enable_comment_insert = True
enable_redditor_insert = True
post_limit = 1000
comment_depth_limit = 4

with open(credentials) as f:
    creds = json.load(f)

# Config Reddit connection
reddit = praw.Reddit(client_id=creds['client_id'],
                     client_secret=creds['client_secret'],
                     user_agent=creds['user_agent'],
                     redirect_uri=creds['redirect_uri'],
                     refresh_token=creds['refresh_token'])
reddit.validate_on_submit = True

# Configuración de DB local con SQLAlchemy
Base = declarative_base()
engine = create_engine('mysql+pymysql://praw_user:reddit@localhost/sql_alchemy')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)
session = DBSession()


# Creacion de tablas
class Subreddit(Base):
    __tablename__ = 'subreddit'
    pk_id_subreddit = Column('pk_id_subreddit', Integer, autoincrement=True, primary_key=True)
    name = Column ('name', String, unique=True)

    def __repr__(self):
        return "pk_id_subreddit=%d name=%s" % (self.pk_id_subreddit, self.name)

class Redditor(Base):
    __tablename__ = 'redditor'
    pk_id_redditor = Column('pk_id_redditor', Integer, autoincrement=True, primary_key=True)
    id_redditor = Column('id_redditor', String, unique=True, nullable=False)
    name = Column('name', String, unique=True, nullable=False)  # unique because it's an username
    verified = Column('verified', Boolean)
    total_karma = Column('total_karma', Integer)

    def __repr__(self):
        return "pk_id_redditor=%d id_redditor=%s name=%s is_verified=%r" % (
        self.pk_id_redditor, self.id_redditor, self.name, self.verified, self.total_karma)


class Submission(Base):
    __tablename__ = 'submission'
    pk_id_submission = Column('pk_id_submission', Integer, autoincrement=True, primary_key=True)
    id_submission = Column('id_submission', String, unique=True)
    title = Column('title', String)
    selftext = Column('selftext', String)
    fk_id_author = Column('fk_id_author', Integer, ForeignKey(Redditor.pk_id_redditor), unique=True)
    id_author = Column('id_author', String, unique=True)
    ups = Column('ups', Integer)
    downs = Column('downs', Integer)
    upvote_ratio = Column('upvote_ratio', Float)
    url = Column('url', String)

    def __repr__(self):
        return "pk_id_submission=%d id_submission=%s title=%s sefltext=%s fk_id_author=%d id_author=%s ups=%d downs=%d upvote_ratio=%f url=%s" \
               % (
               self.pk_id_submission, self.id_submission, self.title, self.sefltext, self.fk_id_author, self.id_author,
               self.ups, self.downs, self.upvote_ratio, self.url)


class Comment(Base):
    __tablename__ = 'comment'
    pk_id_comment = Column('pk_id_comment', Integer, autoincrement=True, primary_key=True)
    id_comment = Column('id_comment', String, unique=True)
    fk_id_submission = Column('fk_id_submission', Integer, ForeignKey(Submission.pk_id_submission), unique=True)
    id_submission = Column('id_submission', String, unique=True)
    fk_id_author = Column('fk_id_author', Integer, ForeignKey(Redditor.pk_id_redditor), unique=True)
    id_author = Column('id_author', String, ForeignKey(Redditor.pk_id_redditor), unique=True)
    id_parent = Column('id_parent', String)
    body = Column('body', String)
    ups = Column('ups', Integer)
    downs = Column('downs', Integer)
    depth = Column('depth', Integer)

    def __repr__(self):
        return "pk_id_comment=%d id_comment=%s fk_id_submission=%d id_submission=%s fk_id_author=%d id_author=%s fk_id_parent=%d id_parent=%s body=%s ups=%d downs=%d depth=%d" \
               % (self.pk_id_comment, self.id_comment, self.fk_id_submission, self.id_submission, self.fk_id_author,
                  self.id_author, self.id_parent, self.body, self.ups, self.downs, self.depth)


print("Muestro session: ", session)
session.commit()

subreddit = reddit.subreddit("cancer")
hot_subreddit = subreddit.hot(limit=post_limit)

query = ""
for submission in hot_subreddit:
    try:
        if not submission.stickied:     #Descarta mensajes fijados
            print(40 * '-')

            print("title : {}, submission.id: {}, author: {}, author.id: {}, num_comments: {}".format(
                submission.title,
                submission.id,
                submission.author.name,
                submission.author.id,
                submission.num_comments))
            # print("Text: \n" + submission.selftext)

            # Compruebo autor del submission
            db_author = select(Redditor.id_redditor).where(Redditor.id_redditor == submission.author.id)
            resultado = session.execute(db_author).fetchone()  # devuelve uno
            if resultado is not None:  # conocemos al redditor
                print("NO insertado el \"Redditor\" con id: " + submission.author.id)
            else:  # no tenemos este submission -> lo anhadimos
                db_author = Redditor(id_redditor=submission.author.id, name=submission.author.name, \
                                     verified=submission.author.verified, total_karma=submission.author.total_karma)
                if enable_redditor_insert:
                    session.add(db_author)
                    # session.commit()
                    print("Insertado el \"Redditor\" con id: " + submission.author.id)

            # Compruebo el submission
            db_submission = select(Submission.id_submission).where(Submission.id_submission == submission.id)
            resultado = session.execute(db_submission).fetchone()  # devuelve uno

            if (resultado is not None):  # ya tenemos el submission
                print("NO insertado el \"Submission\" con id: " + submission.id)
            else:  # no tenemos este submission -> lo anhadimos
                fk_id_author = select(Redditor.pk_id_redditor).where(Redditor.id_redditor == submission.author.id)
                fk_id_author = session.execute(fk_id_author).fetchone()[0]
                print("Here! fk_id_author: ", fk_id_author)
                db_submission = Submission(id_submission=submission.id, title=submission.title,
                                           selftext=submission.selftext, \
                                           fk_id_author=fk_id_author, id_author=submission.author.id,
                                           ups=submission.ups,
                                           downs=submission.downs, \
                                           upvote_ratio=submission.upvote_ratio, url=submission.url)
                if enable_submission_insert:
                    session.add(db_submission)
                    # session.commit()
                    print("Insertado el \"Submission\" con id: " + submission.id)


            # Recorremos los comentario de las publicaciones (todos)
            comments = submission.comments.list()
            for comment in comments:
                if comment_depth_limit is not None and comment.depth < comment_depth_limit:
                    try:
                        print(20 * '-')
                        print("author: {}, author.id: {}, comment.id: {}, parent.id: {}, depth: {}".format(
                            comment.author.name,
                            comment.author.id,
                            comment.id,
                            comment.parent_id,
                            comment.depth))
                        # print(comment.body)
                        # Compruebo autor del comentario

                        db_author = select(Redditor.id_redditor).where(Redditor.id_redditor == comment.author.id)
                        resultado = session.execute(db_author)  # devuelve uno
                        print(resultado)
                        resultado = resultado.fetchone()
                        if resultado is not None:
                            print(resultado)
                            print("NO insertado el \"Redditor\" con id: " + comment.author.id)
                        else:  # no conocemos al autor -> lo anhadimos
                            db_author = Redditor(id_redditor=comment.author.id, name=comment.author.name, \
                                                 verified=comment.author.verified, total_karma=comment.author.total_karma)
                            if enable_redditor_insert:
                                session.add(db_author)
                                print("Insertado el \"Redditor\" con id: " + comment.author.id)

                        # Compruebo comentario

                        db_comment = select(Comment.pk_id_comment).where(Comment.id_comment == comment.id)
                        resultado = session.execute(db_comment).fetchone()  # devuelve uno

                        if resultado is not None:
                            print(resultado)
                            print("NO insertado el \"Comment\" con id: " + comment.id)
                        else:  # no tenemos este submission -> lo anhadimos
                            fk_id_submission = select(Submission.pk_id_submission).where(Submission.id_submission == submission.id)
                            fk_id_submission = session.execute(fk_id_submission).fetchone()[0]

                            fk_id_author = select(Redditor.pk_id_redditor).where(Redditor.id_redditor == comment.author.id)
                            fk_id_author = session.execute(fk_id_author).fetchone()[0]

                            '''
                                "parent_id" - The ID of the parent comment (prefixed with "t1_").
                                If it is a top-level comment, this returns the submission ID instead
                                (prefixed with "t3_").
                            '''

                            db_comment = Comment(id_comment=comment.id, fk_id_submission=fk_id_submission, id_submission=submission.id,
                                                 fk_id_author=fk_id_author, id_author=comment.author.id, id_parent=comment.parent_id,
                                                 body=comment.body, ups=comment.ups, downs=comment.downs, depth=comment.depth)
                            if enable_comment_insert:
                                session.add(db_comment)
                                print("Insertado el \"Comment\" con id: " + comment.id)
                    except AttributeError:
                        print("Error en Comment: AttributeError.")
                    except TypeError:
                        print("Error en Comment: TypeError.")
                    except:
                        print("Error en Comment.")
        if enable_commit:
            try:
                session.commit()
                print("Actualizada la BBDD")
            except:
                session.rollback()
                print("No se ha podido actualizar la BBDD. Rollback...")
    except:
        print("Error en Submission.")
