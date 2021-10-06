from tokenize import String

from sqlalchemy import Table, Column, Integer, String, ForeignKey, create_engine
from sqlalchemy.orm import relationship, Session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class User(Base):
    __tablename__ = "Person"

    id = Column('id', Integer, primary_key=True)
    age = Column('age', Integer, unique=True)
    name = Column('name', String, unique=False)


engine = create_engine('sqlite:///user1.db')
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)

session = Session()
user = User()
user.id = 1
user.age = 15
user.name = "pablo"

session.add(user)
session.commit()

users = session.query(User).all()
for user in users:
    print(("User with id ", user.id, "age ", user.age, "and name", user.name))
session.close()
