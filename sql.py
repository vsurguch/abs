
import datetime
from sqlalchemy import create_engine
from sqlalchemy import  Table, Column, Integer, String, DateTime, MetaData, ForeignKey, PrimaryKeyConstraint
from sqlalchemy.orm import mapper, sessionmaker, relationship, backref
from reader import reader


class Post(object):
    def __init__(self, date, question, answer):
        self.date = date
        self.question = question
        self.answer = answer

    def __repr__(self):
        return "<Post on {}: {}>".format(self.date, self.answer)

class Tag(object):
    def __init__(self, tag):
        self.tag = tag

    def __repr__(self):
        return '<Tag: {}>'.format(self.tag)


class Database(object):
    def __init__(self):

        self.engine = create_engine('mysql+pymysql://surguch:sql0000@188.226.188.24:3306/abs?charset=utf8',
                                    echo=False)
        metadata = MetaData()
        metadata.bind = self.engine

        posts = Table('posts', metadata, Column('id', Integer, primary_key=True, autoincrement=True),
                      Column('date', DateTime),
                      Column('question', String(4096)),
                      Column('answer', String(4096)))

        tags = Table('tags', metadata, Column('id', Integer, primary_key=True),
                     Column('tag', String(64), unique=True))

        tags_to_posts = Table('association', metadata,
                              Column('post_id', Integer, ForeignKey('posts.id')),
                              Column('tag_id', Integer, ForeignKey('tags.id')),
                              PrimaryKeyConstraint('post_id', 'tag_id'))

        metadata.create_all(self.engine)

        # mapper(Tag, tags)
        mapper(Tag, tags, properties={'posts': relationship(Post, secondary=tags_to_posts,
                                                              primaryjoin=(posts.c.id == tags_to_posts.c.post_id),
                                                              secondaryjoin=(tags.c.id == tags_to_posts.c.tag_id),
                                                              backref=('posts'))})

        mapper(Post, posts, properties={'tags': relationship(Tag, secondary=tags_to_posts,
                                                               primaryjoin=(posts.c.id == tags_to_posts.c.post_id),
                                                               secondaryjoin=(tags.c.id == tags_to_posts.c.tag_id),
                                                               backref=('tags'))})

        self.Session = sessionmaker(bind=self.engine)

        # self.change_encoding()

    def close_db(self):
        self.engine.dispose()

    def add_post(self, session, date, question, answer, tags):
        if len(question) > 4096:
            question = question[0:4096]
        if len(answer) > 4096:
            answer = answer[0:4096]
        post = Post(date, question, answer)
        for tag in tags:
            fetched = session.query(Tag).filter_by(tag=tag).all()
            if len(fetched) == 0:
                a_tag = Tag(tag)
                session.add(a_tag)
                session.commit()
            else:
                a_tag = fetched[0]
            post.tags.append(a_tag)
        session.add(post)
        session.commit()

    def delete_post(self, session, id):
        post = session.query(Post).filter_by(id=id).first()
        session.delete(post)
        session.commit()

    def get_tags(self, session):
        tags = session.query(Tag).all()
        return tags

    def get_first_post(self, session):
        post = session.query(Post).first()
        return post

    def get_count(self, session):
        count = session.query(Post).count()
        return count

    def get_last_post(self, session):
        offset = self.get_count(session) - 1
        last = session.query(Post).offset(offset).first()
        return last

    def update_post(self, session, post, date, question, answer, tags):
        post.question = question
        post.answer = answer
        post.date = date
        post.tags.clear()
        tags = [tag.encode('utf-8') for tag in tags]
        for tag in tags:
            fetched = session.query(Tag).filter_by(tag=tag).all()
            if len(fetched) == 0:
                a_tag = Tag(tag)
                session.add(a_tag)
                session.commit()
            else:
                a_tag = fetched[0]
            post.tags.append(a_tag)
        session.commit()

    def select_portion(self, session, offset, limit):
        selection = session.query(Post).offset(offset).limit(limit).all()
        return selection

    def change_encoding(self):
        conn = self.engine.connect()
        # cursor = conn.cursor()
        conn.execute("ALTER DATABASE `%s` CHARACTER SET 'utf8' COLLATE 'utf8_unicode_ci'" % 'abs')
        sql = "SELECT DISTINCT(table_name) FROM information_schema.columns WHERE table_schema = '%s'" % 'abs'
        results = conn.execute(sql)
        for row in results:
            sql = "ALTER TABLE `%s` convert to character set DEFAULT COLLATE DEFAULT" % (row[0])
            conn.execute(sql)
        conn.close()


def main():
    db = Database()
    filename = 'abs.txt'
    post_generator = reader(filename)
    session = db.Session()

    # parse and add data
    # for post in post_generator:
    #     s = post.split('MSK')
    #     strdate = s[0][-18:-1]
    #     date = datetime.datetime.strptime(strdate, '%m/%d/%y %H:%M:%S')
    #     question = s[0]
    #     answer = s[1]
    #     tags = []
    #     db.add_post(session, date, question, answer, tags)

    # get all posts
    # posts = session.query(Post).all()
    # for post in posts:
    #     print(post.question)

    # get all tags
    # tags = db.get_tags(session)
    # print(tags)


    # get first post
    # first = db.get_first_post(session)

    # get last post
    print(db.get_count(session))
    last = db.get_last_post(session)
    print(last)

    # update post
    # db.update_post(session, last, question, answer, who_asked, date, tags)

    # get portion
    # portion = db.select_portion(session, 2, 4)
    # for post in portion:
    #     print(post.question)
    #     print('-----------------')
    #     print(post.answer)
    #     print('=================')


    session.close()

if __name__ == '__main__':
    main()

# print(engine)
# conn = engine.connect()
# # cursor = conn.cursor()
# # conn.execute('create database posts')
# # conn.execute('use posts')
# # conn.execute('create database abs')
# dbs = conn.execute('show databases').fetchall()
#
# print(dbs)
# conn.close()















