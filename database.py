from sqlalchemy import MetaData, Table, String, Integer, Column, insert
import sqlalchemy


DB = 'postgresql://netology:1234@127.0.0.1:5432/asyncio'


class NewDataBase:

    def __init__(self, DB):

        self.engine = sqlalchemy.create_engine(DB)
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.metadata.drop_all(self.engine)

        people = Table('people', self.metadata,
                       Column('id', Integer(), primary_key=True),
                       Column('birth_year', String()),
                       Column('eye_color', String()),
                       Column('films', String()),
                       Column('gender', String()),
                       Column('hair_color', String()),
                       Column('height', Integer()),
                       Column('homeworld', String()),
                       Column('mass', Integer()),
                       Column('name', String()),
                       Column('skin_color', String()),
                       Column('species', String()),
                       Column('starships', String()),
                       Column('vehicles', String()))

        self.metadata.create_all(self.engine)


class DataBaseWork:

    def __init__(self, DB):

        self.engine = sqlalchemy.create_engine(DB)
        self.connection = self.engine.connect()
        self.metadata = MetaData()
        self.metadata.drop_all(self.engine)

        self.people = Table('people', self.metadata,
                       Column('id', Integer(), primaty_key=True),
                       Column('birth_year', String()),
                       Column('eye_color', String()),
                       Column('films', String()),
                       Column('gender', String()),
                       Column('hair_color', String()),
                       Column('height', Integer()),
                       Column('homeworld', String()),
                       Column('mass', Integer()),
                       Column('name', String()),
                       Column('skin_color', String()),
                       Column('species', String()),
                       Column('starships', String()),
                       Column('vehicles', String()))


    def insert_people(self, people_info):
        self.ins_people = insert(self.people)
        self.connection.execute(self.ins_people, people_info)

