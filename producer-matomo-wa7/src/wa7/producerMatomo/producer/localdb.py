""" pour retenir les success et le tampon """
from sqlalchemy import create_engine, select, Date, MetaData, Table, Column, String, Integer
import logging

class SqlHandler:
    """ traite le tampon dans une base local sql"""

    def __init__(self, db_uri):
        # self.engine = create_engine(db_uri,echo=True, future=True)
        self.engine = create_engine(db_uri, future=True)

        # Create a metadata instance
        self.metadata = MetaData(bind=self.engine)
        # Declare a table
        self.success = Table('LastSuccess', self.metadata,
                             Column('id', Integer, primary_key=True),
                             Column('date', Date, default='1970-01-01'),
                             Column('value', String))
        self.buffer = Table('Buffer', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('date', Date, default='1970-01-01'),
                            Column('value', String))
        # Create all tables
        self.metadata.create_all()
        self.conn = self.engine.connect()
        self.conn.commit()

    def commit(self):
        """ commit :p """
        self.conn.commit()

# success

    def insert_or_update_success(self, date, value):
        """ update une valeur si presente, insert sinon """
        if self.search_success(value):
            self.update_success(date, value)
        else:
            self.insert_success(date, value)

    def insert_success(self, date, value):
        """ insert la date et la valeur lors d'un success """
        ins = self.success.insert().values(
            date=date,
            value=value)
        self.conn.execute(ins)

    def update_success(self, date, value):
        """ mets à jour la date d'un success """
        update_st = self.success.update().where(self.success.c.value == value).values(date=date)
        self.conn.execute(update_st)

    def search_success(self, value):
        """ retourne le premier la ligne du premier success ayant la valeur """
        select_st = select(self.success).where(self.success.c.value == value)
        res = self.conn.execute(select_st).first()
        return res

    def get_all_success(self):
        """ retourne l'ensemble des success """
        select_st = select(self.success)
        res = self.conn.execute(select_st)
        return res

    def delete_old_success(self, date):
        """ delete toutes les lignes dont la date est inferieur à "date" """
        delete_st = self.success.delete().where(self.success.c.date < date)
        self.conn.execute(delete_st)
        self.conn.commit()

# buffer

    def insert_buffer(self, date, value):
        """ insert le couple date,valeur dans la table buffer """
        ins = self.buffer.insert().values(
            date=date,
            value=value)
        self.conn.execute(ins)

    def get_all_buffer(self):
        """ retourne l'ensemble des couples date,valeur de la table buffer """
        select_st = select(self.buffer)
        res = self.conn.execute(select_st)
        return res

    def delete_buffer(self, myid):
        """ delete une ligne de buffer avec l'id myid """
        delete_st = self.buffer.delete().where(self.buffer.c.id == myid)
        self.conn.execute(delete_st)

