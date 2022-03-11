import datetime

from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, BigInteger, Float, Boolean, ForeignKey, DateTime, Date, insert, select, func, distinct, \
    update, and_, or_, not_, between, delete
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class Database:
    def  __init__(self):
        self.metadata = MetaData()

    def if_not_exist(self, PASSWORD, NAME_BASE):
        connection = psycopg2.connect(user="postgres", password=PASSWORD)
        connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = connection.cursor()
        sql_create_database = cursor.execute('create database ' + NAME_BASE)
        cursor.close()
        connection.close()

    def create_eng(self, PASSWORD, NAME_BASE):
        self.engine = create_engine("postgresql+psycopg2://postgres:" + PASSWORD + "@localhost/" + NAME_BASE)

    def create_tables(self):
        self.users = Table('users', self.metadata,#improve time
                      Column('id', Integer, primary_key=True),
                      Column('username', String(32), nullable=False),
                      Column('tg_id', BigInteger, nullable=False, unique=True),
                      Column('balance', Float, nullable=False),
                      Column('passcode', String(7), nullable=False),
                      Column('fullname', String(128)),
                      Column('email', String(128)),
                      Column('is_registered', Boolean, default=False)
                      )

        self.wash_records = Table('wash_records', self.metadata,
                             Column('id', Integer, primary_key=True),
                             Column('begin', DateTime, nullable=False),
                             Column('finish', DateTime, nullable=False),
                             Column('washer', Integer, nullable=False),
                             Column('user_tg_id',  ForeignKey('users.tg_id'))
                             )

        self.gym_records = Table('gym_records', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('begin', DateTime, nullable=False),
                            Column('finish', DateTime, nullable=False),
                            Column('user_tg_id', ForeignKey('users.tg_id'))
                            )

        self.meet_records = Table('meet_records', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('begin', DateTime, nullable=False),
                            Column('finish', DateTime, nullable=False),
                            Column('user_tg_id', ForeignKey('users.tg_id')),
                            Column('is_approved', Boolean, default=False)
                             )

        self.wash_photo_links = Table('wash_photo_links', self.metadata,
                            Column('id', Integer, primary_key=True),
                            Column('link', String(400), nullable=False),
                            Column('day', Date, unique=True)
                                 )

        self.gym_photo_links = Table('gym_photo_links', self.metadata,
                                 Column('id', Integer, primary_key=True),
                                 Column('link', String(400), nullable=False),
                                 Column('day', Date, unique=True)
                                 )

        self.meet_photo_links = Table('meet_photo_links', self.metadata,
                                Column('id', Integer, primary_key=True),
                                Column('link', String(400), nullable=False),
                                Column('day', Date, unique=True)
                                )

        self.working_passcodes = Table('working_passcodes', self.metadata,
                                  Column('id', Integer, primary_key=True),
                                  Column('passcode', String(7), nullable=False, default='0007573'),
                                  Column('day', Date)
                                  )

        self.metadata.create_all(self.engine)

    def connection(self, sql_command):
        conn = self.engine.connect()
        return conn.execute(sql_command)

    def add_user(self, user, id, code='0007573', money=0, name=None, mail=None):
        self.connection(insert(self.users).values
                        (username = user,
                         tg_id = id,
                         balance = money,
                         passcode = code,
                         fullname = name,
                         email = mail,
                         is_registered = False))

    def get_user_by_id(self, tg_id):
        return self.connection(select([self.users]).where(self.users.c.tg_id == tg_id)).fetchall()

    def get_user_by_username(self, username):
        return self.connection(select([self.users]).where(self.users.c.username == username)).fetchall()

    def users_count(self):
        return self.connection(select(func.count(distinct(self.users.c.tg_id)))).fetchone()

    def get_user_with_balance(self):
        return self.connection(select(self.users).where(self.users.c.balance > 0)).fetchall()

    def change_balance(self, tg_id, diff):
        self.connection(update(self.users).where(and_(self.users.c.balance + diff > 0,
                                          self.users.c.tg_id == tg_id)).values(balance = self.users.c.balance + diff))

    def change_passcode(self, tg_id, new_passcode):
        self.connection(update(self.users).where(self.users.c.tg_id == tg_id).values(passcode = new_passcode))

    def change_fullname(self, tg_id, new_fullname):
        self.connection(update(self.users).where(self.users.c.tg_id == tg_id).values(fullname = new_fullname))

    def register_user(self, tg_id):
        self.connection(update(self.users).where(self.users.c.tg_id == tg_id).values(is_registered = True))

    def add_record(self, name_table, begin, end, user_tg_id, washer=0):
        if (washer == 0):
            if (name_table == self.wash_records):
                raise Exception("Wrong table, you didn't specify the washer")
            self.connection(insert(name_table).values(begin = begin,
                                                      finish = end,
                                                      user_tg_id = user_tg_id))
        else:
            if (name_table != self.wash_records):
                raise Exception("Wrong table, what the fuck is washer in gym or meet??")
            self.connection(insert(name_table).values(begin = begin,
                                                      finish = end,
                                                      washer = washer,
                                                      user_tg_id = user_tg_id))

    def approve_meet_record(self, record_id):
        self.connection(update(self.meet_records).where(self.meet_records.c.id == record_id).values(is_approved = True))

    def count_records(self, name_table, beg, end):
        return self.connection(select(func.count(distinct(name_table.c.id))).where(and_(name_table.c.begin >= beg,
                                                                   name_table.c.finish <= end))).scalar()

    def delete_inactive(self):
        self.connection(delete(self.users).where(self.users.c.is_registered == False))

    def delete_by_time(self, name_table, date, delta):#delta MUST be timedelta(if u bydlo and don't understand)
        self.connection(delete(name_table).where((date - name_table.c.begin) >= delta))

    def get_records(self, name_table, day, delta):#delta MUST be timedelta(if u bydlo and don't understand)
        return self.connection(select([name_table]).where(and_(name_table.c.begin >= day,
                                                   name_table.c.finish <= day + delta))).fetchall()

    def get_user_records(self, name_table, tg_id, start, delta):#delta MUST be timedelta(if u bydlo and don't understand)
        return self.connection(select([name_table]).where(and_(name_table.c.begin >= start,
                                                   name_table.c.finish <= start + delta,
                                                   name_table.c.user_tg_id == tg_id))).fetchall()

    def get_record_id(self, name_table, record_id):
        return self.connection(select([name_table]).where(name_table.c.id == record_id)).fetchall()

    def delete_record(self, name_table, record_id):
        self.connection(delete(name_table).where(name_table.c.id == record_id))

    def update_link(self, name_table, links, days):
        self.connection(insert(name_table).values(link = links, day = days))

    def get_link(self, name_table, days):
        return self.connection(select([name_table]).where(name_table.c.day == days)).fetchall()

    def delete_link(self, name_table, days):
        self.connection(delete(name_table).where(name_table.c.day == days))

    def update_passcode(self, code, days):
        self.connection(insert(self.working_passcodes).values(passcode = code, day = days))

    def get_passcode(self, days):
        return self.connection(select(self.working_passcodes).where(self.working_passcodes.c.day == days)).fetchall()

    def delete_passcode(self, days):
        return self.connection(delete(self.working_passcodes).where(self.working_passcodes.c.day <= days))

#DB = Database()
#DB.if_not_exist(password, server_name) - only if you launch first time(create server)
#DB.create_eng('passwird', 'server_name')
#DB.create_tables()

