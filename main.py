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

    def add_user(self, user, id, code='0007573', money=0, name=None, mail=None):
        ins = insert(self.users)
        conn = self.engine.connect()

        r = conn.execute(ins,
            username = user,
            tg_id = id,
            balance = money,
            passcode = code,
            fullname = name,
            email = mail,
            is_registered = False)

    def get_user_by_id(self, tg_id):
        conn = self.engine.connect()

        s = select([self.users]).where(self.users.c.tg_id == tg_id)
        r = conn.execute(s)
        #print(r.fetchall()) - for tests
        return r.fetchall()

    def get_user_by_username(self, username):
        conn = self.engine.connect()

        s = select([self.users]).where(self.users.c.username == username)
        r = conn.execute(s)
        #print(r.fetchall()) - for tests
        return r.fetchall()

    def users_count(self):
        conn = self.engine.connect()

        r = func.count(distinct(self.users.c.tg_id))
        s = select(r)
        rs = conn.execute(s)
        #print(rs.fetchone()) - for tests
        return rs.fetchone()

    def get_user_with_balance(self):
        conn = self.engine.connect()

        s = select(self.users).where(self.users.c.balance > 0)
        r = conn.execute(s)
        #print(r.fetchall()) - for tests
        return r.fetchall()

    def change_balance(self, tg_id, diff):
        conn = self.engine.connect()

        s = update(self.users).where(and_(self.users.c.balance + diff > 0,
                                          self.users.c.tg_id == tg_id)).values(balance = self.users.c.balance + diff)
        rs = conn.execute(s)

    def change_passcode(self, tg_id, new_passcode):
        conn = self.engine.connect()

        s = update(self.users).where(self.users.c.tg_id == tg_id).values(passcode = new_passcode)
        rs = conn.execute(s)

    def change_fullname(self, tg_id, new_fullname):
        conn = self.engine.connect()

        s = update(self.users).where(self.users.c.tg_id == tg_id).values(fullname = new_fullname)
        rs = conn.execute(s)

    def register_user(self, tg_id):
        conn = self.engine.connect()

        s = update(self.users).where(self.users.c.tg_id == tg_id).values(is_registered = True)
        rs = conn.execute(s)

    def add_wash_record(self, begin, end, washer, user_tg_id): #improve
        conn = self.engine.connect()

        ins = insert(self.wash_records)
        r = conn.execute(ins,
                         begin = begin,
                         finish = end,
                         washer = washer,
                         user_tg_id = user_tg_id)

    def add_gym_record(self, begin, end, user_tg_id): #improve
        conn = self.engine.connect()

        ins = insert(self.gym_records)
        r = conn.execute(ins,
                         begin = begin,
                         finish = end,
                         user_tg_id = user_tg_id)

    def add_meet_record(self, begin, end, user_tg_id): #improve
        conn = self.engine.connect()

        ins = insert(self.meet_records)
        r = conn.execute(ins,
                         begin = begin,
                         finish = end,
                         user_tg_id = user_tg_id)

    def approve_meet_record(self, record_id):
        conn = self.engine.connect()

        s = update(self.meet_records).where(self.meet_records.c.id == record_id).values(is_approved = True)
        r = conn.execute(s)

    def count_gym_records(self, beg, end):
        conn = self.engine.connect()

        s = select(func.count(distinct(self.gym_records.c.id))).where(and_(self.gym_records.c.begin >= beg,
                                                                   self.gym_records.c.finish <= end))
        rs = conn.execute(s).scalar()
        #print(rs)
        return rs
    def count_wash_records(self, beg, end):
        conn = self.engine.connect()

        s = select(func.count(distinct(self.wash_records.c.id))).where(or_(self.wash_records.c.begin.between(beg, end),
                                                              self.wash_records.c.finish.between(beg, end)))
        rs = conn.execute(s).scalar()
        #print(rs)
        return rs

    def count_meet_records(self, beg, end):
        conn = self.engine.connect()

        s = select(func.count(distinct(self.meet_records.c.id))).where(or_(self.meet_records.c.begin.between(beg, end),
                                                              self.meet_records.c.finish.between(beg, end)))
        rs = conn.execute(s).scalar()
        #print(rs)
        return rs

    def delete_inactive(self):
        conn = self.engine.connect()

        s = delete(self.users).where(self.users.c.is_registered == False)
        rs = conn.execute(s)

    def delete_by_time(self, name_table, date, delta):#delta MUST be timedelta(if u bydlo and don't understand)
        conn = self.engine.connect()

        s = delete(name_table).where((date - name_table.c.begin) >= delta)
        rs = conn.execute(s)

    def get_wash_records(self, day):#improve
        day_start = day
        day_end = day_start + datetime.timedelta(days=1)

        conn = self.engine.connect()

        s = select([self.wash_records]).where(and_(self.wash_records.c.begin >= day_start,
                                                   self.wash_records.c.finish <= day_end))
        rs = conn.execute(s).fetchall()
        return rs

    def get_gym_records(self, day):
        day_start = day
        day_end = day_start + datetime.timedelta(days=1)

        conn = self.engine.connect()

        s = select([self.gym_records]).where(and_(self.gym_records.c.begin >= day_start,
                                                   self.gym_records.c.finish <= day_end))
        rs = conn.execute(s).fetchall()
        return rs

    def get_meet_records(self, day):
        day_start = day
        day_end = day_start + datetime.timedelta(days=1)

        conn = self.engine.connect()

        s = select([self.meet_records]).where(and_(self.meet_records.c.begin >= day_start,
                                                   self.meet_records.c.finish <= day_end))
        rs = conn.execute(s).fetchall()
        return rs

    def get_user_wash_records(self, tg_id, start, end):
        conn = self.engine.connect()

        s = select([self.wash_records]).where(and_(self.wash_records.c.begin >= start,
                                                   self.wash_records.c.finish <= end,
                                                   self.wash_records.c.user_tg_id == tg_id))

        rs = conn.execute(s).fetchall()
        return rs

    def get_user_gym_records(self, tg_id, start, end):
        conn = self.engine.connect()

        s = select([self.wash_records]).where(and_(self.gym_records.c.begin >= start,
                                                   self.gym_records.c.finish <= end,
                                                   self.gym_records.c.user_tg_id == tg_id))
        rs = conn.execute(s).fetchall()
        return rs

    def get_user_meet_records(self, tg_id, start, end):
        conn = self.engine.connect()

        s = select([self.meet_records]).where(and_(self.meet_records.c.begin >= start,
                                                   self.meet_records.c.finish <= end,
                                                   self.meet_records.c.user_tg_id == tg_id))
        rs = conn.execute(s).fetchall()
        return rs

    def get_wash_record_id(self, record_id):
        conn = self.engine.connect()

        s = select([self.wash_records]).where(self.wash_records.c.id == record_id)

        rs = conn.execute(s).fetchall()
        return rs

    def get_gym_record_id(self, record_id):
        conn = self.engine.connect()

        s = select([self.gym_records]).where(self.gym_records.c.id == record_id)

        rs = conn.execute(s).fetchall()
        return rs

    def get_meet_record_id(self, record_id):
        conn = self.engine.connect()

        s = select([self.meet_records]).where(self.meet_records.c.id == record_id)

        rs = conn.execute(s).fetchall()
        return rs

    def delete_wash_record(self, record_id):
        conn = self.engine.connect()

        s = delete(self.wash_records).where(self.wash_records.c.id == record_id)
        rs = conn.execute(s)

    def delete_gym_record(self, record_id):
        conn = self.engine.connect()

        s = delete(self.gym_records).where(self.gym_records.c.id == record_id)
        rs = conn.execute(s)

    def delete_meet_record(self, record_id):
        conn = self.engine.connect()

        s = delete(self.meet_records).where(self.meet_records.c.id == record_id)
        rs = conn.execute(s)

    def update_link(self, name_table, links, days):
        conn = self.engine.connect()

        ins = insert(name_table)
        r = conn.execute(ins,
                         link=links,
                         day=days)

    def get_link(self, name_table, days):
        conn = self.engine.connect()

        s = select([name_table]).where(name_table.c.day == days)

        rs = conn.execute(s).fetchall()
        return rs

    def delete_link(self, name_table, days):
        conn = self.engine.connect()

        s = delete(name_table).where(name_table.c.day == days)
        rs = conn.execute(s)

    def update_passcode(self, code, days):
        conn = self.engine.connect()

        ins = insert(self.working_passcodes)
        r = conn.execute(ins,
                         passcode=code,
                         day=days)

    def get_passcode(self, days):
        conn = self.engine.connect()

        s = select([self.working_passcodes]).where(self.working_passcodes.c.day == days)

        rs = conn.execute(s).fetchall()
        return rs

#DB = Database() - create object
#DB.if_not_exist(password, server_name) - only if you launch first time(create server)
#DB.create_eng('password', 'server_name') - create engine
#DB.create_tables()
