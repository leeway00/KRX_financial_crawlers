from abc import abstractmethod
import psycopg2
import time
import dbconfig
import os
import datetime

os.environ['TZ'] = 'UTC'

KST = datetime.timezone(datetime.timedelta(hours=9))
UTC = datetime.timezone.utc


class hiroku:
    def __init__(self):
        self.connect()

    def connect(self):
        self.connection = psycopg2.connect(host=dbconfig.DB_HOST,
                                           database=dbconfig.DB_NAME,
                                           user=dbconfig.DB_USER,
                                           password=dbconfig.DB_PASS)
        self.cursor = self.connection.cursor()

    def connection_check(self):
        err_cnt = 0
        while self.connection.closed != 0:
            self.connect()
            if self.connection.closed != 0:
                err_cnt += 1
                time.sleep(60)
            if err_cnt == 10:
                raise Exception("Failed to restore Connection")
            else:
                print('Connection Restored')
        else:
            pass

    def insert(self, table, data, names: str):
        try:
            cursor = self.connection.cursor()
            user_records = ",".join(["%s"] * len(data[0]))
            insert_query = (
                f"INSERT INTO {table} \
                    ({names}) \
                        VALUES ({user_records})"
            )
            cursor.executemany(insert_query, data)
            self.connection.commit()
            return 1
        except Exception as e:
            self.rollback()
            print(e)
            return 0

    def rollback(self):
        self.cursor.execute('rollback')
        self.connection.commit()

    def get_latest_date(self, table, ticker):
        try:
            self.cursor.execute(
                f'select date from {table} \
                    where ticker = \'{ticker}\' \
                        order by date desc limit 1')
            self.connection.commit()
            res = self.cursor.fetchall()[0][0]
            return res, f'{res.year}-{res.month}-{res.day}'
        except Exception as e:
            self.rollback()
            print(e)
            