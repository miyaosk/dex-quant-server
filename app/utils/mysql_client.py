import traceback

import pymysql
from dbutils.pooled_db import PooledDB
from retry import retry
import os
from app.config import mysql_db


class Singleton(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        key = str(cls) + str(args) + str(kwargs)
        if key not in cls._instances:
            cls._instances[key] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[key]


class MysqlSQL(object, metaclass=Singleton):
    def __init__(self, db_conf=mysql_db):
        self.db_conf = db_conf
        self.create_pool()

    @retry(tries=10, delay=1, backoff=1, max_delay=3)
    def create_pool(self):
        db_conf = self.db_conf
        pool_size = os.getenv('DB_POOL_SIZE', 1)
        pool_size = int(pool_size)
        self.db_pool = PooledDB(pymysql, pool_size,
                                database=db_conf['db'],
                                user=db_conf['user'],
                                password=db_conf['password'],
                                host=db_conf['host'],
                                port=db_conf['port'])

    @property
    def conn(self):
        print('[Error] access conn by attribute')
        return self.db_pool.connection()

    @retry(tries=10, delay=1, backoff=1, max_delay=3)
    def connect(self):
        return self.db_pool.connection()

    def __del__(self):
        if self.db_pool:
            self.db_pool.close()

    def return_connction(self, conn):
        conn.close()

    def insert(self, data, table_name):
        """
            插入方法
        :param data: 数据
        :param table_name: 表名
        :return:
        """
        conn = self.connect()
        try:
            columns = ', '.join(data.keys())
            placeholders = ', '.join(['%s'] * len(data))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cur = conn.cursor()
            cur.execute(query, list(data.values()))
            cur.close()
        except Exception as e:
            traceback.print_exc()
            return 1
        else:
            pass
        finally:
            conn.commit()
            self.return_connction(conn)

    def insert_many(self, data_list, table_name):
        """
            批量插入方法
        :param data_list: 数据列表，每个元素是一个dict
        :param table_name: 表名
        :return:
        """
        if not data_list:
            return 0
        conn = self.connect()
        try:
            columns = ', '.join(data_list[0].keys())
            placeholders = ', '.join(['%s'] * len(data_list[0]))
            query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cur = conn.cursor()
            values = [list(data.values()) for data in data_list]
            cur.executemany(query, values)
            cur.close()
        except Exception as e:
            traceback.print_exc()
            return 1
        else:
            pass
        finally:
            conn.commit()
            self.return_connction(conn)

    def update(self, dt_update, dt_condition, table):
        """
            更新
        :param dt_update: 更新后的数据
        :param dt_condition:  条件
        :param table:
        :return:
        """
        conn = self.connect()
        sql = 'UPDATE %s SET ' % table + ",".join(["%s='%s'" % (k, dt_update[k].replace("'", "''")) for k in dt_update]) \
              + ' WHERE ' + ' AND '.join(
            ["%s='%s'" % (k, dt_condition[k].replace("'", "''")) for k in dt_condition]) + ';'
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        self.return_connction(conn)

    def select_where(self, table_name, condition=None, is_json=False):
        """
            查询
        :param table_name: 表名
        :param condition: 条件
        :return:
        """
        conn = self.connect()
        cur = conn.cursor()
        query = f"SELECT * FROM {table_name}"
        if condition:
            conditions = ' AND '.join([f"{key} = %s" for key in condition.keys()])
            query += f" WHERE {conditions}"
            cur.execute(query, list(condition.values()))
        else:
            cur.execute(query)
        rows = cur.fetchall()
        desc = cur.description
        conn.commit()
        cur.close()
        self.return_connction(conn)
        if is_json:
            return [dict(zip([col[0] for col in desc], row)) for row in rows]
        return rows

    def delete(self, table_name, condition):
        """
            删除数据
        :param table_name: 表名
        :param condition: 条件
        :return:
        """
        conn = self.connect()
        row_str = ''
        for i in condition:
            str_ = i + "='" + condition[i].replace("'", "''") + "'"
            row_str += str_
        sql = 'DELETE from ' + table_name + ' where ' + row_str
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        self.return_connction(conn)

    def creat_table(self, data, table_name):
        """
            创建表格
        :param data: 数据格式
        :param table_name: 表名
        :return:
        """
        conn = self.connect()
        COLstr = ''
        ColumnStyle = 'text'
        for key in data.keys():
            COLstr = COLstr + ' ' + key + ' ' + ColumnStyle + ','
        cur = conn.cursor()
        cur.execute("CREATE TABLE %s (%s)" % (table_name, COLstr[:-1]))
        conn.commit()
        cur.close()
        self.return_connction(conn)

    def execute_sql(self, sql, data=None, is_json=False):
        conn = self.connect()
        cur = conn.cursor()
        if data:
            cur.execute(sql, data)
        else:
            cur.execute(sql)
        rows = cur.fetchall()
        desc = cur.description
        conn.commit()
        cur.close()
        self.return_connction(conn)
        if is_json:
            rows = [dict(zip([col[0] for col in desc], row)) for row in rows]
        return rows

    def insert_and_get_id(self, sql, data=None):
        """INSERT 并返回自增 ID（同一连接内获取 lastrowid）。"""
        conn = self.connect()
        cur = conn.cursor()
        try:
            cur.execute(sql, data)
            conn.commit()
            return cur.lastrowid
        finally:
            cur.close()
            self.return_connction(conn)

    def execute_many(self, sql, list):
        conn = self.connect()
        cur = conn.cursor()
        cur.executemany(sql, list)
        conn.commit()
        cur.close()
        self.return_connction(conn)

    def upsert(self, data, table):
        conn = self.connect()
        try:
            cur = conn.cursor()
            columns = ', '.join([f'`{k}`' for k in data.keys()])
            placeholders = ', '.join(['%s'] * len(data))
            update_clause = ', '.join([f'`{key}` = VALUES(`{key}`)' for key in data.keys()])
            query = f"""
                INSERT INTO {table} ({columns})
                VALUES ({placeholders})
                ON DUPLICATE KEY UPDATE {update_clause}
            """
            values = list(data.values())
            cur.execute(query, values)
        except Exception as e:
            print(f"Upsert 操作失败: {e}")
            return None
        finally:
            conn.commit()
            self.return_connction(conn)


mysql = MysqlSQL()
