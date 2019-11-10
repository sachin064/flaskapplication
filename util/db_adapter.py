import mysql.connector


class DbAdapter:

    def __init__(self, db_config):
        self.host = db_config.get('host')
        self.port = db_config.get('port')
        self.db = db_config.get('db')
        self.user = db_config.get('user')
        self.password = db_config.get('password')

    def get_connection(self):
        try:
            conn = mysql.connector.connect(
                host=self.host,
                port=self.port,
                db=self.db,
                user=self.user,
                passwd=self.password
            )
        except Exception as e:
            return False, None, str(e)
        return True, conn, None

    def fetch(self, query, _type=None):
        try:
            conn_status, conn, err = self.get_connection()
            if conn_status:
                cur = conn.cursor()
                cur.execute(query)
                if _type == 'single':
                    data = cur.fetchone()
                else:
                    data = cur.fetchall()
                conn.close()
                return True, data, None
            else:
                return False, None, err
        except Exception as e:
            return False, None, str(e)

    def manipulate_record(self, query, value):
        try:
            conn_status, conn, err = self.get_connection()
            if conn_status:
                cur = conn.cursor()
                cur.executemany(query, value)
                conn.commit()
                rows_count = cur.rowcount
                conn.close()
                return True, rows_count, None
            else:
                return False, None, err
        except Exception as e:
            return False, None, str(e)

    def delete_record(self, query):
        try:
            conn_status, conn, err = self.get_connection()
            if conn_status:
                cur = conn.cursor()
                cur.execute(query)
                conn.commit()
                conn.close()
                return True, None
            else:
                return False, err
        except Exception as e:
            return False, str(e)
