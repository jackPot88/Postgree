import psycopg2
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

class DB_class:
    def __init__(self, user_for_db_connection, password_for_db_connection, host_name_for_db_connection, port_for_db_connection):
        self.user_for_db_connection = user_for_db_connection
        self.password_for_db_connection = password_for_db_connection
        self.host_name_for_db_connection = host_name_for_db_connection
        self.port_for_db_connection = port_for_db_connection



    def create_db(self,db_name):
        '''create DB with name and connection parameters'''
        try:
            # Подключение к существующей базе данных
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection,
                                          host=self.host_name_for_db_connection,
                                          port=self.port_for_db_connection)
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cursor = connection.cursor()
            sql_create_database = 'create database '+str(db_name)
            cursor.execute(sql_create_database)
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def create_table(self,create_table_cols, db_name,table_name):
        '''create TABLE with name and connection parameters'''
        create_table_query = 'CREATE TABLE IF NOT EXISTS '+str(table_name)+' ('+create_table_cols+');'
                                     
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection, 
                                          database=db_name)
            cursor = connection.cursor()
            cursor.execute(create_table_query)
            print("Результат", connection.commit())
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def drop_table(self, db_name,table_name):
        '''DROP TABLE method for delete table'''
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection,  
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection, 
                                          database=db_name)

            cursor = connection.cursor()
            cursor.execute("DROP TABLE "+str(table_name))
            print("Результат", connection.commit())
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def truncate_table(self, db_name,table_name):
        '''TRUNCATE TABLE method for clear table'''
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection,
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection, 
                                          database=db_name)

            cursor = connection.cursor()
            cursor.execute("TRUNCATE TABLE "+str(table_name))
            connection.commit()
            print("Результат", connection.commit())
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def show_all_db_name(self):
        # SELECT datname FROM pg_database;
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection, 
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection)  

            cursor = connection.cursor()
            cursor.execute("SELECT datname FROM pg_database")
            rows = cursor.fetchall()
            print("Результат")
            # print(rows)
            for i in rows:
                # print(i)
                print(i[0])

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def show_all_tables_name_from_db(self, db_name): # SELECT * FROM pg_catalog.pg_tables
        '''TRUNCATE TABLE method for clear table'''
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection, 
                                          port=self.port_for_db_connection,  
                                          database=db_name) 

            cursor = connection.cursor()
            cursor.execute("SELECT * FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND  schemaname != 'information_schema'")
            rows = cursor.fetchall()
            print("Результат")
            n = 1
            print('--------------------------------')
            for i in rows:
                print(n)
                print('schemaname:',i[0])
                print('tablename:',i[1])
                print('tableowner:',i[2])
                print('tablespace:',i[3])
                print('hasindexes:',i[4])
                print('hasrules:',i[5])
                print('hastriggers:',i[6])
                print('rowsecurity:',i[7])
                print('--------------------------------')
                n+=1

        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def show_rows_name_from_table(self,db_name,table_name):
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection,  
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection, 
                                          database=db_name) 

            cursor = connection.cursor()
            cursor.execute("SELECT column_name from information_schema.columns WHERE columns.table_name='"+str(table_name)+"'")
            print("Результат")
            rows = cursor.fetchall()
            for i in rows:
                print(i[0])
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")


    def show_all_from_table(self, db_name,table_name,select_rows='*',condition=''):
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection,  
                                          port=self.port_for_db_connection, 
                                          database=db_name)

            cursor = connection.cursor()
            if condition == '':
                cursor.execute("SELECT "+select_rows+" from "+str(table_name))
                print("Результат")
                rows = cursor.fetchall()
                for i in rows:
                    for j in i:
                        print('|'+str(j)+'|',end=' ')
                    print('')
            else:
                cursor.execute("SELECT " + select_rows + " from " + str(table_name) + ' WHERE '+str(condition))
                print("Результат")
                rows = cursor.fetchall()
                for i in rows:
                    for j in i:
                        print('|' + str(j) + '|', end=' ')
                    print('')
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def insert_to_table(self, db_name,table_name, insert_colums_name,insert_data):
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection,  
                                          password=self.password_for_db_connection,
                                          host=self.host_name_for_db_connection,
                                          port=self.port_for_db_connection,  
                                          database=db_name)

            cursor = connection.cursor()
            l = insert_colums_name.split(",")
            print(l)
            v = '%s'
            d = ', %s'
            v = v+d*(len(l)-1)
            print(v)
            print(insert_colums_name)
            insert_query = f"""INSERT INTO {table_name} ({insert_colums_name}) VALUES ({v})"""
            print(insert_query)
            print(insert_data)
            cursor.execute(insert_query, insert_data) # insert_data = tuple
            connection.commit()
            print("элемент успешно добавлен")
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")

    def update_from_in_db(self, db_name,table_name,colums_name_for_set,value_for_set,condition_where):
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection,  
                                          host=self.host_name_for_db_connection, 
                                          port=self.port_for_db_connection, 
                                          database=db_name)
            cursor = connection.cursor()
            sample = []
            for i in range(len(colums_name_for_set)):
                sample.append(colums_name_for_set[i]+'='+value_for_set[i])
            s = ''.join(sample)
            cursor.execute("UPDATE " + table_name + " SET " + s.replace(' ',',') + " WHERE " + condition_where)
            connection.commit()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")


    def get_data_from_table(self, db_name,table_name,select_rows='*',condition=''):
        # здась будет скрипт который выведет содержимое таблицы разделяя |
        rows = []
        try:
            connection = psycopg2.connect(user=self.user_for_db_connection, 
                                          password=self.password_for_db_connection, 
                                          host=self.host_name_for_db_connection, 
                                          port=self.port_for_db_connection, 
                                          database=db_name) 

            cursor = connection.cursor()
            if condition == '':
                cursor.execute("SELECT "+select_rows+" from "+str(table_name))
                print("Результат")
                rows = cursor.fetchall()
            else:
                cursor.execute("SELECT " + select_rows + " from " + str(table_name)+' WHERE '+str(condition))
                print("Результат")
                rows = cursor.fetchall()
        except (Exception, Error) as error:
            print("Ошибка при работе с PostgreSQL", error)
        finally:
            if connection:
                cursor.close()
                connection.close()
                print("Соединение с PostgreSQL закрыто")
                return rows

if __name__ == "__main__":
    pass

