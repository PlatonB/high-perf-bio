__version__ = 'v1.2'

from pymongo import MongoClient

class DbAlreadyExistsError(Exception):
        '''
        high-perf-bio не любит путаницу,
        поэтому за крайне редким исключением
        не позволяет редактировать имеющиеся БД.
        На сегодняшний день актуально правило:
        новый результат - в новый файл или базу.
        '''
        def __init__(self):
                err_msg = '''\nUploading data to the source DB is
strictly denied, whereas uploading to
another existing DB is possible only if
explicitly specified corresponding argument'''
                super().__init__(err_msg)
                
def resolve_db_existence(db_name):
        '''
        Функция, дающая возможность удаления базы
        данных в случае конфликта имён. Учтите,
        что защиты от удаления исходной БД тут нет.
        '''
        client = MongoClient()
        if db_name in client.list_database_names():
                db_to_remove = input(f'''\n{db_name} DB already exists.
To confirm DB re-creation,
type or paste its name: ''')
                if db_to_remove == db_name:
                        client.drop_database(db_name)
                else:
                        raise DbAlreadyExistsError()
        client.close()
