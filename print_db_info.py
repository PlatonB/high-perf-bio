__version__ = 'v4.0'

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient

def conv_data_measure(size):
        '''
        Функция конвертирует байты в такую единицу
        измерения, чтобы значение получалось
        наиболее компактным/читабельным.
        '''
        if size < 1000:
                return f'{size} Б'
        elif size < 1000000:
                return f'{round(size/1000, 1)} КБ'
        elif size < 1000000000:
                return f'{round(size/1000000, 1)} МБ'
        elif size < 1000000000000:
                return f'{round(size/1000000000, 1)} ГБ'
        else:
                return f'{round(size/1000000000000, 1)} ТБ'
        
#Работа с аргументами командной строки.
argparser = ArgumentParser(description=f'''
Программа, позволяющая вывести
имена всех баз данных или ключевую
информацию об определённой БД.

Версия: {__version__}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Этот инструмент создан на случай, если вы используете
Linux без графической оболочки. Если же у вас дистрибутив
с DE, предпросматривайте базы в MongoDB Compass.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]]
''',
                           formatter_class=RawTextHelpFormatter)
argparser.add_argument('-d', '--src-db-name', metavar='[None]', dest='src_db_name', type=str,
                       help='Имя БД, для которой надо вывести информацию ([[имена всех БД]])')
argparser.add_argument('-c', '--colls-quan-limit', metavar='[5]', default=5, dest='colls_quan_limit', type=int,
                       help='Ограничение на количество характеризуемых коллекций (0 - вывести характеристики всех коллекций)')
argparser.add_argument('-s', '--docs-quan-limit', metavar='[5]', default=5, dest='docs_quan_limit', type=int,
                       help='Ограничение на количество выводимых документов каждой коллекции (0 - вывести все документы)')
args = argparser.parse_args()

#Если исследователь не задал ни одного аргумента,
#выведутся имена всех имеющихся на компе MongoDB-баз.
#Если же он указал имя конкретной базы, появятся
#её характеристики. Выведутся как общие сведения
#о БД, так и описания некоторых составляющих её
#коллекций с примерами документов. Некоторых -
#потому что, как правило, для понимания устройства
#базы, достаточно заглянуть в пару-тройку коллекций.
client = MongoClient()
if args.src_db_name is None:
        all_db_names = sorted(client.list_database_names())
        print('\nИмена всех MongoDB-баз:',
              f'\n{", ".join(all_db_names)}')
else:
        src_db_name = args.src_db_name
        src_db_obj = client[src_db_name]
        src_coll_names = sorted(src_db_obj.list_collection_names())
        src_colls_quan = len(src_coll_names)
        src_db_storagesize = conv_data_measure(src_db_obj.command('dbstats')['storageSize'])
        src_db_indexsize = conv_data_measure(src_db_obj.command('dbstats')['indexSize'])
        src_db_totalsize = conv_data_measure(src_db_obj.command('dbstats')['totalSize'])
        src_coll_ext = src_coll_names[0].rsplit('.', maxsplit=1)[1]
        print(f'\nХарактеристики БД {src_db_name}:')
        print('\n\tКоличество коллекций:',
              f'\n\t{src_colls_quan}')
        print('\n\tstorageSize:',
              f'\n\t{src_db_storagesize}')
        print('\n\tindexSize:',
              f'\n\t{src_db_indexsize}')
        print('\n\ttotalSize:',
              f'\n\t{src_db_totalsize}')
        print('\n\tКвазирасширение коллекций:',
              f'\n\t{src_coll_ext}')
        print('\n\tХарактеристики первых коллекций:')
        if args.colls_quan_limit == 0 or args.colls_quan_limit > src_colls_quan:
                colls_quan_limit = src_colls_quan
        else:
                colls_quan_limit = args.colls_quan_limit
        docs_quan_limit = args.docs_quan_limit
        for src_coll_index in range(colls_quan_limit):
                src_coll_name = src_coll_names[src_coll_index]
                src_coll_obj = src_db_obj[src_coll_name]
                docs_quan = src_coll_obj.count_documents({})
                src_coll_storagesize = conv_data_measure(src_db_obj.command('collstats',
                                                                            src_coll_name)['storageSize'])
                src_coll_totalindexsize = conv_data_measure(src_db_obj.command('collstats',
                                                                               src_coll_name)['totalIndexSize'])
                src_coll_totalsize = conv_data_measure(src_db_obj.command('collstats',
                                                                          src_coll_name)['totalSize'])
                field_names = src_coll_obj.find_one().keys()
                ind_info = src_coll_obj.index_information()
                ind_field_names, ind_names = [], []
                for ind_name, ind_details in ind_info.items():
                        for tup in ind_details['key']:
                                if tup[0] not in ind_field_names:
                                        ind_field_names.append(tup[0])
                        ind_names.append(ind_name)
                curs_obj = src_coll_obj.find(limit=docs_quan_limit)
                print(f'\n\t\tКоллекция {src_coll_name}:')
                print('\n\t\t\tКоличество документов:',
                      f'\n\t\t\t{docs_quan}')
                print('\n\t\t\tstorageSize:',
                      f'\n\t\t\t{src_coll_storagesize}')
                print('\n\t\t\ttotalIndexSize:',
                      f'\n\t\t\t{src_coll_totalindexsize}')
                print('\n\t\t\ttotalSize:',
                      f'\n\t\t\t{src_coll_totalsize}')
                print('\n\t\t\tИмена полей первого документа:',
                      f'\n\t\t\t{", ".join(field_names)}')
                print('\n\t\t\tИмена проиндексированных полей:',
                      f'\n\t\t\t{", ".join(ind_field_names)}')
                print('\n\t\t\tИмена индексов:',
                      f'\n\t\t\t{", ".join(ind_names)}')
                print('\n\t\t\tПервые документы:')
                for doc in curs_obj:
                        print(f'\n\t\t\t\t{doc}')
client.close()
