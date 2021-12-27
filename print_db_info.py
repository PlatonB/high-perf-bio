__version__ = 'v5.3'

import sys, locale
sys.dont_write_bytecode = True
from cli.print_db_info_cli import add_args_ru, add_args_en
from pymongo import MongoClient
from backend.get_field_paths import parse_nested_objs

def conv_data_measure(size):
        '''
        Функция конвертирует байты в такую единицу
        измерения, чтобы значение получалось
        наиболее компактным/читабельным.
        '''
        if size < 1000:
                return f'{size} B'
        elif size < 1000000:
                return f'{round(size/1000, 1)} KB'
        elif size < 1000000000:
                return f'{round(size/1000000, 1)} MB'
        elif size < 1000000000000:
                return f'{round(size/1000000000, 1)} GB'
        else:
                return f'{round(size/1000000000000, 1)} TB'
        
#CLI.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
        
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
        print('\nNames of all MongoDB databases:',
              f'\n{", ".join(all_db_names)}')
else:
        src_db_name = args.src_db_name
        src_db_obj = client[src_db_name]
        src_coll_names = sorted(src_db_obj.list_collection_names())
        src_colls_quan = len(src_coll_names)
        src_db_stats = src_db_obj.command('dbstats')
        src_db_storagesize = conv_data_measure(src_db_stats['storageSize'])
        src_db_indexsize = conv_data_measure(src_db_stats['indexSize'])
        src_db_totalsize = conv_data_measure(src_db_stats['totalSize'])
        src_coll_ext = src_coll_names[0].rsplit('.', maxsplit=1)[1]
        if args.colls_quan_limit == 0 or args.colls_quan_limit > src_colls_quan:
                colls_quan_limit = src_colls_quan
        else:
                colls_quan_limit = args.colls_quan_limit
        print(f'\nCharacteristics of {src_db_name} DB:')
        print('\n\tQuantity of collections:',
              f'\n\t{src_colls_quan}')
        print('\n\tstorageSize:',
              f'\n\t{src_db_storagesize}')
        print('\n\tindexSize:',
              f'\n\t{src_db_indexsize}')
        print('\n\ttotalSize:',
              f'\n\t{src_db_totalsize}')
        print('\n\tQuasi-extension of collections:',
              f'\n\t{src_coll_ext}')
        if colls_quan_limit == src_colls_quan:
                print('\n\tCharacteristics of all collections:')
        elif colls_quan_limit == 1:
                print('\n\tCharacteristics of the first collection:')
        else:
                print(f'\n\tCharacteristics of the first {colls_quan_limit} collections:')
        docs_quan_limit = args.docs_quan_limit
        for src_coll_index in range(colls_quan_limit):
                src_coll_name = src_coll_names[src_coll_index]
                src_coll_obj = src_db_obj[src_coll_name]
                docs_quan = src_coll_obj.count_documents({})
                src_coll_stats = src_db_obj.command('collstats', src_coll_name)
                src_coll_storagesize = conv_data_measure(src_coll_stats['storageSize'])
                src_coll_totalindexsize = conv_data_measure(src_coll_stats['totalIndexSize'])
                src_coll_totalsize = conv_data_measure(src_coll_stats['totalSize'])
                field_paths = parse_nested_objs(src_coll_obj.find_one({'meta': {'$exists': False}}))
                ind_info = src_coll_obj.index_information()
                ind_field_paths, ind_names = [], []
                for ind_name, ind_details in ind_info.items():
                        for tup in ind_details['key']:
                                if tup[0] not in ind_field_paths:
                                        ind_field_paths.append(tup[0])
                        ind_names.append(ind_name)
                curs_obj = src_coll_obj.find(limit=docs_quan_limit)
                print(f'\n\t\t{src_coll_name} collection:')
                print('\n\t\t\tQuantity of documents:',
                      f'\n\t\t\t{docs_quan}')
                print('\n\t\t\tstorageSize:',
                      f'\n\t\t\t{src_coll_storagesize}')
                print('\n\t\t\ttotalIndexSize:',
                      f'\n\t\t\t{src_coll_totalindexsize}')
                print('\n\t\t\ttotalSize:',
                      f'\n\t\t\t{src_coll_totalsize}')
                print('\n\t\t\tPaths to fields of the second document:',
                      f'\n\t\t\t{", ".join(field_paths)}')
                print('\n\t\t\tPaths to indexed fields:',
                      f'\n\t\t\t{", ".join(ind_field_paths)}')
                print('\n\t\t\tNames of indexes:',
                      f'\n\t\t\t{", ".join(ind_names)}')
                if docs_quan_limit == 0:
                        print('\n\t\t\tAll documents:')
                elif docs_quan_limit == 1:
                        print('\n\t\t\tThe first document:')
                else:
                        print(f'\n\t\t\tThe first {docs_quan_limit} documents:')
                for doc in curs_obj:
                        print(f'\n\t\t\t\t{doc}')
client.close()
