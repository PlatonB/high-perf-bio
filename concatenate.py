__version__ = 'v2.1'

import sys, locale, datetime
sys.dont_write_bytecode = True
from cli.concatenate_cli import add_args_ru
from pymongo import MongoClient, IndexModel, ASCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from backend.create_index_models import create_index_models

#CLI.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
        
#Основополагающие для всей программы переменные. Получаются
#они в основном из указанных исследователем аргументов.
#Некоторые неочевидные, но важные детали об этих переменных.
#Квази-расширение конечной коллекции. Оно нужно, чтобы потом
#парсеры знали, как её обрабатывать. Проджекшен (отбор полей).
#Что касается как src-db-VCF, так и src-db-BED, когда мы
#оставляем только часть полей, невозможно гарантировать
#соблюдение спецификаций этих форматов, поэтому вывод будет
#формироваться не более, чем просто табулированным (trg-db-TSV).
client = MongoClient()
src_db_name = args.src_db_name
src_coll_names = client[src_db_name].list_collection_names()
src_coll_ext = src_coll_names[0].rsplit('.', maxsplit=1)[1]
if args.trg_db_name != src_db_name:
        trg_db_name = args.trg_db_name
        resolve_db_existence(trg_db_name)
else:
        raise DbAlreadyExistsError()
if args.proj_fields is None:
        mongo_project = {'_id': 0}
        trg_coll_ext = src_coll_ext
else:
        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
        mongo_project['_id'] = 0
        trg_coll_ext = 'tsv'
mongo_aggr_arg = [{'$project': mongo_project}]
if args.ind_field_names is None:
        ind_field_names = args.ind_field_names
else:
        ind_field_names = args.ind_field_names.split(',')
        
print(f'\nConcatenating {src_db_name} database')

#Конкатенацию можно произвести в двух режимах - с сохранением повторяющихся
#конечных MongoDB-документов и без. Со вторым не так всё очевидно. 1. Сравнение
#идёт по документам как единому целому, т.е. несоответствие хотя бы по одному полю
#позволит обоим документам сосуществовать. 2. Идентификаторы всех документов уникальны
#и при сопоставлении не учитываются. 3. Документы становятся неполными после применения
#проджекшена и сверяются уже в таком виде. 4. Unique-индексы конечной коллекции
#блокируют заливку вложенных структур, а значит, trg-db-VCF уникализировать нельзя.
exec_time_start = datetime.datetime.now()
src_db_obj = client[src_db_name]
trg_db_obj = client[trg_db_name]
trg_coll_name = f'{trg_db_name}.{trg_coll_ext}'
trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                            storageEngine={'wiredTiger':
                                                           {'configString':
                                                            'block_compressor=zstd'}})
if not args.del_copies:
        mongo_on = '_id'
        mongo_when_match = 'keepExisting'
else:
        mongo_on = list(src_db_obj[src_coll_names[0]].find_one(None, mongo_project))
        mongo_when_match = 'replace'
        trg_coll_obj.create_index([(field_name, ASCENDING) for field_name in mongo_on],
                                  unique=True)
mongo_merge = {'into': {'db': trg_db_name,
                        'coll': trg_coll_name},
               'on': mongo_on,
               'whenMatched': mongo_when_match}
mongo_aggr_arg.append({'$merge': mongo_merge})
for src_coll_name in src_coll_names:
        src_db_obj[src_coll_name].aggregate(mongo_aggr_arg)
if trg_coll_obj.count_documents({}) == 0:
        trg_db_obj.drop_collection(trg_coll_name)
else:
        index_models = create_index_models(trg_coll_ext,
                                           ind_field_names)
        if index_models != []:
                trg_coll_obj.create_indexes(index_models)
exec_time = datetime.datetime.now() - exec_time_start

print(f'\tcomputation time: {exec_time}')

client.close()
