__version__ = 'v5.1'

class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасный параллельный поиск
        по набору коллекций MongoDB.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной под многопроцессовое
                выполнение функции отбора документов. Атрибуты ни в коем случае не
                должны будут потом в параллельных процессах изменяться. Получаются они
                в основном из указанных исследователем аргументов. Некоторые неочевидные,
                но важные детали об атрибутах. Квази-расширение коллекций. Оно нужно,
                как минимум, для определения правил сортировки и форматирования конечных
                файлов. Сортировка db-VCF и db-BED. Она делается по координатам для
                обеспечения поддержки tabix-индексации конечных таблиц. Проджекшен
                (отбор полей). Для db-VCF его крайне трудно реализовать из-за наличия
                в соответствующих коллекциях разнообразных вложенных структур и запрета
                со стороны MongoDB на применение точечной формы обращения к отбираемым
                элементам массивов. Что касается db-BED, когда мы оставляем только часть
                полей, невозможно гарантировать соблюдение спецификаций BED-формата, поэтому
                вывод будет формироваться не более, чем просто табулированным (trg-TSV).
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if '/' in args.trg_place:
                        self.trg_dir_path = os.path.normpath(args.trg_place)
                elif args.trg_place != self.src_db_name:
                        self.trg_db_name = args.trg_place
                        resolve_db_existence(self.trg_db_name)
                else:
                        raise DbAlreadyExistsError()
                self.mongo_aggr_draft = [{'$match': eval(args.mongo_query)}]
                if src_coll_ext == 'vcf':
                        self.mongo_aggr_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                    ('POS', ASCENDING)])})
                elif src_coll_ext == 'bed':
                        self.mongo_aggr_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                    ('start', ASCENDING),
                                                                    ('end', ASCENDING)])})
                if args.proj_fields is None or src_coll_ext == 'vcf':
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = src_coll_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_aggr_draft.append({'$project': mongo_project})
                        self.mongo_findone_args = [None, mongo_project]
                        self.trg_file_fmt = 'tsv'
                if args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'low_line':
                        self.sec_delimiter = '_'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                if args.ind_field_names is None:
                        self.ind_field_names = args.ind_field_names
                else:
                        self.ind_field_names = args.ind_field_names.split(',')
                self.ver = ver
                client.close()
                
        def search(self, src_coll_name):
                '''
                Функция поиска по одной коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                src_coll_obj = src_db_obj[src_coll_name]
                
                #Aggregation-инструкция может быть потом дополнена
                #индивидуальным для текущей коллекции $out-этапом.
                #В связи с перспективой внутрипроцессовой модификации
                #общего выражения, создаём отдельный объект с этим
                #выражением, который точно не страшно ковырять.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                
                #Получаем имя конечного файла. Оно же при
                #необходимости - имя конечной коллекции.
                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'{src_coll_base}_query_res.{self.trg_file_fmt}'
                
                #Этот большой блок осуществляет
                #запрос с выводом результатов в файл.
                if hasattr(self, 'trg_dir_path'):
                        curs_obj = src_coll_obj.aggregate(mongo_aggr_arg)
                        
                        #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                        #коллекция, создадим её из имён полей. Projection при этом учтём.
                        #Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
                        #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                        header_row = list(src_coll_obj.find_one(*self.mongo_findone_args))[1:]
                        if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Конструируем абсолютный путь к конечному файлу.
                        trg_file_path = os.path.join(self.trg_dir_path,
                                                     trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                if self.trg_file_fmt == 'vcf':
                                        trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##database={self.src_db_name}\n')
                                trg_file_opened.write(f'##collection={src_coll_name}\n')
                                trg_file_opened.write(f'##query={mongo_aggr_arg[0]["$match"]}\n')
                                if self.mongo_findone_args[1] is not None:
                                        trg_file_opened.write(f'##project={self.mongo_findone_args[1]}\n')
                                trg_file_opened.write(header_line + '\n')
                                
                                #Извлечение из объекта курсора отвечающих запросу
                                #документов, преобразование их значений в обычные
                                #строки и прописывание последних в конечный файл.
                                #Проверка, вылез ли по запросу хоть один документ.
                                empty_res = True
                                for doc in curs_obj:
                                        trg_file_opened.write(restore_line(doc,
                                                                           self.trg_file_fmt,
                                                                           self.sec_delimiter))
                                        empty_res = False
                                        
                        #Удаление конечного файла, если в
                        #нём очутились только метастроки.
                        if empty_res:
                                os.remove(trg_file_path)
                                
                #Создание конечной базы и коллекции.
                #Обогащение aggregation-инструкции этапом
                #вывода в конечную коллекцию. Последняя,
                #если не пополнилась результатами, удаляется.
                #Для непустых конечных коллекций создаются
                #обязательные и пользовательские индексы.
                elif hasattr(self, 'trg_db_name'):
                        trg_db_obj = client[self.trg_db_name]
                        trg_coll_obj = trg_db_obj.create_collection(trg_file_name,
                                                                    storageEngine={'wiredTiger':
                                                                                   {'configString':
                                                                                    'block_compressor=zstd'}})
                        mongo_aggr_arg.append({'$out': {'db': self.trg_db_name,
                                                        'coll': trg_file_name}})
                        src_coll_obj.aggregate(mongo_aggr_arg)
                        if trg_coll_obj.count_documents({}) == 0:
                                trg_db_obj.drop_collection(trg_file_name)
                        else:
                                index_models = create_index_models(self.trg_file_fmt,
                                                                   self.ind_field_names)
                                if index_models != []:
                                        trg_coll_obj.create_indexes(index_models)
                                        
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, locale, os, datetime, copy
sys.dont_write_bytecode = True
from cli.query_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from bson.decimal128 import Decimal128
from multiprocessing import Pool
from bson.son import SON
from backend.doc_to_line import restore_line
from backend.create_index_models import create_index_models

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#парсимых коллекций, определение
#оптимального числа процессов.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args,
                                  __version__)
src_coll_names = prep_single_proc.src_coll_names
colls_quan = len(src_coll_names)
if max_proc_quan > colls_quan <= 8:
        proc_quan = colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nQueriing by {prep_single_proc.src_db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск поиска. Замер времени
#выполнения вычислений с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.search, src_coll_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
