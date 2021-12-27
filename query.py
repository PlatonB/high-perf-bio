__version__ = 'v6.2'

import sys, locale, os, datetime, copy, gzip
sys.dont_write_bytecode = True
from cli.query_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING
from multiprocessing import Pool
from bson.son import SON
from bson.decimal128 import Decimal128
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from backend.get_field_paths import parse_nested_objs
from backend.doc_to_line import restore_line
from backend.create_index_models import create_index_models

class NoSuchFieldError(Exception):
        '''
        Если исследователь, допустим, опечатавшись,
        указал поле, которого нет в коллекциях.
        '''
        def __init__(self, field_path):
                err_msg = f'\nThe field {field_path} does not exist'
                super().__init__(err_msg)
                
class Main():
        '''
        Основной класс. args, подаваемый иниту на вход, не обязательно
        должен формироваться argparse. Этим объектом может быть экземпляр
        класса из стороннего Python-модуля, в т.ч. имеющего отношение к GUI.
        Кстати, написание сообществом всевозможных графических интерфейсов
        к high-perf-bio люто, бешено приветствуется! В ините на основе args
        создаются как атрибуты, используемые распараллеливаемой функцией,
        так и атрибуты, нужные для кода, её запускающего. Что касается этой
        функции, её можно запросто пристроить в качестве коллбэка кнопки в GUI.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов как для основной функции программы, так и для блока
                многопроцессового запуска таковой. Первые из перечисленных ни в коем
                случае не должны будут потом в параллельных процессах изменяться. Некоторые
                неочевидные, но важные детали об атрибутах. Квази-расширение коллекций.
                Оно нужно, как минимум, для определения правил сортировки и форматирования
                конечных файлов. Сортировка db-VCF и db-BED. Она делается по координатам
                для обеспечения поддержки tabix-индексации конечных таблиц. Проджекшен
                (отбор полей). Поля src-db-VCF я, скрепя сердце, позволил отбирать, но
                документы со вложенными объектами, как, например, в INFO, не сконвертируются
                в обычные строки, а сериализуются как есть. Что касается и src-db-VCF, и
                src-db-BED, когда мы оставляем только часть полей, невозможно гарантировать
                соблюдение спецификаций соответствующих форматов, поэтому вывод будет
                формироваться не более, чем просто табулированным (trg-(db-)TSV).
                '''
                client = MongoClient()
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
                self.src_db_name = args.src_db_name
                src_db_obj = client[self.src_db_name]
                self.src_coll_names = src_db_obj.list_collection_names()
                src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if '/' in args.trg_place:
                        self.trg_dir_path = os.path.normpath(args.trg_place)
                elif args.trg_place != self.src_db_name:
                        self.trg_db_name = args.trg_place
                        resolve_db_existence(self.trg_db_name)
                else:
                        raise DbAlreadyExistsError()
                max_proc_quan = args.max_proc_quan
                src_files_quan = len(self.src_file_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > src_files_quan <= cpus_quan:
                        self.proc_quan = src_files_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                self.meta_lines_quan = args.meta_lines_quan
                self.mongo_aggr_draft = [{'$match': None}]
                if src_coll_ext == 'vcf':
                        self.mongo_aggr_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                    ('POS', ASCENDING)])})
                elif src_coll_ext == 'bed':
                        self.mongo_aggr_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                    ('start', ASCENDING),
                                                                    ('end', ASCENDING)])})
                self.mongo_exclude_meta = {'meta': {'$exists': False}}
                src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(self.mongo_exclude_meta))
                if args.proj_field_names is None:
                        self.mongo_findone_args = [self.mongo_exclude_meta, None]
                        self.trg_file_fmt = src_coll_ext
                else:
                        proj_field_names = args.proj_field_names.split(',')
                        for proj_field_name in proj_field_names:
                                if proj_field_name not in src_field_paths:
                                        raise NoSuchFieldError(proj_field_name)
                        mongo_project = {proj_field_name: 1 for proj_field_name in proj_field_names}
                        self.mongo_aggr_draft.append({'$project': mongo_project})
                        self.mongo_findone_args = [self.mongo_exclude_meta, mongo_project]
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
                if args.ind_field_paths is None:
                        self.ind_field_paths = args.ind_field_paths
                else:
                        self.ind_field_paths = args.ind_field_paths.split(',')
                self.ver = ver
                client.close()
                
        def search(self, src_file_name):
                '''
                Функция выполнения запросов из
                одного JSONl-подобного файла.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                
                #Открытие исходного файла на чтение, смещение курсора к его основной части.
                with open(os.path.join(self.src_dir_path, src_file_name)) as src_file_opened:
                        for meta_line_index in range(self.meta_lines_quan):
                                src_file_opened.readline()
                                
                        #Запрос, вынесенный в отдельный объект,
                        #можно будет спокойно модифицировать
                        #внутри распараллеливаемой функции.
                        mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                        
                        #Счётчик запросов, а также название исходного
                        #файла (без расширения), потом пригодятся для
                        #построения имён конечных файлов или коллекций.
                        src_line_num = 0
                        src_file_base = src_file_name.rsplit('.', maxsplit=1)[0]
                        
                        #Этот большой блок вытаскивает запросы из
                        #исходного файла и запускает их выполнение
                        #с выводом результатов в соответствующие
                        #парам запрос-коллекция конечные файлы.
                        if hasattr(self, 'trg_dir_path'):
                                
                                #Одна строка - один запрос. Строки без обязательных
                                #для любого MongoDB-запроса фигурных скобок игнорируются.
                                #Это позволяет программе, в частности, не спотыкаться
                                #на умышленно или случайно оставленных пустых строках.
                                #eval используется вместо json.loads, чтобы не придираться
                                #к виду кавычек и для поддержки чуждого для классического
                                #JSON типа данных Decimal128. Номер запроса нужен будет
                                #исключительно ради уникальности имени конечного файла.
                                for src_line in src_file_opened:
                                        if '{' not in src_line:
                                                continue
                                        mongo_aggr_arg[0]['$match'] = self.mongo_exclude_meta | eval(src_line)
                                        src_line_num += 1
                                        
                                        #Каждый запрос делается по всем
                                        #коллекциям MongoDB-базы. Т.е.
                                        #даже, если по одной из коллекций
                                        #уже получились результаты, обход
                                        #будет продолжаться и завершится лишь
                                        #после обращения к последней коллекции.
                                        for src_coll_name in self.src_coll_names:
                                                
                                                #Создание двух объектов: текущей коллекции и курсора.
                                                src_coll_obj = src_db_obj[src_coll_name]
                                                curs_obj = src_coll_obj.aggregate(mongo_aggr_arg)
                                                
                                                #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                                                #коллекция, создадим её из имён полей. Projection при этом учтём.
                                                #Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
                                                #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                                                trg_header_row = list(src_coll_obj.find_one(*self.mongo_findone_args))[1:]
                                                if self.trg_file_fmt == 'vcf' and len(trg_header_row) > 8:
                                                        trg_header_row.insert(8, 'FORMAT')
                                                trg_header_line = '\t'.join(trg_header_row)
                                                
                                                #Конструируем имя конечного архива и абсолютный путь.
                                                #Пытаться поместить в имя запрос абсурдно, поэтому
                                                #ограничимся номером запроса. Пайплайн, содержащий запрос,
                                                #пропишется потом вовнутрь файла как строка метаинформации.
                                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                                trg_file_name = f'file-{src_file_base}__query-{src_line_num}__coll-{src_coll_base}.{self.trg_file_fmt}.gz'
                                                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                                                
                                                #Открытие конечного файла на запись.
                                                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                                        
                                                        #Формируем и прописываем метастроки,
                                                        #повествующие о происхождении конечного
                                                        #файла. Прописываем также табличную шапку.
                                                        if self.trg_file_fmt == 'vcf':
                                                                trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                                        trg_file_opened.write(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                                        trg_file_opened.write(f'##src_file_name={src_file_name}\n')
                                                        trg_file_opened.write(f'##mongo_aggr={mongo_aggr_arg}\n')
                                                        trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                                                        trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                                                        trg_file_opened.write(trg_header_line + '\n')
                                                        
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
                                                        
                        #Та же работа, но с выводом в БД. Опишу некоторые
                        #особенности. Aggregation-инструкция обогащается
                        #этапом вывода в конечную коллекцию. Метастроки
                        #складываются в список, а он, в свою очередь,
                        #встраивается в первый документ коллекции. Если
                        #этот документ так и остаётся в гордом одиночестве,
                        #коллекция удаляется. Для непустых конечных коллекций
                        #создаются обязательные и пользовательские индексы.
                        elif hasattr(self, 'trg_db_name'):
                                trg_db_obj = client[self.trg_db_name]
                                mongo_aggr_arg.append({'$merge': {'into': {'db': self.trg_db_name,
                                                                           'coll': None}}})
                                for src_line in src_file_opened:
                                        if '{' not in src_line:
                                                continue
                                        mongo_aggr_arg[0]['$match'] = self.mongo_exclude_meta | eval(src_line)
                                        src_line_num += 1
                                        for src_coll_name in self.src_coll_names:
                                                src_coll_obj = src_db_obj[src_coll_name]
                                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                                trg_coll_name = f'file-{src_file_base}__query-{src_line_num}__coll-{src_coll_base}.{self.trg_file_fmt}'
                                                mongo_aggr_arg[-1]['$merge']['into']['coll'] = trg_coll_name
                                                trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                                                                            storageEngine={'wiredTiger':
                                                                                                           {'configString':
                                                                                                            'block_compressor=zstd'}})
                                                meta_lines = {'meta': []}
                                                if self.trg_file_fmt == 'vcf':
                                                        meta_lines['meta'].append(f'##fileformat={self.trg_file_fmt.upper()}')
                                                meta_lines['meta'].append(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.ver}>')
                                                meta_lines['meta'].append(f'##src_file_name={src_file_name}')
                                                meta_lines['meta'].append(f'##mongo_aggr={mongo_aggr_arg}')
                                                meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                                                meta_lines['meta'].append(f'##src_coll_name={src_coll_name}')
                                                trg_coll_obj.insert_one(meta_lines)
                                                src_coll_obj.aggregate(mongo_aggr_arg)
                                                if trg_coll_obj.count_documents({}) == 1:
                                                        trg_db_obj.drop_collection(trg_coll_name)
                                                else:
                                                        index_models = create_index_models(self.trg_file_fmt,
                                                                                           self.ind_field_paths)
                                                        if index_models != []:
                                                                trg_coll_obj.create_indexes(index_models)
                                                                
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#поиска. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nQueriing by {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.search, main.src_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
