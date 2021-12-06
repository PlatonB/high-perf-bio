__version__ = 'v3.0'

import sys, locale, os, datetime, copy, gzip
sys.dont_write_bytecode = True
from cli.split_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from multiprocessing import Pool
from bson.son import SON
from backend.doc_to_line import restore_line
from backend.create_index_models import create_index_models

class NoSuchFieldError(Exception):
        '''
        Если исследователь, допустим, опечатавшись,
        указал поле, которого нет в коллекциях.
        '''
        def __init__(self, spl_field_name):
                err_msg = f'\nThe field {spl_field_name} does not exist'
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
                конечных файлов. Сортировка src-db-VCF и src-db-BED. Она делается по
                координатам для обеспечения поддержки tabix-индексации конечных таблиц.
                Проджекшен (отбор полей). Поля src-db-VCF я, скрепя сердце, позволил
                отбирать, но документы со вложенными объектами, как, например, в INFO,
                не сконвертируются в обычные строки, а сериализуются как есть. Что касается
                и src-db-VCF, и src-db-BED, когда мы оставляем только часть полей, невозможно
                гарантировать соблюдение спецификаций соответствующих форматов, поэтому
                вывод будет формироваться не более, чем просто табулированным (trg-(db-)TSV).
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
                max_proc_quan = args.max_proc_quan
                src_colls_quan = len(self.src_coll_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > src_colls_quan <= cpus_quan:
                        self.proc_quan = src_colls_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                mongo_exclude_meta = {'meta': {'$exists': False}}
                src_field_names = list(client[self.src_db_name][self.src_coll_names[0]].find_one(mongo_exclude_meta))
                if args.spl_field_name is None:
                        if src_coll_ext == 'vcf':
                                self.spl_field_name = '#CHROM'
                        elif src_coll_ext == 'bed':
                                self.spl_field_name = 'chrom'
                        else:
                                self.spl_field_name = src_field_names[1]
                else:
                        self.spl_field_name = args.spl_field_name
                        if self.spl_field_name not in src_field_names:
                                raise NoSuchFieldError(self.spl_field_name)
                self.mongo_aggr_draft = [{'$match': {self.spl_field_name: None}}]
                if src_coll_ext == 'vcf':
                        self.mongo_aggr_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                    ('POS', ASCENDING)])})
                elif src_coll_ext == 'bed':
                        self.mongo_aggr_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                    ('start', ASCENDING),
                                                                    ('end', ASCENDING)])})
                if args.proj_fields is None:
                        self.mongo_findone_args = [mongo_exclude_meta, None]
                        self.trg_file_fmt = src_coll_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_aggr_draft.append({'$project': mongo_project})
                        self.mongo_findone_args = [mongo_exclude_meta, mongo_project]
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
                
        def split(self, src_coll_name):
                '''
                Функция дробления одной коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                src_coll_obj = src_db_obj[src_coll_name]
                
                #Накопления уникализированных (далее - раздельных) значений обозначенного поля
                #коллекции. _id - null, т.к. нам не нужно растаскивание этих значений по каким-либо
                #группам. Операция не является частью пайплайна $match-$sort(-$project)(-$out).
                get_sep_vals = [{'$group': {'_id': 'null',
                                            self.spl_field_name: {'$addToSet': f'${self.spl_field_name}'}}}]
                sep_vals = [doc for doc in src_coll_obj.aggregate(get_sep_vals)][0][self.spl_field_name]
                
                #Запрос, вынесенный в отдельный объект,
                #можно будет спокойно модифицировать
                #внутри распараллеливаемой функции.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                
                #Название исходной коллекции (без квазирасширения) потом
                #пригодится для построения имени конечного файла или коллекции.
                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                
                #Этот большой блок осуществляет
                #запрос с выводом результатов в файлы.
                if hasattr(self, 'trg_dir_path'):
                        
                        #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                        #коллекция, создадим её из имён полей. Projection при этом учтём.
                        #Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
                        #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                        header_row = list(src_coll_obj.find_one(*self.mongo_findone_args))[1:]
                        if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Один конечный файл будет
                        #содержать данные, соответствующие
                        #одному раздельному значению.
                        for sep_val in sep_vals:
                                
                                #Внедряем в фильтрационную стадию
                                #пайплайна текущее раздельное значение.
                                mongo_aggr_arg[0]['$match'][self.spl_field_name] = sep_val
                                
                                #Создаём объект курсора.
                                curs_obj = src_coll_obj.aggregate(mongo_aggr_arg)
                                
                                #Конструируем имя конечного архива и абсолютный путь.
                                trg_file_name = f'coll_{src_coll_base}__{self.spl_field_name}_{sep_val}.{self.trg_file_fmt}.gz'
                                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                                
                                #Открытие конечного файла на запись.
                                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                        
                                        #Формируем и прописываем метастроки,
                                        #повествующие о происхождении конечного
                                        #файла. Прописываем также табличную шапку.
                                        if self.trg_file_fmt == 'vcf':
                                                trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                        trg_file_opened.write(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                        trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                                        trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                                        trg_file_opened.write(f'##mongo_aggr={mongo_aggr_arg}\n')
                                        trg_file_opened.write(header_line + '\n')
                                        
                                        #Извлечение из объекта курсора отвечающих запросу
                                        #документов, преобразование их значений в обычные
                                        #строки и прописывание последних в конечный файл.
                                        for doc in curs_obj:
                                                trg_file_opened.write(restore_line(doc,
                                                                                   self.trg_file_fmt,
                                                                                   self.sec_delimiter))
                                                
                #Та же работа, но с выводом в БД. Опишу некоторые особенности.
                #При работе с каждым раздельным значением Aggregation-инструкция
                #обогащается этапом вывода в конечную коллекцию. Метастроки
                #складываются в список, а он, в свою очередь, встраивается
                #в первый документ коллекции. Для конечных коллекций
                #создаются обязательные и пользовательские индексы.
                elif hasattr(self, 'trg_db_name'):
                        trg_db_obj = client[self.trg_db_name]
                        mongo_aggr_arg.append({'$merge': {'into': {'db': self.trg_db_name,
                                                                   'coll': None}}})
                        for sep_val in sep_vals:
                                mongo_aggr_arg[0]['$match'][self.spl_field_name] = sep_val
                                trg_coll_name = f'coll_{src_coll_base}__{self.spl_field_name}_{sep_val}.{self.trg_file_fmt}'
                                mongo_aggr_arg[-1]['$merge']['into']['coll'] = trg_coll_name
                                trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                                                            storageEngine={'wiredTiger':
                                                                                           {'configString':
                                                                                            'block_compressor=zstd'}})
                                meta_lines = {'meta': []}
                                if self.trg_file_fmt == 'vcf':
                                        meta_lines['meta'].append(f'##fileformat={self.trg_file_fmt.upper()}')
                                meta_lines['meta'].append(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>')
                                meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                                meta_lines['meta'].append(f'##src_coll_name={src_coll_name}')
                                meta_lines['meta'].append(f'##mongo_aggr={mongo_aggr_arg}')
                                trg_coll_obj.insert_one(meta_lines)
                                src_coll_obj.aggregate(mongo_aggr_arg)
                                index_models = create_index_models(self.trg_file_fmt,
                                                                   self.ind_field_names)
                                if index_models != []:
                                        trg_coll_obj.create_indexes(index_models)
                                        
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#измельчения. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nSplitting collections of {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.split, main.src_coll_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')