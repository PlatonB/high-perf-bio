__version__ = 'v6.3'

import sys, locale, os, datetime, gzip, copy
sys.dont_write_bytecode = True
from cli.annotate_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from multiprocessing import Pool
from bson.son import SON
from backend.def_data_type import def_data_type
from backend.doc_to_line import restore_line
from backend.create_index_models import create_index_models

class DifFmtsError(Exception):
        '''
        Аннотировать по базе можно только одноформатные таблицы.
        '''
        def __init__(self, src_file_fmts):
                err_msg = f'\nSource files are in different formats: {src_file_fmts}'
                super().__init__(err_msg)
                
class ByLocTsvError(Exception):
        '''
        В исследуемом TSV или основанной на TSV
        коллекции может не быть геномных координат.
        Ну или бывает, когда координатные столбцы
        располагаются, где попало. Поэтому нельзя,
        чтобы при пересечении по локации хоть в одном
        из этих двух мест витал вольноформатный дух.
        '''
        def __init__(self):
                err_msg = '\nIntersection by location is not possible for src-TSV or src-db-TSV'
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
                многопроцессового запуска таковой. Первые из перечисленных ни в коем случае
                не должны будут потом в параллельных процессах изменяться. Немного о наиболее
                значимых атрибутах. Расширение исходных таблиц и квази-расширение коллекций
                нужны, как минимум, для выбора формат-ориентированного пересекательного запроса,
                определения правил сортировки и форматирования конечных файлов. Умолчания по
                столбцам и полям выбраны на основе здравого смысла: к примеру, аннотировать
                src-VCF по src-db-VCF или src-db-BED логично, пересекая столбец и поле, оба
                из которых с идентификаторами вариантов. Сортировка src-db-VCF и src-db-BED
                делается по координатам для обеспечения поддержки tabix-индексации конечных
                таблиц. Важные замечания по проджекшену. Поля src-db-VCF я, скрепя сердце,
                позволил отбирать, но документы со вложенными объектами, как, например, в INFO,
                не сконвертируются в обычные строки, а сериализуются как есть. Что касается
                и src-db-VCF, и src-db-BED, когда мы оставляем только часть полей, невозможно
                гарантировать соблюдение спецификаций соответствующих форматов, поэтому
                вывод будет формироваться не более, чем просто табулированным (trg-(db-)TSV).
                '''
                client = MongoClient()
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
                src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                        self.src_file_names))
                if len(src_file_fmts) > 1:
                        raise DifFmtsError(src_file_fmts)
                self.src_file_fmt = list(src_file_fmts)[0]
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                self.src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
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
                self.by_loc = args.by_loc
                if self.by_loc:
                        if self.src_file_fmt not in ['vcf', 'bed'] or self.src_coll_ext not in ['vcf', 'bed']:
                                raise ByLocTsvError()
                        self.mongo_aggr_draft = [{'$match': {'$or': []}}]
                else:
                        if args.ann_col_num is None:
                                if self.src_file_fmt == 'vcf':
                                        self.ann_col_index = 2
                                elif self.src_file_fmt == 'bed':
                                        self.ann_col_index = 3
                                else:
                                        self.ann_col_index = 0
                        else:
                                self.ann_col_index = args.ann_col_num - 1
                        if args.ann_field_name is None:
                                if self.src_coll_ext == 'vcf':
                                        self.ann_field_name = 'ID'
                                elif self.src_coll_ext == 'bed':
                                        self.ann_field_name = 'name'
                                else:
                                        self.ann_field_name = 'rsID'
                        else:
                                self.ann_field_name = args.ann_field_name
                        self.mongo_aggr_draft = [{'$match': {self.ann_field_name: {'$in': []}}}]
                if self.src_coll_ext == 'vcf':
                        self.mongo_aggr_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                    ('POS', ASCENDING)])})
                elif self.src_coll_ext == 'bed':
                        self.mongo_aggr_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                    ('start', ASCENDING),
                                                                    ('end', ASCENDING)])})
                if args.proj_fields is None:
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = self.src_coll_ext
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
                
        def annotate(self, src_file_name):
                '''
                Функция пересечения одной таблицы со
                всеми коллекциями БД по заданной паре
                столбец-поле или по позиции в геноме.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                
                #Открытие исходной архивированной таблицы на чтение, смещение курсора к её основной части.
                with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
                        if self.src_file_fmt == 'vcf':
                                for line in src_file_opened:
                                        if not line.startswith('##'):
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        src_file_opened.readline()
                                        
                        #Пополняем список, служащий основой будущего запроса. Для координатных вычислений
                        #предусматриваем структуры запроса под все 4 возможных сочетания форматов VCF и BED.
                        #Несмотря на то, что запрос может стать абсурдно длинным, программа кладёт в него
                        #сразу всё, что отобрано из исходного файла. Если запрашивать по одному элементу, то
                        #непонятно, как сортировать конечные данные. Неспособным на внешнюю сортировку питоновским
                        #sorted? А вдруг на запрос откликнется неприлично много документов?.. При создании
                        #БД для каждого значения устанавливался оптимальный тип данных. При работе с MongoDB
                        #важно соблюдать соответствие типа данных запрашиваемого слова и размещённых в базе
                        #значений. Для этого присвоим подходящий тип данных каждому аннотируемому элементу.
                        mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                        for src_line in src_file_opened:
                                src_row = src_line.rstrip().split('\t')
                                if self.by_loc:
                                        if self.src_file_fmt == 'vcf':
                                                src_chrom, src_pos = def_data_type(src_row[0].replace('chr', '')), int(src_row[1])
                                                if self.src_coll_ext == 'vcf':
                                                        mongo_aggr_arg[0]['$match']['$or'].append({'#CHROM': src_chrom,
                                                                                                   'POS': src_pos})
                                                elif self.src_coll_ext == 'bed':
                                                        mongo_aggr_arg[0]['$match']['$or'].append({'chrom': src_chrom,
                                                                                                   'start': {'$lt': src_pos},
                                                                                                   'end': {'$gte': src_pos}})
                                        elif self.src_file_fmt == 'bed':
                                                src_chrom, src_start, src_end = def_data_type(src_row[0].replace('chr', '')), int(src_row[1]), int(src_row[2])
                                                if self.src_coll_ext == 'vcf':
                                                        mongo_aggr_arg[0]['$match']['$or'].append({'#CHROM': src_chrom,
                                                                                                   'POS': {'$gt': src_start,
                                                                                                           '$lte': src_end}})
                                                elif self.src_coll_ext == 'bed':
                                                        mongo_aggr_arg[0]['$match']['$or'].append({'chrom': src_chrom,
                                                                                                   'start': {'$lt': src_end},
                                                                                                   'end': {'$gt': src_start}})
                                else:
                                        mongo_aggr_arg[0]['$match'][self.ann_field_name]['$in'].append(def_data_type(src_row[self.ann_col_index]))
                                        
                #Название исходной коллекции (без квазирасширения) потом
                #пригодится для построения имени конечного файла или коллекции.
                src_file_base = src_file_name.rsplit('.', maxsplit=2)[0]
                
                #Этот большой блок осуществляет
                #запрос с выводом результатов в файл.
                if hasattr(self, 'trg_dir_path'):
                        
                        #Обработка каждой исходной
                        #таблицы производится по всем
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
                                #Имя сугубо технического поля _id проигнорируется. Если в db-VCF
                                #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                                header_row = list(src_coll_obj.find_one(*self.mongo_findone_args))[1:]
                                if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                                        header_row.insert(8, 'FORMAT')
                                header_line = '\t'.join(header_row)
                                
                                #Конструируем имя конечного архива и абсолютный путь к этому файлу.
                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                trg_file_name = f'file_{src_file_base}__coll_{src_coll_base}.{self.trg_file_fmt}.gz'
                                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                                
                                #Открытие конечного файла на запись.
                                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                        
                                        #Формируем и прописываем метастроки,
                                        #повествующие о происхождении конечного
                                        #файла. Прописываем также табличную шапку.
                                        if self.trg_file_fmt == 'vcf':
                                                trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                        trg_file_opened.write(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                        trg_file_opened.write(f'##src_file_name={src_file_name}\n')
                                        trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                                        trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                                        if not self.by_loc:
                                                trg_file_opened.write(f'##ann_field_name={self.ann_field_name}\n')
                                        if self.mongo_findone_args[1] is not None:
                                                trg_file_opened.write(f'##mongo_project={self.mongo_findone_args[1]}\n')
                                        trg_file_opened.write(header_line + '\n')
                                        
                                        #Извлечение из объекта курсора отвечающих запросу документов,
                                        #преобразование их значений в обычные строки и прописывание
                                        #последних в конечный файл. Проверка, вылез ли по запросу хоть
                                        #один документ. В аннотируемый набор затесались одинаковые элементы?
                                        #Ничего страшного - СУБД подготовит результат только для одного из них.
                                        #Также поступает BedTools, если к команде пересечения добавить опцию -u.
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
                        for src_coll_name in self.src_coll_names:
                                src_coll_obj = src_db_obj[src_coll_name]
                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                trg_coll_name = f'file_{src_file_base}__coll_{src_coll_base}.{self.trg_file_fmt}'
                                mongo_aggr_arg[-1]['$merge']['into']['coll'] = trg_coll_name
                                trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                                                            storageEngine={'wiredTiger':
                                                                                           {'configString':
                                                                                            'block_compressor=zstd'}})
                                meta_lines = {'meta': []}
                                if self.trg_file_fmt == 'vcf':
                                        meta_lines['meta'].append(f'##fileformat={self.trg_file_fmt.upper()}')
                                meta_lines['meta'].append(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>')
                                meta_lines['meta'].append(f'##src_file_name={src_file_name}')
                                meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                                meta_lines['meta'].append(f'##src_coll_name={src_coll_name}')
                                if not self.by_loc:
                                        meta_lines['meta'].append(f'##ann_field_name={self.ann_field_name}')
                                if self.mongo_findone_args[1] is not None:
                                        meta_lines['meta'].append(f'##mongo_project={self.mongo_findone_args[1]}')
                                trg_coll_obj.insert_one(meta_lines)
                                src_coll_obj.aggregate(mongo_aggr_arg)
                                if trg_coll_obj.count_documents({}) == 1:
                                        trg_db_obj.drop_collection(trg_coll_name)
                                else:
                                        index_models = create_index_models(self.trg_file_fmt,
                                                                           self.ind_field_names)
                                        if index_models != []:
                                                trg_coll_obj.create_indexes(index_models)
                                                
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#аннотирования. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nAnnotation by {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.annotate, main.src_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
