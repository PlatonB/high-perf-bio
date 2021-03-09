__version__ = 'v5.0'

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
                
def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Аннотируемый столбец:
- должен занимать одно и то же положение во всех исходных таблицах;
- целиком размещается в оперативную память, что может замедлить работу компьютера.

Также в качестве эксперимента существует возможность пересечения
по координатам. Поддерживаются все 4 комбинации VCF и BED.

Каждая аннотируемая таблица обязана быть сжатой с помощью GZIP.

Источником характеристик должна быть база данных, созданная с помощью create_db.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - аннотируемые таблицы определённого формата;
scr/trg-db-FMT - исходная/конечная БД с коллекциями,
соответствующими по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми аннотируемыми таблицами')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, по которой аннотировать')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать по геномной локации (экспериментальная фича; src-TSV, src-db-TSV: не применяется)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Номер аннотируемого столбца (применяется без -n; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                             help='Имя поля БД, по которому аннотировать (применяется без -n; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; src-db-VCF: не применяется; src-db-BED: trg-(db-)TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                             help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-(db-)BED): не применяется)')
        opt_grp.add_argument('-i', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Имена индексируемых полей (через запятую без пробела; trg-db-VCF: проиндексируются #CHROM+POS и ID; trg-db-BED: проиндексируются chrom+start+end и name)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно аннотируемых таблиц')
        args = arg_parser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под безопасное
        параллельное аннотирование набора таблиц.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной под многопроцессовое выполнение
                функции пересечения данных исходного файла с данными из базы. Атрибуты ни в
                коем случае не должны будут потом в параллельных процессах изменяться. Получаются
                они в основном из указанных исследователем аргументов. Немного о наиболее значимых
                атрибутах. Расширение исходных таблиц и квази-расширение коллекций нужны, как минимум,
                для выбора формат-ориентированного пересекательного запроса, определения правил
                сортировки и форматирования конечных файлов. Умолчания по столбцам и полям выбраны
                на основе здравого смысла: к примеру, аннотировать src-VCF по src-db-VCF или src-db-BED
                логично, пересекая столбец и поле, оба из которых с идентификаторами вариантов. Сортировка
                src-db-VCF и src-db-BED делается по координатам для обеспечения поддержки tabix-индексации
                конечных таблиц. Важные замечания по проджекшену. Для src-db-VCF его крайне трудно
                реализовать из-за наличия в соответствующих коллекциях разнообразных вложенных структур
                и запрета со стороны MongoDB на применение точечной формы обращения к отбираемым
                элементам массивов. Что касается src-db-BED, когда мы оставляем только часть
                полей, невозможно гарантировать соблюдение спецификаций BED-формата, поэтому
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
                self.meta_lines_quan = args.meta_lines_quan
                self.by_loc = args.by_loc
                if self.by_loc:
                        if self.src_file_fmt not in ['vcf', 'bed'] or self.src_coll_ext not in ['vcf', 'bed']:
                                raise ByLocTsvError()
                        self.mongo_aggregate_draft = [{'$match': {'$or': []}}]
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
                        self.mongo_aggregate_draft = [{'$match': {self.ann_field_name: {'$in': []}}}]
                if self.src_coll_ext == 'vcf':
                        self.mongo_aggregate_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                         ('POS', ASCENDING)])})
                elif self.src_coll_ext == 'bed':
                        self.mongo_aggregate_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                         ('start', ASCENDING),
                                                                         ('end', ASCENDING)])})
                if args.proj_fields is None or self.src_coll_ext == 'vcf':
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = self.src_coll_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_aggregate_draft.append({'$project': mongo_project})
                        self.mongo_findone_args = [None, mongo_project]
                        self.trg_file_fmt = 'tsv'
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
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
                        #Несмотря на угрозу перерасхода RAM, программа кладёт в один запрос сразу всё, что
                        #отобрано из исходного файла. Если запрашивать по одному элементу, то непонятно,
                        #как сортировать конечные данные. Неспособным на внешнюю сортировку питоновским
                        #sorted? А вдруг на запрос откликнется неприлично много документов?.. При создании
                        #БД для каждого значения устанавливался оптимальный тип данных. При работе с MongoDB
                        #важно соблюдать соответствие типа данных запрашиваемого слова и размещённых в базе
                        #значений. Для этого присвоим подходящий тип данных каждому аннотируемому элементу.
                        mongo_aggr_arg = copy.deepcopy(self.mongo_aggregate_draft)
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
                                
                                #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                trg_file_name = f'{src_file_base}_ann_by_{src_coll_base}.{self.trg_file_fmt}'
                                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                                
                                #Открытие конечного файла на запись.
                                with open(trg_file_path, 'w') as trg_file_opened:
                                        
                                        #Формируем и прописываем метастроки,
                                        #повествующие о происхождении конечного
                                        #файла. Прописываем также табличную шапку.
                                        if self.trg_file_fmt == 'vcf':
                                                trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                        trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                        trg_file_opened.write(f'##table={src_file_name}\n')
                                        trg_file_opened.write(f'##database={self.src_db_name}\n')
                                        trg_file_opened.write(f'##collection={src_coll_name}\n')
                                        if not self.by_loc:
                                                trg_file_opened.write(f'##field={self.ann_field_name}\n')
                                        if self.mongo_findone_args[1] is not None:
                                                trg_file_opened.write(f'##project={self.mongo_findone_args[1]}\n')
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
                                        
                #Создание конечной базы и коллекций. При
                #работе с каждой исходной коллекцией делается
                #обогащение aggregation-инструкции этапом
                #вывода в конечную коллекцию. Последняя,
                #если не пополнилась результатами, удаляется.
                #Для непустых конечных коллекций создаются
                #обязательные и пользовательские индексы.
                elif hasattr(self, 'trg_db_name'):
                        trg_db_obj = client[self.trg_db_name]
                        for src_coll_name in self.src_coll_names:
                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                trg_coll_name = f'{src_file_base}_ann_by_{src_coll_base}.{self.trg_file_fmt}'
                                src_coll_obj = src_db_obj[src_coll_name]
                                trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                                                            storageEngine={'wiredTiger':
                                                                                           {'configString':
                                                                                            'block_compressor=zstd'}})
                                mongo_aggr_arg.append({'$out': {'db': self.trg_db_name,
                                                                'coll': trg_coll_name}})
                                src_coll_obj.aggregate(mongo_aggr_arg)
                                if trg_coll_obj.count_documents({}) == 0:
                                        trg_db_obj.drop_collection(trg_coll_name)
                                else:
                                        index_models = create_index_models(self.trg_file_fmt,
                                                                           self.ind_field_names)
                                        if index_models != []:
                                                trg_coll_obj.create_indexes(index_models)
                                del mongo_aggr_arg[-1]
                                
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime, gzip, copy

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from multiprocessing import Pool
from bson.son import SON
from backend.def_data_type import def_data_type
from backend.doc_to_line import restore_line
from backend.create_index_models import create_index_models

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#аннотируемых файлов, определение
#оптимального числа процессов.
args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args,
                                  __version__)
src_file_names = prep_single_proc.src_file_names
src_files_quan = len(src_file_names)
if max_proc_quan > src_files_quan <= 8:
        proc_quan = src_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nAnnotation by {prep_single_proc.src_db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск аннотирования. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.annotate, src_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
