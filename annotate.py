__version__ = 'v4.3'

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
                err_msg = '\nIntersection by location is not possible for src-TSV or db-TSV'
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
src-FMT - аннотируемые таблицы определённого формата (VCF, BED, TSV);
db-FMT - коллекции БД, полученные из таблиц определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях аргумент проигнорируется или вызовет ошибку
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми аннотируемыми таблицами')
        man_grp.add_argument('-D', '--db-name', required=True, metavar='str', dest='db_name', type=str,
                             help='Имя БД, по которой аннотировать')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-t', '--trg-top-dir-path', metavar='[None]', dest='trg_top_dir_path', type=str,
                             help='Путь к папке для результатов ([[путь к исходной папке]])')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать по геномной локации (экспериментальная фича; src-TSV, db-TSV: не применяется)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Номер аннотируемого столбца (применяется без -l; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                             help='Имя поля БД, по которому аннотировать (применяется без -l; db-VCF: [[ID]]; db-BED: [[name]]; db-TSV: [[rsID]])')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; db-VCF: не применяется; db-BED: trg-TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                             help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (db-VCF, db-BED (trg-BED): не применяется)')
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
                функции пересечения данных исходного файла с данными из базы. Атрибуты ни
                в коем случае не должны будут потом в параллельных процессах изменяться.
                Получаются они в основном из указанных исследователем опций. Немного о
                наиболее значимых атрибутах. Расширение исходных таблиц и квази-расширение
                коллекций нужны, как минимум, для выбора формат-ориентированного пересекательного
                запроса, определения правил сортировки и форматирования конечных файлов. Умолчания
                по столбцам и полям выбраны на основе здравого смысла: к примеру, аннотировать VCF
                логично, пересекая столбец и поле, оба из которых с идентификаторами вариантов.
                Важные замечания по проджекшену. Для db-VCF его крайне трудно реализовать
                из-за наличия в соответствующих коллекциях разнообразных вложенных структур
                и запрета со стороны MongoDB на применение точечной формы обращения к отбираемым
                элементам массивов. Что касается db-BED, когда мы оставляем только часть
                полей, невозможно гарантировать соблюдение спецификаций BED-формата, поэтому
                вывод будет формироваться не более, чем просто табулированным (trg-TSV).
                Сортировка BED и VCF делается по координатам для поддержки tabix-индексации.
                '''
                client = MongoClient()
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
                src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                        self.src_file_names))
                if len(src_file_fmts) > 1:
                        raise DifFmtsError(src_file_fmts)
                self.src_file_fmt = list(src_file_fmts)[0]
                self.db_name = args.db_name
                self.coll_names = client[self.db_name].list_collection_names()
                self.coll_name_ext = self.coll_names[0].rsplit('.', maxsplit=1)[1]
                if args.trg_top_dir_path is None:
                        self.trg_top_dir_path = self.src_dir_path
                else:
                        self.trg_top_dir_path = os.path.normpath(args.trg_top_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                self.by_loc = args.by_loc
                if self.by_loc:
                        if self.src_file_fmt not in ['vcf', 'bed'] or self.coll_name_ext not in ['vcf', 'bed']:
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
                                if self.coll_name_ext == 'vcf':
                                        self.ann_field_name = 'ID'
                                elif self.coll_name_ext == 'bed':
                                        self.ann_field_name = 'name'
                                else:
                                        self.ann_field_name = 'rsID'
                        else:
                                self.ann_field_name = args.ann_field_name
                        self.mongo_aggregate_draft = [{'$match': {self.ann_field_name: {'$in': []}}}]
                if args.proj_fields is None or self.coll_name_ext == 'vcf':
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = self.coll_name_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_aggregate_draft.append({'$project': mongo_project})
                        self.mongo_findone_args = [None, mongo_project]
                        self.trg_file_fmt = 'tsv'
                if self.trg_file_fmt == 'vcf':
                        self.mongo_aggregate_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                         ('POS', ASCENDING)])})
                elif self.trg_file_fmt == 'bed':
                        self.mongo_aggregate_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                                         ('start', ASCENDING),
                                                                         ('end', ASCENDING)])})
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
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
                db_obj = client[self.db_name]
                
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
                        mongo_aggregate_arg = copy.deepcopy(self.mongo_aggregate_draft)
                        for src_line in src_file_opened:
                                src_row = src_line.rstrip().split('\t')
                                if self.by_loc:
                                        if self.src_file_fmt == 'vcf':
                                                src_chrom, src_pos = def_data_type(src_row[0].replace('chr', '')), int(src_row[1])
                                                if self.coll_name_ext == 'vcf':
                                                        mongo_aggregate_arg[0]['$match']['$or'].append({'#CHROM': src_chrom,
                                                                                                        'POS': src_pos})
                                                elif self.coll_name_ext == 'bed':
                                                        mongo_aggregate_arg[0]['$match']['$or'].append({'chrom': src_chrom,
                                                                                                        'start': {'$lt': src_pos},
                                                                                                        'end': {'$gte': src_pos}})
                                        elif self.src_file_fmt == 'bed':
                                                src_chrom, src_start, src_end = def_data_type(src_row[0].replace('chr', '')), int(src_row[1]), int(src_row[2])
                                                if self.coll_name_ext == 'vcf':
                                                        mongo_aggregate_arg[0]['$match']['$or'].append({'#CHROM': src_chrom,
                                                                                                        'POS': {'$gt': src_start,
                                                                                                                '$lte': src_end}})
                                                elif self.coll_name_ext == 'bed':
                                                        mongo_aggregate_arg[0]['$match']['$or'].append({'chrom': src_chrom,
                                                                                                        'start': {'$lt': src_end},
                                                                                                        'end': {'$gt': src_start}})
                                else:
                                        mongo_aggregate_arg[0]['$match'][self.ann_field_name]['$in'].append(def_data_type(src_row[self.ann_col_index]))
                                        
                #Построение имени подпапки для результатов работы
                #над текущим исходным файлом и пути к этой подпапке.
                src_file_base = src_file_name.rsplit('.', maxsplit=2)[0]
                trg_dir_path = os.path.join(self.trg_top_dir_path,
                                            f'{src_file_base}_ann')
                
                #Обработка каждой исходной
                #таблицы производится по всем
                #коллекциям MongoDB-базы. Т.е.
                #даже, если по одной из коллекций
                #уже получились результаты, обход
                #будет продолжаться и завершится лишь
                #после обращения к последней коллекции.
                for coll_name in self.coll_names:
                        
                        #Создание двух объектов: текущей коллекции и курсора.
                        coll_obj = db_obj[coll_name]
                        curs_obj = coll_obj.aggregate(mongo_aggregate_arg)
                        
                        #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                        #коллекция, создадим её из имён полей. Projection при этом учтём.
                        #Имя сугубо технического поля _id проигнорируется. Если в db-VCF
                        #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                        header_row = list(coll_obj.find_one(*self.mongo_findone_args))[1:]
                        if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Создание конечной подпапки. Не факт,
                        #что она просуществует до окончания
                        #выполнения программы, т.к. за отсутствие
                        #в ней результатов полагается удаление.
                        if not os.path.exists(trg_dir_path):
                                os.mkdir(trg_dir_path)
                                
                        #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                        coll_name_base = coll_name.rsplit('.', maxsplit=1)[0]
                        trg_file_name = f'{src_file_base}_ann_by_{coll_name_base}.{self.trg_file_fmt}'
                        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                if self.trg_file_fmt == 'vcf':
                                        trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##table={src_file_name}\n')
                                trg_file_opened.write(f'##database={self.db_name}\n')
                                trg_file_opened.write(f'##collection={coll_name}\n')
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
                                
                #Удаление оставшейся пустой подпапки.
                try:
                        os.rmdir(trg_dir_path)
                except OSError:
                        pass
                
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime, gzip, copy

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from multiprocessing import Pool
from bson.son import SON
from backend.def_data_type import def_data_type
from backend.doc_to_line import restore_line

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
        
print(f'\nAnnotation by {prep_single_proc.db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск аннотирования. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.annotate, src_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
