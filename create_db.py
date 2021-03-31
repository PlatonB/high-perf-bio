__version__ = 'v3.5'

class DifFmtsError(Exception):
        '''
        Создавать базу можно только по одноформатным таблицам.
        '''
        def __init__(self, src_file_fmts):
                err_msg = f'\nSource files are in different formats: {src_file_fmts}'
                super().__init__(err_msg)
                
def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, создающая MongoDB-базу данных
по VCF, BED или любым другим таблицам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Формат исходных таблиц:
- Должен быть одинаковым для всех.
- Определяется программой по расширению.

TSV: так будет условно обозначаться неопределённый табличный формат.

Требования к наличию табличной шапки (набора имён столбцов)
и подсчёту количества нечитаемых программой строк:

Формат     |  Наличие         |  Модификация значения
src-файла  |  шапки           |  -m при наличии шапки
-----------------------------------------------------
VCF        |  Обязательно     |  Аргумент не применим
BED        |  Не обязательно  |  Прибавьте 1
TSV        |  Обязательно     |  Не прибавляйте 1

Каждая исходная таблица должна быть сжата с помощью GZIP.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - исходные таблицы определённого формата (VCF, BED, TSV);
trg-db-FMT - коллекции БД, полученные из таблиц определённого формата;
не применяется - при обозначенных условиях аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля коллекций БД с составным индексом
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми таблицами, преобразуемыми в коллекции MongoDB-базы')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-d', '--trg-db-name', metavar='[None]', dest='trg_db_name', type=str,
                             help='Имя создаваемой базы данных ([[имя папки со сжатыми таблицами]])')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку)')
        opt_grp.add_argument('-r', '--minimal', dest='minimal', action='store_true',
                             help='Загружать только минимально допустимый форматом набор столбцов (src-VCF: 1-ые 8; src-BED: 1-ые 3; src-TSV: не применяется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[None]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для разбиения ячейки на список (src-VCF, src-BED: не применяется)')
        opt_grp.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                             help='Максимальное количество строк фрагмента заливаемой таблицы')
        opt_grp.add_argument('-i', '--ind-col-names', metavar='[None]', dest='ind_col_names', type=str,
                             help='Имена индексируемых полей (через запятую без пробела; trg-db-VCF: проиндексируются #CHROM+POS и ID; trg-db-BED: проиндексируются chrom+start+end и name)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
        args = arg_parser.parse_args()
        return args

def process_info_cell(info_cell):
        '''
        Функция преобразования ячейки INFO-столбца VCF-таблицы в список
        из словаря и списка. У INFO есть такая особенность: одни элементы
        представляют собой пары ключ-значение, другие (далее - флаги)
        идут без ключей. Пары разместятся в словарь, а флаги - в список,
        расположенный на одинаковом со словарём уровне. Если у одного ключа
        несколько значений, программа положит их в подсписок. Поскольку
        официальные рекомендации по составу INFO-столбца не являются
        строгими, тип данных каждого элемента приходится подбирать.
        '''
        info_row, info_obj = info_cell.split(';'), [{}, []]
        for info_subcell in info_row:
                if '=' in info_subcell:
                        pair = info_subcell.split('=')
                        if ',' in pair[1]:
                                pair[1] = [def_data_type(val) for val in pair[1].split(',')]
                        else:
                                pair[1] = def_data_type(pair[1])
                        info_obj[0][pair[0]] = pair[1]
                else:
                        info_obj[1].append(def_data_type(info_subcell))
        return info_obj

def process_gt_cell(format_cell, gt_cell):
        '''
        Функция объединения ячеек FORMAT- и GT-столбца VCF-таблицы
        в словарь. Из-за непредсказуемости состава GT-столбца для
        каждого значения тип данных будет определяться подбором.
        '''
        format_row, gt_row, gt_two_dim = format_cell.split(':'), gt_cell.split(':'), []
        for gt_subcell in gt_row:
                if ',' in gt_subcell:
                        gt_two_dim.append([def_data_type(val) for val in gt_subcell.split(',')])
                else:
                        gt_two_dim.append(def_data_type(gt_subcell))
        gt_obj = dict(zip(format_row, gt_two_dim))
        return gt_obj

class PrepSingleProc():
        '''
        Класс, спроектированный под безопасное
        параллельное создание базы данных.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых заточенной под многопроцессовое
                выполнение функции построения коллекций MongoDB с нуля. Атрибуты ни в
                коем случае не должны будут потом в параллельных процессах изменяться.
                Получаются они в основном из указанных исследователем опций. Разрешение
                конфликта в случае существования БД с тем же именем, что и у создаваемой.
                '''
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
                src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                        self.src_file_names))
                if len(src_file_fmts) > 1:
                        raise DifFmtsError(src_file_fmts)
                self.src_file_fmt = list(src_file_fmts)[0]
                if args.trg_db_name is None:
                        self.trg_db_name = os.path.basename(self.src_dir_path)
                else:
                        self.trg_db_name = args.trg_db_name
                resolve_db_existence(self.trg_db_name)
                self.meta_lines_quan = args.meta_lines_quan
                self.minimal = args.minimal
                if args.sec_delimiter is None:
                        self.sec_delimiter = args.sec_delimiter
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'low_line':
                        self.sec_delimiter = '_'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                self.max_fragment_len = args.max_fragment_len
                if args.ind_col_names is None:
                        self.ind_col_names = args.ind_col_names
                else:
                        self.ind_col_names = args.ind_col_names.split(',')
                        
        def create_collection(self, src_file_name):
                '''
                Функция создания и наполнения одной
                MongoDB-коллекции данными одного файла,
                а также индексации полей этой коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                trg_db_obj = client[self.trg_db_name]
                
                #Открытие исходной архивированной таблицы на чтение.
                with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
                        
                        #Политика обработки метастрок задаётся исследователем.
                        #В любом случае, всё сводится к их холостому прочтению.
                        #Программа либо выявляет идущие в игнор строки по
                        #характерным для биоинформатических форматов комментирующим
                        #символам, либо скипает фиксированное количество строк
                        #начала файла. После метастрок, по-хорошему, должна
                        #следовать шапка, но во многих BED-файлах её нет. Для BED
                        #приходится вручную вписывать в код референсную шапку.
                        if self.src_file_fmt == 'vcf':
                                for line in src_file_opened:
                                        if not line.startswith('##'):
                                                header_row = line.rstrip().split('\t')
                                                if len(header_row) > 8:
                                                        del header_row[8]
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        src_file_opened.readline()
                                if self.src_file_fmt == 'bed':
                                        header_row = ['chrom', 'start', 'end', 'name',
                                                      'score', 'strand', 'thickStart', 'thickEnd',
                                                      'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
                                else:
                                        header_row = src_file_opened.readline().rstrip().split('\t')
                                        
                        #Создание коллекции. Для оптимального соотношения
                        #скорости записи/извлечения с объёмом хранимых данных,
                        #я выбрал в качестве алгоритма сжатия Zstandard.
                        coll_obj = trg_db_obj.create_collection(src_file_name[:-3],
                                                                storageEngine={'wiredTiger':
                                                                               {'configString':
                                                                                'block_compressor=zstd'}})
                        
                        #Данные будут поступать в коллекцию
                        #базы одним или более фрагментами.
                        #Для контроля работы с фрагментами
                        #далее будет отмеряться их размер.
                        #Стартовое значение размера - 0 строк.
                        fragment, fragment_len = [], 0
                        
                        #Коллекция БД будет пополняться
                        #до тех пор, пока не закончится
                        #перебор строк исходной таблицы.
                        for line in src_file_opened:
                                
                                #Преобразование очередной строки
                                #исходной таблицы в список.
                                row = line.rstrip().split('\t')
                                
                                #MongoDB позволяет размещать в одну коллекцию документы
                                #с переменным количеством полей и разными типами данных
                                #значений. Воспользуемся такой гибкостью СУБД, создавая
                                #структуры, максимально заточенные под содержимое конкретной
                                #исходной строки. VCF и BED обрабатываются полностью автоматически:
                                #значениям определённых столбцов присваиваются типы данных int
                                #и decimal, где-то производится разбивка на списки, а INFO- и
                                #GT-ячейки конвертируются в многослойные структуры. Для кастомных
                                #табличных форматов типы данных определяются подбором по принципу
                                #"подходит - не подходит", а разбиение на списки делается при
                                #наличии в ячейке обозначенного исследователем разделителя.
                                if self.src_file_fmt == 'vcf':
                                        row[0] = def_data_type(row[0].replace('chr', ''))
                                        row[1] = int(row[1])
                                        if ';' in row[2]:
                                                row[2] = row[2].split(';')
                                        if ',' in row[4]:
                                                row[4] = row[4].split(',')
                                        row[5] = def_data_type(row[5])
                                        row[7] = process_info_cell(row[7])
                                        if self.minimal:
                                                row = row[:8]
                                        elif len(row) > 8:
                                                gt_objs = [process_gt_cell(row[8], gt_cell) for gt_cell in row[9:]]
                                                row = row[:8] + gt_objs
                                elif self.src_file_fmt == 'bed':
                                        row[0] = def_data_type(row[0].replace('chr', ''))
                                        row[1], row[2] = int(row[1]), int(row[2])
                                        if self.minimal:
                                                row = row[:3]
                                        elif len(row) > 4:
                                                row[4] = int(row[4])
                                                if len(row) > 6:
                                                        row[6], row[7], row[9] = int(row[6]), int(row[7]), int(row[9])
                                                        row[10] = list(map(int, row[10].rstrip(',').split(',')))
                                                        row[11] = list(map(int, row[11].rstrip(',').split(',')))
                                else:
                                        for cell_index in range(len(row)):
                                                if self.sec_delimiter is not None and self.sec_delimiter in row[cell_index]:
                                                        row[cell_index] = row[cell_index].split(self.sec_delimiter)
                                                        for subcell_index in range(len(row[cell_index])):
                                                                row[cell_index][subcell_index] = def_data_type(row[cell_index][subcell_index])
                                                else:
                                                        row[cell_index] = def_data_type(row[cell_index])
                                                        
                                #MongoDB - документоориентированная СУБД.
                                #Каждая коллекция строится из т.н. документов,
                                #Python-предшественниками которых могут быть
                                #только словари. Поэтому для подготовки размещаемого
                                #в базу фрагмента сшиваем из списка элементов
                                #шапки и списка, созданного из очередной строки,
                                #словарь, затем добавляем его в список таких словарей.
                                #Набор ключей любого словаря может получиться меньшим,
                                #чем шапка. Есть две возможные причины срезания: 1.
                                #Если реальный BED отстаёт по количеству элементов
                                #от ранее подготовленной стандартной 12-элементной
                                #шапки; 2. Когда по инициативе исследователя в коллекцию
                                #направляются лишь основополагающие столбцы VCF или BED.
                                fragment.append(dict(zip(header_row[:len(row)], row)))
                                
                                #Сразу после пополнения
                                #фрагмента регистрируем это
                                #событие с помощью счётчика.
                                fragment_len += 1
                                
                                #Исходная таблица ещё не до конца
                                #считалась, а фрагмент достиг порогового
                                #значения количества строк. Тогда
                                #прописываем фрагмент в коллекцию,
                                #очищаем его и обнуляем счётчик.
                                if fragment_len == self.max_fragment_len:
                                        coll_obj.insert_many(fragment)
                                        fragment.clear()
                                        fragment_len = 0
                                        
                #Чтение исходной таблицы
                #завершилось, но остался
                #непрописанный фрагмент.
                #Исправляем ситуацию.
                if fragment_len > 0:
                        coll_obj.insert_many(fragment)
                        
                #Создание обязательных и (при наличии
                #таковых) пользовательских индексов.
                index_models = create_index_models(self.src_file_fmt,
                                                   self.ind_col_names)
                if index_models != []:
                        coll_obj.create_indexes(index_models)
                        
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime, gzip

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient
from backend.resolve_db_existence import resolve_db_existence
from multiprocessing import Pool
from backend.def_data_type import def_data_type
from backend.create_index_models import create_index_models

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса, определение
#оптимального количества процессов.
args = add_args(__version__)
prep_single_proc = PrepSingleProc(args)
max_proc_quan = args.max_proc_quan
src_file_names = prep_single_proc.src_file_names
src_files_quan = len(src_file_names)
if max_proc_quan > src_files_quan <= 8:
        proc_quan = src_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nReplenishment and indexing {prep_single_proc.trg_db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск создания коллекций. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.create_collection,
                     src_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
