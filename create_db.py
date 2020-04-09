__version__ = 'V2.0'

def add_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, создающая MongoDB-базу данных.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: {__version__}.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).

Формат исходных таблиц:
- Должен быть одинаковым для всех.
- Определяется программой по расширению.

TSV: так будет условно обозначаться
неопределённый табличный формат.

Требование к наличию табличной шапки:
- VCF: шапка обязательна.
- BED: шапка не нужна, но
если она есть, включите её
в список нечитаемых строк
флагом --meta-lines-quan.
- TSV: шапка обязательна,
включать её в список
нечитаемых строк нельзя.

Каждая исходная таблица должна
быть сжата с помощью GZIP.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений;
- через плюс - перечисление будущих полей с составным индексом.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-S', '--arc-dir-path', metavar='str', dest='arc_dir_path', type=str,
                               help='Путь к папке со сжатыми таблицами, преобразуемыми в коллекции MongoDB-базы')
        argparser.add_argument('-d', '--db-name', metavar='[None]', dest='db_name', type=str,
                               help='Имя создаваемой базы данных (по умолчанию - имя папки со сжатыми таблицами)')
        argparser.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                               help='Количество строк метаинформации (VCF: опция не применяется; TSV: нельзя включать шапку)')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[None]', choices=['comma', 'semicolon', 'colon', 'pipe'], dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для разбиения ячейки на список (VCF, BED: опция не применяется)')
        argparser.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                               help='Максимальное количество строк фрагмента заливаемой в БД таблицы')
        argparser.add_argument('-i', '--ind-col-names', metavar='[None]', dest='ind_col_names', type=str,
                               help='Имена индексируемых полей (через запятую без пробела; VCF: проиндексируются #CHROM+POS и ID; BED: проиндексируются chrom+start+end)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
        args = argparser.parse_args()
        return args

def remove_database(db_name):
        '''
        Функция, дающая возможность
        полного удаления базы данных.
        '''
        client = MongoClient()
        if db_name in client.list_database_names():
                db_to_remove = input(f'''\nБаза данных {db_name} уже существует.
Для пересоздания введите её имя: ''')
                if db_to_remove == db_name:
                        client.drop_database(db_name)
                else:
                        print(f'\nБаза данных {db_name} останется')
                        client.close()
                        sys.exit()
                        
        client.close()
        
def def_data_type(string):
        '''
        Функция, подбирающая подходящий
        тип данных какого-либо значения.
        Если ничего не подходит, то
        значение останется строкой.
        '''
        try:
                result = int(string)
        except ValueError:
                try:
                        result = Decimal128(string)
                except InvalidOperation:
                        result = string
        return result

def process_info_cell(info_cell):
        '''
        Функция преобразования ячейки
        INFO-столбца VCF-таблицы в
        список из словаря и списка.
        У INFO есть такая особенность:
        одни элементы представляют
        собой пары ключ-значение, другие
        (далее - флаги) идут без ключей.
        Пары разместятся в словарь, а
        флаги - в список, расположенный
        на одинаковом со словарём уровне.
        Если у одного ключа несколько значений,
        программа положит их в подсписок.
        Поскольку официальные рекомендации
        по составу INFO-столбца не являются
        строгими, тип данных каждого
        элемента приходится подбирать.
        '''
        info_row, info_obj = info_cell.split(';'), [{}, []]
        for info_subcell in info_row:
                if info_subcell.find('=') != -1:
                        pair = info_subcell.split('=')
                        if pair[1].find(',') != -1:
                                pair[1] = [def_data_type(val) for val in pair[1].split(',')]
                        else:
                                pair[1] = def_data_type(pair[1])
                        info_obj[0][pair[0]] = pair[1]
                else:
                        info_obj[1].append(def_data_type(info_subcell))
        return info_obj

def process_gt_cell(format_cell, gt_cell):
        '''
        Функция объединения ячеек FORMAT-
        и GT-столбца VCF-таблицы в словарь.
        Из-за непредсказуемости состава
        GT-столбца для каждого значения тип
        данных будет определяться подбором.
        '''
        format_row, gt_row, gt_two_dim = format_cell.split(':'), gt_cell.split(':'), []
        for gt_subcell in gt_row:
                if gt_subcell.find(',') != -1:
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
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции построения
                коллекций MongoDB с нуля.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.arc_dir_path = os.path.normpath(args.arc_dir_path)
                if args.db_name == None:
                        self.db_name = os.path.basename(self.arc_dir_path)
                else:
                        self.db_name = args.db_name
                self.meta_lines_quan = args.meta_lines_quan
                if args.sec_delimiter == None:
                        self.sec_delimiter = args.sec_delimiter
                elif args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                self.max_fragment_len = args.max_fragment_len
                if args.ind_col_names == None:
                        self.ind_col_names = args.ind_col_names
                else:
                        self.ind_col_names = args.ind_col_names.split(',')
                        
        def create_collection(self, arc_file_name):
                '''
                Функция создания и наполнения
                одной MongoDB-коллекции
                данными одного файла,
                а также индексации
                выбранных исследователем
                полей этой коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                db_obj = client[self.db_name]
                
                #Автоматическое определение
                #формата исходной сжатой таблицы.
                src_file_format = arc_file_name.split('.')[-2]
                
                #Открытие исходной архивированной таблицы на чтение.
                with gzip.open(os.path.join(self.arc_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                        
                        #Политика обработки метастрок
                        #задаётся исследователем.
                        #В любом случае, всё сводится
                        #к их холостому прочтению.
                        #Программа либо выявляет идущие
                        #в игнор строки по характерным
                        #для биоинформатических
                        #форматов комментирующим
                        #символам, либо скипает
                        #фиксированное количество
                        #строк начала файла.
                        #После метастрок, по-хорошему,
                        #должна следовать шапка, но
                        #во многих BED-файлах её нет.
                        #Для BED приходится искусственно
                        #прописывать референсную шапку.
                        if src_file_format == 'vcf':
                                for line in arc_file_opened:
                                        if line.startswith('##') == False:
                                                header_row = line.rstrip().split('\t')
                                                if len(header_row) > 8:
                                                        del header_row[8]
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        arc_file_opened.readline()
                                if src_file_format == 'bed':
                                        header_row = ['chrom', 'start', 'end', 'name',
                                                      'score', 'strand', 'thickStart', 'thickEnd',
                                                      'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
                                else:
                                        header_row = arc_file_opened.readline().rstrip().split('\t')
                                        
                        #Создание коллекции.
                        #Для оптимального соотношения
                        #скорости записи/извлечения
                        #с объёмом хранимых данных,
                        #я выбрал в качестве
                        #алгоритма сжатия Zstandard.
                        coll_obj = db_obj.create_collection(arc_file_name[:-3],
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
                        for line in arc_file_opened:
                                
                                #Преобразование очередной строки
                                #исходной таблицы в список.
                                row = line.rstrip().split('\t')
                                
                                #MongoDB позволяет размещать в одну
                                #коллекцию документы с переменным количеством
                                #полей и разными типами данных значений.
                                #Воспользуемся такой гибкостью СУБД,
                                #создавая структуры, максимально заточенные
                                #под содержимое конкретной исходной строки.
                                #VCF и BED обрабатываются полностью
                                #автоматически: значениям определённых
                                #столбцов присваиваются типы данных
                                #int и decimal, где-то производится
                                #разбивка на списки, а INFO- и GT-ячейки
                                #конвертируются в многослойные структуры.
                                #Для кастомных табличных форматов типы
                                #данных определяются подбором по
                                #принципу "подходит - не подходит",
                                #а разбиение на списки делается
                                #при наличии в ячейке обозначенного
                                #исследователем разделителя.
                                if src_file_format == 'vcf':
                                        row[1] = int(row[1])
                                        if row[2].find(';') != -1:
                                                row[2] = row[2].split(';')
                                        if row[4].find(',') != -1:
                                                row[4] = row[4].split(',')
                                        row[5] = def_data_type(row[5])
                                        row[7] = process_info_cell(row[7])
                                        if len(row) > 8:
                                                gt_objs = [process_gt_cell(row[8], gt_cell) for gt_cell in row[9:]]
                                                row = row[:8] + gt_objs
                                elif src_file_format == 'bed':
                                        row[1], row[2] = int(row[1]), int(row[2])
                                        if len(row) > 4:
                                                row[4] = int(row[4])
                                        if len(row) > 6:
                                                row[6], row[7], row[9] = int(row[6]), int(row[7]), int(row[9])
                                                row[10] = list(map(int, row[10].rstrip(',').split(',')))
                                                row[11] = list(map(int, row[11].rstrip(',').split(',')))
                                else:
                                        for cell_index in range(len(row)):
                                                if self.sec_delimiter != None and row[cell_index].find(self.sec_delimiter) != -1:
                                                        row[cell_index] = row[cell_index].split(self.sec_delimiter)
                                                        for subcell_index in range(len(row[cell_index])):
                                                                row[cell_index][subcell_index] = def_data_type(row[cell_index][subcell_index])
                                                else:
                                                        row[cell_index] = def_data_type(row[cell_index])
                                                        
                                #MongoDB - документоориентированная СУБД.
                                #Каджая коллекция строится из т.н.
                                #документов, Python-предшественниками
                                #которых могут быть только словари.
                                #Поэтому для подготовки размещаемого
                                #в базу фрагмента сшиваем из списка
                                #элементов шапки и списка, созданного
                                #из очередной строки, словарь,
                                #добавляем его в список таких словарей.
                                #Поскольку для BED мы готовили
                                #стандартную 12-элементную шапку,
                                #то если реальная шапка окажется
                                #меньше, она будет срезана
                                #до числа элементов списка,
                                #сделанного из текущей строки.
                                fragment.append(dict(zip(header_row[:len(row)], row)))
                                
                                #Сразу после пополнения
                                #фрагмента регистрируем это
                                #событие с помощью счётчика.
                                fragment_len += 1
                                
                                #Исходная таблица ещё не до
                                #конца считалась, а фрагмент достиг
                                #порогового значения количества строк.
                                #Тогда прописываем фрагмент в коллекцию,
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
                        
                #Независимо от того, указал
                #исследователь имена индексируемых
                #полей или нет, для ex-VCF
                #и ex-BED индексация будет
                #произведена по первым трём полям.
                #Притом индексы хромосом и
                #позиций построятся составные,
                #что потом позволит парсерам
                #эффективно сортировать по
                #этим полям свои результаты.
                #Для обозначенных исследователем
                #полей появятся раздельные индексы.
                #Если они придутся на хромосому
                #или позицию ex-VCF/ex-BED,
                #то будут сосуществовать с
                #обязательным компаундным индексом.
                if src_file_format == 'vcf':
                        index_models = [IndexModel([('#CHROM', ASCENDING),
                                                    ('POS', ASCENDING)]),
                                        IndexModel([('ID', ASCENDING)])]
                elif src_file_format == 'bed':
                        index_models = [IndexModel([('chrom', ASCENDING),
                                                    ('start', ASCENDING),
                                                    ('end', ASCENDING)])]
                else:
                        index_models = []
                if self.ind_col_names != None:
                        index_models += [IndexModel([(ind_col_name, ASCENDING)]) for ind_col_name in self.ind_col_names]
                if index_models != []:
                        coll_obj.create_indexes(index_models)
                        
                client.close()
                
####################################################################################################

import sys, os, gzip
from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, IndexModel, ASCENDING
from multiprocessing import Pool
from bson.decimal128 import Decimal128
from decimal import InvalidOperation

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#удаление старой базы, определение
#оптимального количества процессов.
args = add_args()
prep_single_proc = PrepSingleProc(args)
db_name = prep_single_proc.db_name
remove_database(db_name)
max_proc_quan = args.max_proc_quan
arc_file_names = os.listdir(prep_single_proc.arc_dir_path)
arc_files_quan = len(arc_file_names)
if max_proc_quan > arc_files_quan <= 8:
        proc_quan = arc_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nПополнение и индексация БД {db_name}')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Параллельный запуск создания коллекций.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.create_collection, arc_file_names)
