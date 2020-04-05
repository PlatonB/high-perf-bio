__version__ = 'V2.3'

print('''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V2.3.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues
Справка по CLI: python3 annotator.py -h

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).

Аннотируемый столбец:
- должен занимать одно и то же
положение во всех исходных таблицах;
- целиком размещается в оперативную память,
что может замедлить работу компьютера.

Каждая аннотируемая таблица
должна быть сжата с помощью GZIP.

Источником характеристик должна быть
база данных, созданная с помощью create_db.

Желательно, чтобы поле БД, по которому
надо аннотировать, было проиндексировано.
''')

def add_main_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description='''
Краткая форма с большой буквы - обязательный аргумент.
В квадратных скобках - значение по умолчанию.
В фигурных скобках - перечисление возможных значений.
''')
        argparser.add_argument('-S', '--arc-dir-path', metavar='str', dest='arc_dir_path', type=str,
                               help='Путь к папке со сжатыми аннотируемыми таблицами')
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой аннотировать')
        argparser.add_argument('-t', '--trg-top-dir-path', metavar='[None]', dest='trg_top_dir_path', type=str,
                               help='Путь к папке для результатов (по умолчанию - путь к исходной папке)')
        argparser.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                               help='Номер аннотируемого столбца (src-VCF: 3 по умолчанию; src-BED: 4 по умолчанию; src-TSV: 1 по умолчанию)')
        argparser.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                               help='Имя поля БД, по которому аннотировать (trg-VCF: ID по умолчанию; trg-BED: name по умолчанию; trg-TSV: ID по умолчанию)')
        argparser.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                               help='Количество строк метаинформации аннотируемых таблиц (src-VCF: опция не применяется; src-BED, src-TSV: следует включать шапку)')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', default='comma', choices=['comma', 'semicolon', 'colon', 'pipe'], dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (trg-VCF, trg-BED: опция не применяется)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно аннотируемых таблиц')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный
        под безопасное параллельное
        аннотирование определённого
        столбца набора таблиц.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции получения
                характеристик элементов столбца.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.arc_dir_path = os.path.normpath(args.arc_dir_path)
                self.db_name = args.db_name
                if args.trg_top_dir_path == None:
                        self.trg_top_dir_path = self.arc_dir_path
                else:
                        self.trg_top_dir_path = os.path.normpath(args.trg_top_dir_path)
                if args.ann_col_num == None:
                        self.ann_col_index = args.ann_col_num
                else:
                        self.ann_col_index = args.ann_col_num - 1
                self.ann_field_name = args.ann_field_name
                self.meta_lines_quan = args.meta_lines_quan
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                        
        def annotate(self, arc_file_name):
                '''
                Функция аннотирования
                столбца одной таблицы.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                db_obj = client[self.db_name]
                
                #Аннотирование предполагается
                #проводить по всем коллекциям
                #базы, поэтому потребуется
                #полный набор имён коллекций.
                coll_names = db_obj.list_collection_names()
                
                #Определение формата исходной сжатой таблицы.
                src_file_format = arc_file_name.split('.')[-2]
                
                #На случай, если исследователь
                #не выберет аннотируемый столбец,
                #предусмотрены столбцы по умолчанию.
                #В биоинформатике чаще всего
                #требуется аннотировать SNPs,
                #поэтому дефолтные значения
                #для исходных VCF и BED
                #указывают на rsID-столбец.
                #В случае неспецифического
                #формата (далее - TSV) приходится
                #действовать пальцем в небо:
                #расчёт идёт на то, что аннотируемый
                #столбец - единственный (как
                #во входном формате "Variant
                #identifiers" программы Ensembl VEP).
                if self.ann_col_index == None:
                        if src_file_format == 'vcf':
                                ann_col_index = 2
                        elif src_file_format == 'bed':
                                ann_col_index = 3
                        else:
                                ann_col_index = 0
                else:
                        ann_col_index = self.ann_col_index
                        
                #Открытие исходной архивированной таблицы на
                #чтение, смещение курсора к её основной части.
                with gzip.open(os.path.join(self.arc_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                        if src_file_format == 'vcf':
                                for line in arc_file_opened:
                                        if line.startswith('##'):
                                                continue
                                        else:
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        arc_file_opened.readline()
                                        
                        #При создании БД для каждого значения
                        #устанавливался оптимальный тип данных.
                        #В MongoDB важно соблюдать соответствие
                        #типа данных запрашиваемого слова
                        #и размещённых в базе значений.
                        #Для этого присвоим подходящий тип
                        #данных каждому аннотируемому элементу.
                        #Если аннотирование задумывается по
                        #большому числу коллекций, то
                        #осуществление запросов по
                        #каждому слову в сумме может
                        #занять неприлично много времени.
                        #Поэтому формируем из всех элементов
                        #список, планируя делать по нему
                        #высокопроизводительные запросы.
                        ann_list = []
                        for line in arc_file_opened:
                                ann_cell = line.rstrip().split('\t')[ann_col_index]
                                try:
                                        ann_list.append(int(ann_cell))
                                except ValueError:
                                        try:
                                                ann_list.append(Decimal128(ann_cell))
                                        except InvalidOperation:
                                                ann_list.append(ann_cell)
                                                
                #Среди аннотируемых файлов
                #могут затесаться пустые.
                #Проигнорируем их.
                if ann_list != []:
                        
                        #В имени коллекции предусмотрительно
                        #сохранено расширение того файла,
                        #по которому коллекция создавалась.
                        #Расширение поможет далее определить,
                        #вставлять ли в шапку элемент FORMAT,
                        #по какому столбцу парсить базу и
                        #как форматировать конечные строки.
                        trg_file_format = coll_names[0].split('.')[-1]
                        
                        #Исследователь может не выбрать
                        #поле, по которому производить поиск.
                        #Программа воспримет это таким
                        #образом, что исследователю нужно
                        #аннотировать SNPs по rsID-столбцу.
                        if self.ann_field_name == None:
                                if trg_file_format == 'vcf':
                                        ann_field_name = 'ID'
                                elif trg_file_format == 'bed':
                                        ann_field_name = 'name'
                                else:
                                        ann_field_name = 'ID'
                        else:
                                ann_field_name = self.ann_field_name
                                
                        #По плану в каждой подпапке
                        #должны размещаться результаты
                        #аннотирования одного файла.
                        #Но не факт, что подпапка далее
                        #пригодится, ведь в коллекции
                        #для аннотируемого набора может
                        #не найтись ни одного совпадения.
                        #Поэтому сейчас лишь получаем имя
                        #конечной подпапки и путь к ней.
                        trg_dir_name = '.'.join(arc_file_name.split('.')[:-2]) + '_ann'
                        trg_dir_path = os.path.join(self.trg_top_dir_path, trg_dir_name)
                        
                        #Аннотирование столбца каждой
                        #исходной таблицы производится
                        #по всем коллекциям MongoDB-базы.
                        #Т.е. даже, если по одной из коллекций
                        #уже получились результаты, обход
                        #будет продолжаться и завершится лишь
                        #после обращения к последней коллекции.
                        for coll_name in coll_names:
                                
                                #Создание объекта текущей коллекции.
                                #Проверка, принесёт ли запрос
                                #хоть какой-то результат.
                                #Если да, то этот запрос
                                #выполнится по-настоящему.
                                #В аннотируемый список
                                #затесались одинаковые элементы?
                                #Ничего страшного - MongoDB
                                #выдаст результат только
                                #по одному из них.
                                coll_obj = db_obj[coll_name]
                                if coll_obj.count_documents({ann_field_name: {'$in': ann_list}}) == 0:
                                        continue
                                curs_obj = coll_obj.find({ann_field_name: {'$in': ann_list}})
                                
                                #Чтобы шапка повторяла шапку
                                #той таблицы, по которой делалась
                                #коллекция, создадим её из имён полей.
                                #Сугубо техническое поле _id проигнорируется.
                                #Если в ex-VCF-коллекции есть поля с генотипами,
                                #то шапка дополнится элементом FORMAT.
                                header_row = list(coll_obj.find_one())[1:]
                                if trg_file_format == 'vcf' and len(header_row) > 8:
                                        header_row.insert(8, 'FORMAT')
                                header_line = '\t'.join(header_row)
                                
                                #Когда ясно, что результаты
                                #запроса имеются, можно смело
                                #создавать конечную подпапку.
                                if os.path.exists(trg_dir_path) == False:
                                        os.mkdir(trg_dir_path)
                                        
                                #Конструируем имя конечного файла
                                #и абсолютный путь к этому файлу.
                                trg_file_name = f'ann_by_{coll_name}'
                                trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                                
                                #Открытие конечного файла на запись.
                                with open(trg_file_path, 'w') as trg_file_opened:
                                        
                                        #Формируем и прописываем
                                        #метастроки, повествующие о
                                        #происхождении конечного файла.
                                        #Прописываем также табличную шапку.
                                        trg_file_opened.write(f'##Annotated: {arc_file_name}\n')
                                        trg_file_opened.write(f'##Database: {self.db_name}\n')
                                        trg_file_opened.write(f'##Collection: {coll_name}\n')
                                        trg_file_opened.write(f'##Field: {ann_field_name}\n')
                                        trg_file_opened.write(header_line + '\n')
                                        
                                        #Извлечение из объекта курсора
                                        #отвечающих запросу документов,
                                        #преобразование их значений в
                                        #обычные строки и прописывание
                                        #последних в конечный файл.
                                        for doc in curs_obj:
                                                trg_file_opened.write(restore_line(doc, trg_file_format, self.sec_delimiter))
                                                
                client.close()
                
####################################################################################################

import sys, os, gzip

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser
from multiprocessing import Pool
from pymongo import MongoClient
from bson.decimal128 import Decimal128
from decimal import InvalidOperation
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#аннотируемых файлов, определение
#оптимального числа процессов.
args = add_main_args()
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
arc_file_names = os.listdir(prep_single_proc.arc_dir_path)
arc_files_quan = len(arc_file_names)
if max_proc_quan > arc_files_quan <= 8:
        proc_quan = arc_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nАннотирование по БД {prep_single_proc.db_name}')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Параллельный запуск аннотирования.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.annotate, arc_file_names)
