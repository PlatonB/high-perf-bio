__version__ = 'v3.7'

def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, позволяющая выполнить
запрос по всем коллекциям MongoDB-базы.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Источником отбираемых данных должна быть база, созданная с помощью create_db.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Поддерживается только Python-диалект языка запросов MongoDB (см. вкладку Python):
https://docs.mongodb.com/manual/tutorial/query-documents/

Допустимые MongoDB-операторы:
https://docs.mongodb.com/manual/reference/operator/query/

Условные обозначения в справке по CLI:
[значение по умолчанию];
{{допустимые значения}};
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
        man_grp.add_argument('-D', '--db-name', required=True, metavar='str', dest='db_name', type=str,
                             help='Имя БД, по которой искать')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-q', '--mongo-query', metavar="['{}']", default='{}', dest='mongo_query', type=str,
                             help='Запрос ко всем коллекциям БД (в одинарных кавычках; синтаксис PyMongo; примеры указания типа данных: "any_str", Decimal128("any_str"))')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; db-VCF: не применяется; db-BED: trg-TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                             help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (db-VCF, db-BED (trg-BED): не применяется)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно парсимых коллекций')
        args = arg_parser.parse_args()
        return args

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
                должны будут потом в параллельных процессах изменяться. Получаются
                они в основном из указанных исследователем опций. Некоторые неочевидные,
                но важные детали об атрибутах. Квази-расширение коллекций. Оно нужно,
                как минимум, для определения правил сортировки и форматирования
                конечных файлов. Проджекшен (отбор полей). Для db-VCF его крайне
                трудно реализовать из-за наличия в соответствующих коллекциях
                разнообразных вложенных структур и запрета со стороны MongoDB на
                применение точечной формы обращения к отбираемым элементам массивов.
                Что касается db-BED, когда мы оставляем только часть полей, невозможно
                гарантировать соблюдение спецификаций BED-формата, поэтому вывод будет
                формироваться не более, чем просто табулированным (trg-TSV). Сортировка
                BED и VCF делается по координатам для поддержки tabix-индексации.
                '''
                client = MongoClient()
                self.db_name = args.db_name
                self.coll_names = client[self.db_name].list_collection_names()
                coll_name_ext = self.coll_names[0].rsplit('.', maxsplit=1)[1]
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.mongo_aggregate_arg = [{'$match': eval(args.mongo_query)}]
                if args.proj_fields is None or coll_name_ext == 'vcf':
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = coll_name_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_aggregate_arg.append({'$project': mongo_project})
                        self.mongo_findone_args = [None, mongo_project]
                        self.trg_file_fmt = 'tsv'
                if self.trg_file_fmt == 'vcf':
                        self.mongo_aggregate_arg.append({'$sort': SON([('#CHROM', ASCENDING),
                                                                       ('POS', ASCENDING)])})
                elif self.trg_file_fmt == 'bed':
                        self.mongo_aggregate_arg.append({'$sort': SON([('chrom', ASCENDING),
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
                
        def search(self, coll_name):
                '''
                Функция поиска по одной коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                db_obj = client[self.db_name]
                coll_obj = db_obj[coll_name]
                
                #Создание объекта курсора.
                curs_obj = coll_obj.aggregate(self.mongo_aggregate_arg)
                
                #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                #коллекция, создадим её из имён полей. Projection при этом учтём.
                #Имя сугубо технического поля _id проигнорируется. Если в db-VCF
                #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                header_row = list(coll_obj.find_one(*self.mongo_findone_args))[1:]
                if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                        header_row.insert(8, 'FORMAT')
                header_line = '\t'.join(header_row)
                
                #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                coll_name_base = coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'{coll_name_base}_query_res.{self.trg_file_fmt}'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                
                #Открытие конечного файла на запись.
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Формируем и прописываем метастроки,
                        #повествующие о происхождении конечного
                        #файла. Прописываем также табличную шапку.
                        if self.trg_file_fmt == 'vcf':
                                trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                        trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                        trg_file_opened.write(f'##database={self.db_name}\n')
                        trg_file_opened.write(f'##collection={coll_name}\n')
                        trg_file_opened.write(f'##query={self.mongo_aggregate_arg[0]["$match"]}\n')
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
                                
                #Удаление конечного файла,если в
                #нём очутились только метастроки.
                if empty_res:
                        os.remove(trg_file_path)
                        
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from bson.decimal128 import Decimal128
from multiprocessing import Pool
from bson.son import SON
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#парсимых коллекций, определение
#оптимального числа процессов.
args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args,
                                  __version__)
coll_names = prep_single_proc.coll_names
colls_quan = len(coll_names)
if max_proc_quan > colls_quan <= 8:
        proc_quan = colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nQueriing by {prep_single_proc.db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск поиска. Замер времени
#выполнения вычислений с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.search, coll_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
