__version__ = 'V3.0'

def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, позволяющая выполнить
запрос по всем коллекциям MongoDB-базы.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, pymongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Источником отбираемых данных должна быть
база, созданная с помощью create_db.

Желательно, чтобы поля БД, по которым
надо искать, были проиндексированы.

Поддерживается только Python-диалект
языка запросов MongoDB (см. вкладку Python):
https://docs.mongodb.com/manual/tutorial/query-documents/

Допустимые операторы:
https://docs.mongodb.com/manual/reference/operator/query/

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой искать')
        argparser.add_argument('-T', '--trg-dir-path', metavar='str', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов')
        argparser.add_argument('-q', '--pymongo-query', metavar="['{}']", default='{}', dest='pymongo_query', type=str,
                               help='Запрос ко всем коллекциям БД (в одинарных кавычках; синтаксис PyMongo; примеры указания типа данных: "any_str", Decimal128("any_str"))')
        argparser.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                               help='Отбираемые поля (через запятую без пробела; DB-VCF: опция не применяется; DB-BED: получится TSV; поле _id не выведется')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (DB-VCF, DB-BED (без -k): опция не применяется)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно парсимых коллекций')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасный параллельный поиск
        по набору коллекций MongoDB.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции отбора документов.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.db_name = args.db_name
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.pymongo_query = eval(args.pymongo_query)
                if args.proj_fields is None:
                        self.pymongo_project = args.proj_fields
                else:
                        self.pymongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        if '_id' in self.pymongo_project:
                                del self.pymongo_project['_id']
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                        
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
                
                #Убеждаемся в том, что в коллекции хоть что-нибудь
                #найдётся. Тогда реально выполним для этой
                #коллекции переданный через команду запрос.
                if coll_obj.count_documents(self.pymongo_query) > 0:
                        pymongo_find_args = [self.pymongo_query]
                        
                        #В имени коллекции предусмотрительно сохранено расширение того
                        #файла, по которому коллекция создавалась. Расширение поможет далее
                        #разобраться, как минимум, с сортировкой и форматированием результатов.
                        coll_name_base, trg_file_format = coll_name.rsplit('.', maxsplit=1)
                        
                        #Project в MongoDB - это условие, отбирающее данные лишь
                        #определённых полей коллекции. Pymongo-методы find и find_one
                        #принимают его отдельным от запроса аргументом. Когда мы
                        #оставляем только часть полей, невозможно гарантировать
                        #соблюдение спецификаций VCF- и BED-форматов, поэтому вывод
                        #будет формироваться не более, чем просто табулированным.
                        if self.pymongo_project is not None and trg_file_format != 'vcf':
                                pymongo_find_args.append(self.pymongo_project)
                                trg_file_format = 'tsv'
                                
                        #Создание объекта курсора.
                        curs_obj = coll_obj.find(*pymongo_find_args)
                        
                        #Таблицы биоинформатических форматов
                        #нужно сортировать по хромосомам и позициям.
                        #Задаём правило сортировки будущего VCF
                        #или BED на уровне MongoDB-курсора.
                        if trg_file_format == 'vcf':
                                curs_obj.sort([('#CHROM', ASCENDING),
                                               ('POS', ASCENDING)])
                        elif trg_file_format == 'bed':
                                curs_obj.sort([('chrom', ASCENDING),
                                               ('start', ASCENDING),
                                               ('end', ASCENDING)])
                                
                        #Чтобы шапка повторяла шапку той таблицы, по которой
                        #делалась коллекция, создадим её из имён полей. Имя сугубо
                        #технического поля _id проигнорируется. Если в ex-VCF-коллекции
                        #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                        header_row = list(coll_obj.find_one(*pymongo_find_args))[1:]
                        if trg_file_format == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                        trg_file_name = f'{coll_name_base}_query_res.{trg_file_format}'
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                trg_file_opened.write(f'##Database: {self.db_name}\n')
                                trg_file_opened.write(f'##Collection: {coll_name}\n')
                                trg_file_opened.write(f'##Query: {self.pymongo_query}\n')
                                if trg_file_format != 'vcf':
                                        trg_file_opened.write(f'##Project: {self.pymongo_project}\n')
                                trg_file_opened.write(header_line + '\n')
                                
                                #Извлечение из объекта курсора отвечающих запросу документов, преобразование
                                #их значений в обычные строки и прописывание последних в конечный файл.
                                for doc in curs_obj:
                                        trg_file_opened.write(restore_line(doc, trg_file_format, self.sec_delimiter))
                                        
                client.close()
                
####################################################################################################

import sys, os

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from bson.decimal128 import Decimal128
from multiprocessing import Pool
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#парсимых коллекций, определение
#оптимального числа процессов.
args, client = add_args(__version__), MongoClient()
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
db_name = prep_single_proc.db_name
coll_names = client[db_name].list_collection_names()
colls_quan = len(coll_names)
if max_proc_quan > colls_quan <= 8:
        proc_quan = colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nПоиск по БД {db_name}')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Параллельный запуск поиска.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.search, coll_names)
