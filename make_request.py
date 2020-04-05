__version__ = 'V1.2'

print('''
Программа, позволяющая выполнить
запрос по всем коллекциям MongoDB-базы.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V1.2.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues
Справка по CLI: python3 make_request.py -h

Перед запуском программы нужно установить
MongoDB и PyMongo (см. README).

Источником отбираемых данных должна быть
база, созданная с помощью create_db.

Желательно, чтобы поля БД, по которым
надо искать, были проиндексированы.

Поддерживается только Python-диалект
языка запросов MongoDB (см. вкладку Python):
https://docs.mongodb.com/manual/tutorial/query-documents/

Допустимые операторы:
https://docs.mongodb.com/manual/reference/operator/query/
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
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой искать')
        argparser.add_argument('-T', '--trg-dir-path', metavar='str', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов')
        argparser.add_argument('-q', '--pymongo-query', metavar="['{}']", default='{}', dest='pymongo_query', type=str,
                               help='Запрос ко всем коллекциям БД (в одинарных кавычках; синтаксис PyMongo; указание типов данных: "str", Decimal128("str"))')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', default='comma', choices=['comma', 'semicolon', 'colon', 'pipe'], dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (trg-VCF, trg-BED: опция не применяется)')
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
                
                #Убеждаемся в том, что в коллекции
                #хоть что-нибудь найдётся.
                #Тогда реально выполним для
                #этой коллекции составленный
                #исследователем запрос.
                if coll_obj.count_documents(self.pymongo_query) > 0:
                        curs_obj = coll_obj.find(self.pymongo_query)
                        
                        #В имени коллекции предусмотрительно
                        #сохранено расширение того файла,
                        #по которому коллекция создавалась.
                        #Расширение поможет далее принять
                        #решение о необходимости коррекции
                        #шапки, а также определить, как
                        #форматировать конечные строки.
                        trg_file_format = coll_name.split('.')[-1]
                        
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
                        
                        #Конструируем имя конечного файла
                        #и абсолютный путь к этому файлу.
                        trg_file_name = f'found_in_{coll_name}'
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем
                                #метастроки, повествующие о
                                #происхождении конечного файла.
                                #Прописываем также табличную шапку.
                                trg_file_opened.write(f'##Database: {self.db_name}\n')
                                trg_file_opened.write(f'##Collection: {coll_name}\n')
                                trg_file_opened.write(f'##Query: {self.pymongo_query}\n')
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

import sys, os

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser
from pymongo import MongoClient
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
args, client = add_main_args(), MongoClient()
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
