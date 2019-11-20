__version__ = 'V1.0'

print('''
Модуль, позволяющий формировать и
выполнять простые запросы к MongoDB-базе.

Автор: Платон Быкадоров (platon.work@gmail.com), 2019.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md

Обязательно!
Перед запуском программы нужно установить
MongoDB и PyMongo (см. README).

Каждая таблица, размещаемая в БД, должна иметь шапку.

Если настройки, запрашиваемые в рамках интерактивного
диалога, вам непонятны - пишите, пожалуйста, в Issues.
''')

def change_data_type(cond, data_type):
        '''
        Функция смены типа
        данных аннотируемых ячеек.
        Он в результате будет
        соответствовать типу
        данных поля, по которому
        планируется аннотировать.
        '''
        
        if data_type == 'int':
                try:
                        return int(cond)
                except TypeError:
                        return [int(key) for key in cond]
        elif data_type == 'decimal':
                try:
                        return Decimal128(cond)
                except ValueError:
                        return [Decimal128(key) for key in cond]
                
####################################################################################################

print('\nИмпорт модулей программы...')

import sys

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from backend.db_manager import create_database, reindex_collections
from pymongo import MongoClient
from bson.decimal128 import Decimal128
import os

#Создание объекта клиента PyMongo.
#Вывод имён всех MongoDB-баз,
#имеющихся на данном компьютере.
#Имена пригодятся для определения,
#существует ли база или ещё нет.
client = MongoClient()
db_names = client.list_database_names()

db_choice = input(f'''\nВыберите базу данных или создайте новую
(игнорирование ввода ==> создать новую)
[{"|".join(db_names)}|<enter>]: ''')
if db_choice == '':
        db_name, coll_names, indexed = create_database(client, db_names)
elif db_choice not in db_names:
        print(f'{db_choice} - недопустимая опция')
        sys.exit()
else:
        db_name, coll_names, indexed = reindex_collections(client, db_choice)
        
trg_dir_path = input('\nПуть к папке для результатов: ')

print(f'''\nПроиндексированные поля коллекций и
соответствующие типы данных базы {db_name}:\n''', indexed)

str_operators, num_operators = {'!=': '$nin',
                                '=': '$in',
                                '==': '$in'}, {'>': '$gt',
                                               '<': '$lt',
                                               '>=': '$gte',
                                               '<=': '$lte',
                                               '!=': '$nin',
                                               '=': '$in',
                                               '==': '$in'}

cont, query = 'y', {}
while cont not in ['no', 'n', '']:
        
        if len(indexed) > 1:
                field_name = input(f'''\nИмя поля, в котором ищем
[{"|".join(indexed.keys())}]: ''')
                if field_name not in indexed:
                        print(f'{field_name} - недопустимая опция')
                        sys.exit()
                        
        else:
                field_name = list(indexed.keys())[0]
                
        if indexed[field_name] == 'string':
                operator = input('''\nОператор сравнения
(игнорирование ввода ==> поиск будет по самим словам)
[!=|=(|==|<enter>)]: ''')
                if operator == '':
                        operator = '='
                        
                cond = input(f'''\nИскомые или исключаемые слова
(через запятую с пробелом):
{operator} ''').split(', ')
                
                query[field_name] = {str_operators[operator]: cond}
                
        else:
                operator = input('''\nОператор сравнения
(игнорирование ввода ==> поиск будет по самим числам)
[>|<|>=|<=|!=|=(|==|<enter>)]: ''')
                if operator == '':
                        operator = '='
                        
                if operator in ['!=', '=', '==']:
                        cond = change_data_type(input(f'''\nИскомые или исключаемые числа
(через запятую с пробелом):
{operator} ''').split(', '), indexed[field_name])
                        
                elif operator in ['>', '<', '>=', '<=']:
                        cond = change_data_type(input(f'''\nПороговое значение:
{operator} '''), indexed[field_name])
                        
                query[field_name] = {num_operators[operator]: cond}
                
        if len(indexed) > 1:
                cont = input('''\nИскать ещё в одном столбце?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print(f'{cont} - недопустимая опция')
                        sys.exit()
                        
        else:
                break
        
#Создание объекта базы данных.
db_obj = client[db_name]

#Поиск производится по всем коллекциям.
#Т.е. даже, если в одной из них уже
#обнаружились соответствия запросу, обход
#будет продолжаться и завершится лишь
#после обращения к последней коллекции.
for coll_name in coll_names:
        
        #Создание объекта коллекции.
        coll_obj = db_obj[coll_name]
        
        print(f'\nПоиск по коллекции {coll_name} базы {db_name}')
        
        #Поиск по ранее сформированному запросу.
        curs_obj = coll_obj.find(query)
        
        #Если хоть что-то нашлось,
        #объект курсора окажется не пустым.
        #В таком случае появляется смысл
        #извлекать из него результаты.
        if curs_obj.count() > 0:
                
                #Конструируем имя конечного файла
                #и абсолютный путь к этому файлу.
                trg_file_name = f'found_in_{".".join(coll_name.split(".")[:-1])}'
                trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                
                #Из имён полей возрождаем шапку
                #таблицы, по данным которой ранее
                #была создана текущая коллекция.
                header_line = '\t'.join(list(db_obj[coll_name].find_one())[1:])
                
                #Открытие конечного файла на запись.
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Первым хэдером конечной таблицы
                        #будет оформленный в соответствии
                        #с синтаксисом MongoDB запрос,
                        #вторым - шапка, по-сути,
                        #заимствованная из таблицы, по
                        #которой создавалась коллекция.
                        trg_file_opened.write(f'##{query}\n')
                        trg_file_opened.write(header_line + '\n')
                        
                        print(f'Извлечение отобранных документов')
                        
                        #Извлечение из объекта курсора
                        #отвечающих запросу документов,
                        #преобразование из значений в
                        #обычные строки и прописывание
                        #последних в конечный файл.
                        for doc in curs_obj:
                                row = [str(val) for val in list(doc.values())[1:]]
                                trg_file_opened.write('\t'.join(row) + '\n')
                                
        else:
                print('Ничего не найдено')
                
client.close()
