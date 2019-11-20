__version__ = 'V1.0'

print('''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

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
        
ann_dir_path = os.path.normpath(input('\nПуть к папке с аннотируемыми таблицами: '))

trg_top_dir_path = input('\nПуть к папке для результатов: ')

print(f'''\nПроиндексированные поля коллекций и
соответствующие типы данных базы {db_name}:\n''', indexed)

if len(indexed) > 1:
        field_name = input(f'''\nИмя поля, по которому аннотируем
[{"|".join(indexed.keys())}]: ''')
        if field_name not in indexed:
                print(f'{field_name} - недопустимая опция')
                sys.exit()
else:
        field_name = list(indexed.keys())[0]
if indexed[field_name] == 'decimal':
        print('\nЧисла с точкой, имхо, бессмысленно аннотировать')
        sys.exit()
        
num_of_headers = input('''\nКоличество не обрабатываемых строк
в начале каждой аннотируемой таблицы
(игнорирование ввода ==> производить работу для всех строк)
[0(|<enter>)|1|2|...]: ''')
if num_of_headers == '':
        num_of_headers = 0
else:
        num_of_headers = int(num_of_headers)
        
ann_col_index = int(input('\nНомер аннотируемого столбца: ')) - 1

verbose = input('''\nВыводить подробную информацию о ходе работы?
(не рекомендуется, если набор
исходных данных очень большой)
(игнорирование ввода ==> не выводить)
[yes(|y)|no(|n|<enter>)]: ''')
if verbose not in ['yes', 'y', 'no', 'n', '']:
        print(f'{verbose} - недопустимая опция')
        sys.exit()
        
#Создание объекта базы данных.
db_obj = client[db_name]

#Получение списка имён аннотируемых
#таблиц и перебор его элементов.
#Игнорирование имён скрытых бэкап-файлов,
#автоматически генерируемых LibreOffice.
ann_file_names = os.listdir(ann_dir_path)
for ann_file_name in ann_file_names:
        if ann_file_name.startswith('.~lock.'):
                continue
        
        #Открытие аннотируемого файла на чтение.
        with open(os.path.join(ann_dir_path, ann_file_name)) as ann_file_opened:
                
                #Скипаем хэдеры аннотируемой таблицы.
                for header_index in range(num_of_headers):
                        ann_file_opened.readline()
                        
                print(f'\nОчистка столбца таблицы {ann_file_name}')
                
                #Аннотируемый столбец желательно
                #очистить от повторяющихся элементов.
                #Для этого создадим из него множество.
                #Сделаем так, чтобы тип данных
                #аннотируемых ячеек соответствовал
                #типу, распространяющемуся на все
                #значения поля, по которому аннотируем.
                if indexed[field_name] == 'string':
                        annotated = list(set(line.rstrip('\n').split('\t')[ann_col_index] for line in ann_file_opened))
                elif indexed[field_name] == 'int':
                        annotated = list(set(int(line.rstrip('\n').split('\t')[ann_col_index]) for line in ann_file_opened))
                        
        #Среди аннотируемых файлов
        #могут затесаться пустые.
        #Проигнорируем их.
        if annotated == []:
                print(f'\tЭтот файл пуст')
                continue
        
        #После того, как выяснилось, что аннотируемый
        #набор не пустой, строим путь к конечной подпапке.
        #Каждая подпапка предназначается для размещения
        #в ней результатов аннотирования одного файла.
        #Несмотря на то, что данные, подлежащие аннотации,
        #есть, не факт, что подпапка далее пригодится, ведь
        #в коллекции может не найтись соответствующих значений.
        #Поэтому создание конечной подпапки отложим на потом.
        trg_dir_name = '.'.join(ann_file_name.split('.')[:-1]) + '_ann'
        trg_dir_path = os.path.join(trg_top_dir_path, trg_dir_name)
        
        print(f'Аннотирование столбца таблицы {ann_file_name} по базе {db_name}')
        
        #Аннотирование каждого исходного файла
        #производится по всем коллекциям.
        #Т.е. даже, если по одной из них
        #уже получились результаты, обход
        #будет продолжаться и завершится лишь
        #после обращения к последней коллекции.
        for coll_name in coll_names:
                
                #Создание объекта коллекции.
                coll_obj = db_obj[coll_name]
                
                if verbose in ['yes', 'y']:
                        print(f'\n\tПоиск по коллекции {coll_name}')
                        
                #Собственно, аннотирование.
                curs_obj = coll_obj.find({field_name: {'$in': annotated}})
                
                #Если хоть для одна ячейка
                #проаннотировалась, объект
                #курсора окажется не пустым.
                #В таком случае появляется смысл
                #извлекать из него результаты.
                if curs_obj.count() > 0:
                        
                        #И уже когда ясно, что результаты имеются,
                        #можно смело создавать конечную подпапку.
                        if os.path.exists(trg_dir_path) == False:
                                os.mkdir(trg_dir_path)
                                
                        #Конструируем имя конечного файла
                        #и абсолютный путь к этому файлу.
                        trg_file_name = f'ann_by_{".".join(coll_name.split(".")[:-1])}'
                        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                        
                        #Чтобы шапка повторяла шапку
                        #той таблицы, по которой делалась
                        #коллекция, создадим её из имён полей.
                        header_line = '\t'.join(list(db_obj[coll_name].find_one())[1:])
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем хэдер, повествующий
                                #о происхождении конечного файла.
                                #Прописываем также табличную шапку.
                                trg_file_opened.write(f'##{ann_file_name} annotated by {coll_name}\n')
                                trg_file_opened.write(header_line + '\n')
                                
                                if verbose in ['yes', 'y']:
                                        print(f'\tИзвлечение отобранных документов')
                                        
                                #Извлечение из объекта курсора
                                #отвечающих запросу документов,
                                #преобразование из значений в
                                #обычные строки и прописывание
                                #последних в конечный файл.
                                for doc in curs_obj:
                                        row = [str(val) for val in list(doc.values())[1:]]
                                        trg_file_opened.write('\t'.join(row) + '\n')
                                        
                else:
                        if verbose in ['yes', 'y']:
                                print('\tНичего не найдено')
                                
client.close()
