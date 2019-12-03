__version__ = 'V1.1'

import os, sys, gzip, re
from pymongo import MongoClient, IndexModel, ASCENDING
from bson.decimal128 import Decimal128

def print_db_info(db_name, coll_names, indexed):
        print(f'''\nИмена всех коллекций базы {db_name}:\n''', coll_names)
        print(f'''\nПроиндексированные поля коллекций и
соответствующие типы данных базы {db_name}:\n''', indexed)
        
def remove_database(client, db_name):
        '''
        Функция, дающая возможность
        полного удаления базы данных.
        '''
        
        remove = input(f'''\nУдалить БД {db_name}?
(игнорирование ввода ==> не удалять)
[yes(|y)|no(|n|<enter>)]: ''')
        if remove in ['yes', 'y']:
                client.drop_database(db_name)
        elif remove in ['no', 'n', '']:
                pass
        else:
                print(f'{remove} - недопустимая опция')
                sys.exit()
        return remove

def create_database(client, db_names):
        '''
        Функция создания MongoDB-базы,
        наполнения её коллекций
        и индексации выбранных полей.
        '''
        
        arc_dir_path = os.path.normpath(input('''\nПуть к папке со сжатыми таблицами,
преобразуемыми в коллекции MongoDB: '''))
        
        #Создание базы, если таковой ещё нет.
        #Имя БД будет повторять имя папки,
        #где хранятся сжатые таблицы, которые
        #планируется размещать в эту базу.
        #Создание объекта клиента PyMongo.
        db_name = os.path.basename(arc_dir_path)
        db_obj = client[db_name]
        
        #Проверка наличия уже созданной базы.
        if db_name in db_names:
                
                print(f'\nБаза данных {db_name} уже существует')
                
                #Если база ранее была создана, то в
                #рамках диалога, находящегося в вызываемой
                #функции, будет предложено её удалить.
                #Если функция удаления вернёт решение
                #исследователя о сохранении предыдущей
                #версии базы, произойдёт досрочный
                #выход из функции создания БД.
                remove = remove_database(client, db_name)
                if remove in ['no', 'n', '']:
                        coll_names = sorted(db_obj.list_collection_names())
                        coll_names.remove('indexed')
                        indexed = db_obj.indexed.find_one()['act']
                        
                        print_db_info(db_name, coll_names, indexed)
                        
                        return db_name, coll_names, indexed
                
        detect_headers = input('''\nРаспознавать хэдеры VCF (##) и UCSC (track_name=)
сжатых таблиц автоматически, или потом вы
укажете количество хэдеров самостоятельно?
(Предпросмотрщик больших файлов есть в репозитории
https://github.com/PlatonB/bioinformatic-python-scripts)
[auto(|a)|manual(|m)]: ''')
        if detect_headers in ['auto', 'a']:
                num_of_unind = None
                
        elif detect_headers in ['manual', 'm']:
                num_of_unind = input('''\nКоличество не обрабатываемых строк
в начале каждой сжатой таблицы
(Важно! Табулированную шапку к ним не причисляйте)
(игнорирование ввода ==> производить работу для всех строк)
[0(|<enter>)|1|2|...]: ''')
                if num_of_unind == '':
                        num_of_unind = 0
                else:
                        num_of_unind = int(num_of_unind)
        else:
                print(f'{detect_headers} - недопустимая опция')
                sys.exit()
                
        cont, col_info = 'y', {}
        while cont not in ['no', 'n', '']:
                col_name = input('''\nИмя столбца, данные которого
потребуется быстро извлекать
(Нужно соблюдать регистр)
(убедитесь, что этот столбец
присутствуют во всех сжатых таблицах)
[#Chrom|pos|RSID|...]: ''')
                
                data_type = input('''\nВ выбранном столбце - целые числа, вещественные числа или строки?
(пример целого числа: 105731746)
(примеры вещественного числа: 0.05, 2.5e-12)
(примеры строки: X, Y, A/C/G, rs4271507, HLA-DQB1)
[integer(|i)|decimal(|d)|string(|s)]: ''')
                if data_type == 'i':
                        data_type = 'int'
                elif data_type == 'd':
                        data_type = 'decimal'
                elif data_type == 's':
                        data_type = 'string'
                elif data_type not in ['integer', 'decimal', 'string']:
                        print(f'{data_type} - недопустимая опция')
                        sys.exit()
                col_info[col_name] = [data_type]
                
                cont = input('''\nПроиндексировать по ещё одному столбцу?
(игнорирование ввода ==> нет)
[yes(|y)|no(|n|<enter>)]: ''')
                if cont not in ['yes', 'y', 'no', 'n', '']:
                        print('{cont} - недопустимая опция')
                        sys.exit()
                        
        #Список для накопления имён
        #создаваемых MongoDB-коллекций.
        coll_names = []
        
        #Работа с исходными архивами, каждый
        #из которых содержит по одной таблице.
        arc_file_names = os.listdir(arc_dir_path)
        for arc_file_name in arc_file_names:

                #Игнорирование бэкапных файлов,
                #оставляемых LibreOffice Calc.
                if arc_file_name.startswith('.~lock.'):
                        continue
                
                #Для поддержания узнаваемости,
                #имена коллекций будут совпадать
                #с именами исходных файлов.
                coll_names.append(arc_file_name)
                
                #Открытие исходной сжатой таблицы на чтение.
                with gzip.open(os.path.join(arc_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                        
                        #Автоматическое определение и прочтение
                        #вхолостую хэдеров таблиц распространённых
                        #биоинформатических форматов VCF и UCSC BED.
                        #Последний из прочитанных хэдеров (он
                        #же - шапка таблицы) будет сохранён.
                        if num_of_unind == None:
                                for line in arc_file_opened:
                                        if re.match(r'##|track_name=', line) == None:
                                                header_row = line.rstrip('\n').split('\t')
                                                break
                                        
                        #Холостое прочтение хэдеров, количество которых
                        #указано пользователем, и сохранение шапки.
                        else:
                                for unind_index in range(num_of_unind):
                                        arc_file_opened.readline()
                                header_row = arc_file_opened.readline().rstrip('\n').split('\t')
                                
                        #На этапе пользовательского
                        #диалога был создан словарь.
                        #Его ключи - указанные исследователем
                        #имена индексируемых столбцов
                        #исходных таблиц (они же -
                        #имена полей создаваемой БД).
                        #Значения - списки, содержащие
                        #алиас предполагаемого BSON-типа
                        #данных каждого будущего поля.
                        #Добавляем в каждый из этих
                        #списков питоновский индекс имени
                        #каждого индексируемого столбца,
                        #обозначающий его позицию в шапке и
                        #определяющий положение его ячеек
                        #в любой строке исходной таблицы.
                        for col_name in col_info.keys():
                                col_info[col_name].append(header_row.index(col_name))
                                
                        #Задаём максимальный размер заливаемых
                        #в БД фрагментов исходных таблиц.
                        #В стандартных случаях он
                        #будет равен 100000 строк.
                        #Но если таблица очень широка,
                        #то во избежание переполнения
                        #RAM ограничим фрагменты до 10000.
                        #Ширина таблицы определяется по
                        #количеству элементов её хэдера.
                        if len(header_row) < 50:
                                max_fragment_len = 100000
                        else:
                                max_fragment_len = 10000
                                
                        #Создание коллекции.
                        #Для оптимального соотношения
                        #скорости записи/извлечения
                        #с объёмом хранимых данных,
                        #я выбрал в качестве
                        #алгоритма сжатия Zstandard.
                        coll_obj = db_obj.create_collection(arc_file_name,
                                                            storageEngine={'wiredTiger':
                                                                           {'configString':
                                                                            'block_compressor=zstd'}})
                        
                        print(f'\nКоллекция {arc_file_name} БД {db_name} пополняется')
                        
                        #Данные будут поступать в базу
                        #одним или более фрагментами.
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
                                row = line.rstrip('\n').split('\t')
                                
                                #MongoDB позволяет привязывать к
                                #одному полю данные разных типов.
                                #Но во избежание ошибок при
                                #осуществлении запросов, программа
                                #будет соблюдать единообразие типа
                                #данных в пределах каждого поля.
                                #Применяется это правило, в
                                #частности, при пополнении коллекций.
                                #Ячейкам каждого столбца, по которым
                                #впоследствии планируется быстрый
                                #поиск, программа присваивает заданный
                                #пользователем тип данных, а для
                                #остальных столбцов оставляет str.
                                for col_ann in col_info.values():
                                        if col_ann[0] == 'int':
                                                row[col_ann[1]] = int(row[col_ann[1]])
                                        elif col_ann[0] == 'decimal':
                                                row[col_ann[1]] = Decimal128(row[col_ann[1]])
                                                
                                #MongoDB - документоориентированная СУБД.
                                #Каджая коллекция строится из т.н. документов,
                                #Python-предшественниками которых являются словари.
                                #Поэтому для подготовки размещаемого в базу фрагмента
                                #создаём из элементов хэдера и очередной строки
                                #словарь, добавляем его в список таких словарей.
                                fragment.append(dict(zip(header_row, row)))
                                
                                #Сразу после после пополнения фрагмента
                                #регистрируем это событие с помощью счётчика.
                                fragment_len += 1
                                
                                #Исходная таблица ещё не до
                                #конца считалась, а фрагмент достиг
                                #порогового значения количества строк.
                                #Тогда прописываем фрагмент в коллекцию,
                                #очищаем его и обнуляем счётчик.
                                if fragment_len == max_fragment_len:
                                        coll_obj.insert_many(fragment)
                                        fragment.clear()
                                        fragment_len = 0
                                        
                        #Чтение исходной таблицы
                        #завершилось, но остался
                        #непрописанный фрагмент.
                        #Исправляем ситуацию.
                        if fragment_len > 0:
                                coll_obj.insert_many(fragment)
                                
                print(f'Коллекция {arc_file_name} БД {db_name} индексируется')
                
                #Генерируем список объектов, обеспечивающих
                #создание одиночных индексов заданных исследователем
                #полей, и производим индексацию этих полей.
                index_objs = [IndexModel([(col_name, ASCENDING)]) for col_name in col_info.keys()]
                coll_obj.create_indexes(index_objs)
                
        #Получаем упрощённую версию
        #словаря с именами индексируемых
        #столбцов: теперь без позиций
        #этих столбцов в исходных таблицах.
        #Последние перестали быть нужными,
        #поскольку дальнейшей работы со
        #сжатыми таблицами не предвидется -
        #всё будет сосредоточено на БД.
        #Прописываем новый словарь
        #дважды в отдельную коллекцию.
        #Один вариант будет хранить в
        #неизменном виде имена и типы данных
        #проиндексированных этой функцией полей.
        #В другом функция переиндексации будет
        #фиксировать внесённые собой изменения.
        #Типы данных я предпочёл хранить в
        #коллекции, потому что определять тип
        #по самим данным будет потом сложнее,
        #чем извлечь заготовленную информацию.
        indexed = {col_name: col_ann[0] for col_name, col_ann in col_info.items()}
        db_obj.indexed.insert_one({'ref': indexed, 'act': indexed})
        
        #Сортируем список коллекций
        #для повышения удобочитаемости
        #соответствующих принтов.
        coll_names.sort()
        
        print_db_info(db_name, coll_names, indexed)
        
        return db_name, coll_names, indexed

def reindex_collections(client, db_name):
        '''
        Функция удаления старых и создания
        новых индексов MongoDB-коллекций.
        '''
        
        #Создание объекта клиента PyMongo.
        #Получение списка всех коллекций,
        #кроме той, в которой содержатся
        #имена и типы данных ранее
        #проиндексированных полей, а
        #также списка имён индексов.
        db_obj = client[db_name]
        coll_names = sorted(db_obj.list_collection_names())
        coll_names.remove('indexed')
        index_names = list(db_obj[coll_names[0]].index_information().keys())[1:]
        
        #Далее пригодятся оба документа,
        #описывающие проиндексированные поля:
        #и который был создан при первом и
        #единственном запуске функции построения
        #базы (далее - референсный), и который
        #мог быть изменён в рамках прошлых запусков
        #текущей функции (далее - актуальный).
        indexed_ref = db_obj.indexed.find_one()['ref']
        indexed_act = db_obj.indexed.find_one()['act']
        
        print(f'\nИндексы базы {db_name}:\n', index_names)
        
        indices_to_delete = input(f'''\nКакие индексы удалить?
(через запятую с пробелом)
(игнорирование ввода ==> не удалять индексы)
[...|<enter>]: ''').split(', ')
        if indices_to_delete != ['']:
                
                #Удаление выбранных исследователем индексов,
                #а также информации о соответствующих
                #им полях из актуального словаря.
                for coll_name in coll_names:
                        for index_name in indices_to_delete:
                                db_obj[coll_name].drop_index(index_name)
                for index_name in indices_to_delete:
                        del indexed_act[index_name[:-2]]
                        
        fields_to_index = input(f'''\nКакие поля проиндексировать?
(через запятую с пробелом)
(игнорирование ввода ==> не индексировать поля)
[...|<enter>]: ''').split(', ')
        if fields_to_index != ['']:
                index_objs = []
                for field_name in fields_to_index:
                        index_objs.append(IndexModel([(field_name, ASCENDING)]))
                        
                        #Исследователь мог по той или
                        #иной причине удалить индекс и
                        #потом решить его воссоздать.
                        #Эта функция не меняет BSON-типы
                        #данных, т.к. такое пока не
                        #реализовано на уровне MongoDB.
                        #Поэтому задача - раздобыть
                        #ранее присвоенный тип.
                        #Типы полей, направляемых на
                        #индексацию функцией создания БД,
                        #наличествуют в референсном словаре.
                        #Он с гарантией от разработчика не
                        #меняется ни бэкендом, ни фронтендами,
                        #в то время как из актуального
                        #словаря пары "имя поля"-"тип данных"
                        #пропадают при удалении индексов.
                        #Следовательно, попытаемся достать
                        #тип данных свежеиндексированного
                        #поля именно из референсного словаря.
                        #Не индексированные функцией
                        #создания БД поля имели и будут
                        #иметь тип string, что программа
                        #запечатлеет в актуальном словаре.
                        if field_name in indexed_ref:
                                indexed_act[field_name] = indexed_ref[field_name]
                        else:
                                indexed_act[field_name] = 'string'
                                
                #Доиндексация.
                for coll_name in coll_names:
                        db_obj[coll_name].create_indexes(index_objs)
                        
        #Функция могла при желании исследователя
        #какие-то индексы удалить, а какие-то создать.
        #Сохраняем актуальную информацию о проиндексированных
        #полях в соответствующее поле специальной коллекции.
        db_obj.indexed.update_one({}, {'$set': {'act': indexed_act}})
        
        print_db_info(db_name, coll_names, indexed_act)
        
        return db_name, coll_names, indexed_act
