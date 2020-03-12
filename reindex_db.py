__version__ = 'V1.0'

print('''
Программа, производящая индексацию
MongoDB-базы в несколько процессов
и удаляющая старые индексы.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues
Справка по CLI: python3 reindex_db.py -h

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).
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
                               help='Имя переиндексируемой БД')
        argparser.add_argument('-i', '--print-db-info', dest='print_db_info', action='store_true',
                               help='Вывести минимальный набор имён индексов, проиндексированных полей и всех полей')
        argparser.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                               help='Имена удаляемых индексов (через запятую без пробела)')
        argparser.add_argument('-a', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                               help='Имена индексируемых полей (через запятую без пробела)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно индексируемых коллекций')
        args = argparser.parse_args()
        
        return args

def print_db_info(db_obj, thin_coll_name):
        '''
        Чтобы принять решение
        о необходимости
        переиндексации, важно
        иметь перед глазами
        информацию о БД: имена
        уже существующих индексов,
        соответствующих этим
        индексам полей и
        вообще всех полей.
        MongoDB позволяет
        держать коллекции с
        разным количеством полей.
        Поэтому надо определиться,
        по какой из коллекций
        давать исследователю
        представление о базе.
        Правильно будет взять
        коллекцию с минимальным
        во всей БД числом полей.
        Таким образом можно
        избежать создания индексов
        существующих не во всех
        коллекциях полей и обречённых
        на OperationFailure попыток
        удаления отсутствующих в той
        или иной коллекции индексов.
        '''
        thin_coll_obj = db_obj[thin_coll_name]
        indices = list(thin_coll_obj.list_indexes())[1:]
        ind_names, ind_field_names = [], []
        for dct in indices:
                ind_names.append(dct['name'])
                ind_field_names.append(dct.values()[1].keys()[0])
        field_names = list(thin_coll_obj.find_one().keys())[1:]
        print(f'''\nИнформация о наименьшей по количеству
полей коллекции - {thin_coll_name}:''')
        print('\nИмена ранее созданных индексов:\n', ind_names)
        print('\nИмена ранее проиндексированных полей:\n', ind_field_names)
        print('\nИмена всех полей:\n', field_names)
        
def remove_indices(coll_names, del_ind_names, db_obj):
        '''
        Функция удаления индексов.
        '''
        for coll_name in coll_names:
                for del_ind_name in del_ind_names:
                        db_obj[coll_name].drop_index(del_ind_name)
                        
class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасную параллельную индексацию.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции индексации
                всех коллекций MongoDB-базы.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.db_name = args.db_name
                self.print_db_info = args.print_db_info
                if args.ind_field_names == None:
                        self.ind_field_names = args.ind_field_names
                else:
                        self.ind_field_names = args.ind_field_names.split(',')
                        
        def add_indices(self, coll_name):
                '''
                Функция создания индексов
                выбранных исследователем
                полей одной коллекции.
                '''
                client = MongoClient()
                db_obj = client[self.db_name]
                ind_objs = [IndexModel([(ind_field_name, ASCENDING)]) for ind_field_name in self.ind_field_names]
                db_obj[coll_name].create_indexes(ind_objs)
                client.close()
                
####################################################################################################

import copy
from argparse import ArgumentParser
from pymongo import MongoClient, IndexModel, ASCENDING
from multiprocessing import Pool

#Подготовительный этап:
#обработка аргументов
#командной строки, создание
#экземпляра содержащего
#ключевую функцию класса
#и MongoDB-объектов,
#получение имён
#всех коллекций.
args = add_main_args()
prep_single_proc = PrepSingleProc(args)
client = MongoClient()
db_obj = client[prep_single_proc.db_name]
coll_names = db_obj.list_collection_names()

#При соответствующем
#желании исследователя
#определяем коллекцию с
#наименьшим числом полей
#и выводим информацию
#об её индексах и полях.
if prep_single_proc.print_db_info:
        min_fields_quan = len(db_obj[coll_names[0]].find_one())
        thin_coll_name = coll_names[0]
        if len(coll_names) > 1:
                for coll_name in coll_names[1:]:
                        fields_quan = len(db_obj[coll_name].find_one())
                        if fields_quan < min_fields_quan:
                                min_fields_quan = copy.deepcopy(fields_quan)
                                thin_coll_name = copy.deepcopy(coll_name)
        print_db_info(db_obj, thin_coll_name)
        
#По запросу исследователя
#удаляем индексы.
#Это - быстрый
#процесс, поэтому
#в распараллеливании
#не нуждается.
if args.del_ind_names != None:
        del_ind_names = args.del_ind_names.split(',')
        print(f'\nУдаляются индексы БД {prep_single_proc.db_name}')
        remove_indices(coll_names, del_ind_names, db_obj)
        
#Необходимости в клиенте
#MongoDB больше нет,
#поэтому дисконнектимся.
#Если дальше последует
#этап многопроцессовой
#индексации, то там уже
#надо будет подключаться
#на уровне каждого процесса.
client.close()

#Если исследователь пожелал
#индексировать поля, то
#сейчас будет произведена
#подготовка к параллельному
#выполнению этой процедуры.
if prep_single_proc.ind_field_names != None:
        inds_quan = len(prep_single_proc.ind_field_names)
        max_proc_quan = args.max_proc_quan
        if max_proc_quan > inds_quan <= 8:
                proc_quan = inds_quan
        elif max_proc_quan > 8:
                proc_quan = 8
        else:
                proc_quan = max_proc_quan
                
        print(f'\nБД {prep_single_proc.db_name} индексируется')
        print(f'\tколичество процессов: {proc_quan}')
        
        #Параллельный запуск индексации коллекций.
        with Pool(proc_quan) as pool_obj:
                pool_obj.map(prep_single_proc.add_indices, coll_names)
