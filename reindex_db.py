__version__ = 'v3.1'

def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, способная удалять имеющиеся
индексы MongoDB-базы и добавлять новые.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Не путайте понятия имени поля и имени индекса.

Для вывода имён баз данных, индексов и полей
рекомендую использовать print_db_info из
состава high-perf-bio, либо MongoDB Compass.

Поддерживается создание/удаление как
одиночных, так и составных индексов.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя переиндексируемой БД')
        argparser.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                               help='Имена удаляемых индексов (через запятую без пробела)')
        argparser.add_argument('-a', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                               help='Имена индексируемых полей (через запятую без пробела; для составного индекса: через плюс без пробелов)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно индексируемых коллекций')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасную параллельную индексацию.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых заточенной под
                многопроцессовое выполнение функции индексации
                всех коллекций MongoDB-базы. Атрибуты должны быть
                созданы единожды и далее ни в коем случае не изменяться.
                Получаются они в основном из указанных исследователем опций.
                '''
                self.db_name = args.db_name
                if args.ind_field_names is None:
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
                ind_objs = [IndexModel([(ind_field_name, ASCENDING) for ind_field_name in element.split('+')]) \
                            for element in self.ind_field_names]
                db_obj[coll_name].create_indexes(ind_objs)
                client.close()
                
####################################################################################################

import datetime
from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, IndexModel, ASCENDING
from multiprocessing import Pool

#Подготовительный этап: обработка
#аргументов командной строки, создание
#экземпляра содержащего ключевую
#функцию класса и MongoDB-объектов,
#получение имён всех коллекций.
args, client = add_args(__version__), MongoClient()
prep_single_proc = PrepSingleProc(args)
db_name = prep_single_proc.db_name
db_obj = client[db_name]
coll_names = db_obj.list_collection_names()

#По запросу исследователя удаляем индексы.
#Это - очень быстрый процесс, поэтому
#в распараллеливании не нуждается.
if args.del_ind_names is not None:
        del_ind_names = args.del_ind_names.split(',')
        print(f'\nRemoving indexes from {db_name} database')
        for coll_name in coll_names:
                for del_ind_name in del_ind_names:
                        db_obj[coll_name].drop_index(del_ind_name)
                        
#Необходимости в подключении
#к серверу MongoDB больше нет,
#поэтому дисконнектимся.
#Если дальше последует
#этап многопроцессовой
#индексации, то там уже
#надо будет подключаться
#на уровне каждого процесса.
client.close()

#Если исследователь пожелал индексировать поля, то
#сейчас будет произведена подготовка к параллельному
#выполнению этой процедуры. Количество процессов будет
#определено по заданному исследователем максимуму этого
#значения и количеству коллекций переиндексируемой БД.
if prep_single_proc.ind_field_names is not None:
        max_proc_quan = args.max_proc_quan
        colls_quan = len(coll_names)
        if max_proc_quan > colls_quan <= 8:
                proc_quan = colls_quan
        elif max_proc_quan > 8:
                proc_quan = 8
        else:
                proc_quan = max_proc_quan
                
        print(f'\nIndexing {db_name} database')
        print(f'\tnumber of parallel processes: {proc_quan}')
        
        #Параллельный запуск индексации коллекций.
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(prep_single_proc.add_indices, coll_names)
                exec_time = datetime.datetime.now() - exec_time_start
                
        print(f'\tparallel computation time: {exec_time}')
