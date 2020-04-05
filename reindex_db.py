__version__ = 'V2.3'

def add_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description='''
Программа, производящая индексацию
MongoDB-базы в несколько процессов
и удаляющая старые индексы.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V2.3.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).

Для вывода имён баз данных, индексов и
полей можете использовать print_db_info.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя переиндексируемой БД')
        argparser.add_argument('-r', '--del-ind-names', metavar='[None]', dest='del_ind_names', type=str,
                               help='Имена удаляемых индексов (через запятую без пробела)')
        argparser.add_argument('-a', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                               help='Имена индексируемых полей (через запятую без пробела)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно индексируемых коллекций')
        args = argparser.parse_args()
        return args

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

from argparse import ArgumentParser, RawTextHelpFormatter
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
args, client = add_args(), MongoClient()
prep_single_proc = PrepSingleProc(args)
db_name = prep_single_proc.db_name
db_obj = client[db_name]
coll_names = db_obj.list_collection_names()

#По запросу исследователя
#удаляем индексы.
#Это - очень быстрый
#процесс, поэтому
#в распараллеливании
#не нуждается.
if args.del_ind_names != None:
        del_ind_names = args.del_ind_names.split(',')
        print(f'\nУдаляются индексы БД {db_name}')
        remove_indices(coll_names, del_ind_names, db_obj)
        
#Необходимости в подключении
#к серверу MongoDB больше нет,
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
#Количество процессов будет
#определено по заданному
#исследователем максимуму
#этого значения и количеству
#коллекций переиндексируемой БД.
if prep_single_proc.ind_field_names != None:
        max_proc_quan = args.max_proc_quan
        colls_quan = len(coll_names)
        if max_proc_quan > colls_quan <= 8:
                proc_quan = colls_quan
        elif max_proc_quan > 8:
                proc_quan = 8
        else:
                proc_quan = max_proc_quan
                
        print(f'\nИндексируется БД {db_name}')
        print(f'\tколичество параллельных процессов: {proc_quan}')
        
        #Параллельный запуск индексации коллекций.
        with Pool(proc_quan) as pool_obj:
                pool_obj.map(prep_single_proc.add_indices, coll_names)
