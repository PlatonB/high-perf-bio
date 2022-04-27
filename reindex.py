__version__ = 'v5.3'

import sys, locale, os, datetime
sys.dont_write_bytecode = True
from cli.reindex_cli import add_args_ru, add_args_en
from pymongo import MongoClient, IndexModel, ASCENDING
from multiprocessing import Pool

class Main():
        '''
        Основной класс. args, подаваемый иниту на вход,
        не обязательно должен формироваться argparse. Этим
        объектом может быть экземпляр класса из стороннего
        Python-модуля, в т.ч. имеющего отношение к GUI. Кстати,
        написание сообществом всевозможных графических интерфейсов
        к high-perf-bio люто, бешено приветствуется! В ините
        на основе args создаются как атрибуты, используемые
        двумя функциями, так и атрибуты, нужные для кода,
        их запускающего. Что касается этих функций, их можно
        запросто пристроить в качестве коллбэков кнопок в GUI.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов для функций удаления и создания
                индексов, а также блока, в котором эти функции
                запускаются. Используемые функцией создания
                индексов атрибуты ни в коем случае не должны
                будут потом в параллельных процессах изменяться.
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                max_proc_quan = args.max_proc_quan
                src_colls_quan = len(self.src_coll_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > src_colls_quan <= cpus_quan:
                        self.proc_quan = src_colls_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                if args.del_ind_names in [None, '']:
                        self.del_ind_names = args.del_ind_names
                else:
                        self.del_ind_names = args.del_ind_names.split(',')
                if args.ind_field_groups in [None, '']:
                        self.index_models = args.ind_field_groups
                else:
                        self.index_models = [IndexModel([(ind_field_path, ASCENDING) for ind_field_path in ind_field_group.split('+')]) \
                                             for ind_field_group in args.ind_field_groups.split(',')]
                client.close()
                
        def del_indices(self):
                '''
                Функция удаления
                выбранных исследователем
                индексов всех коллекций.
                '''
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                for src_coll_name in self.src_coll_names:
                        for del_ind_name in self.del_ind_names:
                                src_db_obj[src_coll_name].drop_index(del_ind_name)
                client.close()
                
        def add_indices(self, src_coll_name):
                '''
                Функция создания индексов
                выбранных исследователем
                полей одной коллекции.
                '''
                client = MongoClient()
                client[self.src_db_name][src_coll_name].create_indexes(self.index_models)
                client.close()
                
#Обработка аргументов командной строки. Создание
#экземпляра содержащего ключевые функции класса.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
main = Main(args)
src_db_name = main.src_db_name

#По запросу исследователя удаляем индексы.
#Это - очень быстрый процесс, поэтому
#в распараллеливании не нуждается.
if main.del_ind_names not in [None, '']:
        print(f'\nRemoving indexes from {src_db_name} DB')
        main.del_indices()
        
#Параллельный запуск индексации, если это действие задал исследователь.
#Замер времени выполнения вычислений с точностью до микросекунды.
if main.index_models not in [None, '']:
        proc_quan = main.proc_quan
        print(f'\nIndexing {src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.add_indices, main.src_coll_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
