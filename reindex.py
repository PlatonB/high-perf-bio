__version__ = 'v6.0'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2020-2023']

import sys, locale, os
sys.dont_write_bytecode = True
from cli.reindex_cli import add_args_ru, add_args_en
from pymongo import MongoClient, IndexModel, ASCENDING
from backend.get_field_paths import parse_nested_objs
from backend.common_errors import NoSuchFieldWarning
from backend.parallelize import parallelize

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
                src_db_obj = client[self.src_db_name]
                self.src_coll_names = src_db_obj.list_collection_names()
                if args.del_ind_names in [None, '']:
                        self.del_ind_names = args.del_ind_names
                else:
                        self.del_ind_names = args.del_ind_names.split(',')
                if args.ind_field_groups in [None, '']:
                        self.index_models = args.ind_field_groups
                else:
                        self.proc_quan = min(args.max_proc_quan,
                                             len(self.src_coll_names),
                                             os.cpu_count())
                        src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one({'meta':
                                                                                                         {'$exists':
                                                                                                          False}}))
                        self.index_models = []
                        for ind_field_group in args.ind_field_groups.split(','):
                                index_tups = []
                                for ind_field_path in ind_field_group.split('+'):
                                        if ind_field_path not in src_field_paths:
                                                NoSuchFieldWarning(ind_field_path)
                                        index_tups.append((ind_field_path, ASCENDING))
                                self.index_models.append(IndexModel(index_tups))
                client.close()
                
        def del_indices(self):
                '''
                Функция удаления
                выбранных исследователем
                индексов всех коллекций.
                '''
                client = MongoClient()
                for src_coll_name in self.src_coll_names:
                        for del_ind_name in self.del_ind_names:
                                client[self.src_db_name][src_coll_name].drop_index(del_ind_name)
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
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. По запросу исследователя
#удаляем индексы. Это - очень быстрый
#процесс, поэтому в распараллеливании
#не нуждается. Параллельный запуск
#индексации, если это действие задал
#исследователь. Замер времени выполнения
#индексации с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__,
                                   __authors__)
        else:
                args = add_args_en(__version__,
                                   __authors__)
        main = Main(args)
        src_db_name = main.src_db_name
        if main.del_ind_names not in [None, '']:
                print(f'\nRemoving indexes from {src_db_name} DB')
                main.del_indices()
        if main.index_models not in [None, '']:
                proc_quan = main.proc_quan
                print(f'\nIndexing {src_db_name} DB')
                print(f'\tquantity of parallel processes: {proc_quan}')
                exec_time = parallelize(proc_quan, main.add_indices,
                                        main.src_coll_names)
                print(f'\tparallel computation time: {exec_time}')
