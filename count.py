__version__ = 'v3.0'

import sys, locale, os, datetime, copy, gzip
sys.dont_write_bytecode = True
from cli.count_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from bson.decimal128 import Decimal128
from bson.son import SON
from multiprocessing import Pool

class CombFiltersError(Exception):
        '''
        Считаю, что сочетание этих
        фильтров бессмысленно и
        лишь замедляет подсчёты.
        Можете оспорить в Issues.
        '''
        def __init__(self):
                err_msg = '\nIt is not possible to apply quantity and frequency filters at once'
                super().__init__(err_msg)
                
class Main():
        '''
        Основной класс. args, подаваемый иниту на вход, не обязательно
        должен формироваться argparse. Этим объектом может быть экземпляр
        класса из стороннего Python-модуля, в т.ч. имеющего отношение к GUI.
        Кстати, написание сообществом всевозможных графических интерфейсов
        к high-perf-bio люто, бешено приветствуется! В ините на основе args
        создаются как атрибуты, используемые распараллеливаемой функцией,
        так и атрибуты, нужные для кода, её запускающего. Что касается этой
        функции, её можно запросто пристроить в качестве коллбэка кнопки в GUI.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов как для основной функции программы, так и для блока
                многопроцессового запуска таковой. Первые из перечисленных ни в коем
                случае не должны будут потом в параллельных процессах изменяться. В
                ините собирается пайплайн, который при выполнении способен будет не
                только подсчитать количество и частоту каждого значения нужного поля,
                но и поработать над этими результатами: отфильтровать, если исследователю
                требуется, по минимальному порогу, плюс отсортировать вверх или вниз.
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                self.src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if '/' in args.trg_place:
                        self.trg_dir_path = os.path.normpath(args.trg_place)
                elif args.trg_place != self.src_db_name:
                        self.trg_db_name = args.trg_place
                        resolve_db_existence(self.trg_db_name)
                else:
                        raise DbAlreadyExistsError()
                max_proc_quan = args.max_proc_quan
                colls_quan = len(self.src_coll_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > colls_quan <= cpus_quan:
                        self.proc_quan = colls_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                if args.field_name is None:
                        if self.src_coll_ext == 'vcf':
                                self.field_name = 'ID'
                        elif self.src_coll_ext == 'bed':
                                self.field_name = 'name'
                        else:
                                self.field_name = 'rsID'
                else:
                        self.field_name = args.field_name
                self.mongo_aggr_draft = [{'$group': {'_id': f'${self.field_name}',
                                                     'quantity': {'$sum': 1}}},
                                         {'$addFields': {'frequency': {'$divide': ['$quantity', None]}}}]
                self.addfields_ind = 1
                if args.quan_thres > 1 and args.freq_thres is not None:
                        raise CombFiltersError()
                elif args.quan_thres > 1:
                        self.mongo_aggr_draft.insert(1, {'$match': {'quantity': {'$gte': args.quan_thres}}})
                        self.addfields_ind = 2
                elif args.freq_thres is not None:
                        self.mongo_aggr_draft.append({'$match': {'frequency': {'$gte': Decimal128(args.freq_thres)}}})
                if args.quan_sort_order == 'asc':
                        self.quan_sort_order = ASCENDING
                elif args.quan_sort_order == 'desc':
                        self.quan_sort_order = DESCENDING
                self.mongo_aggr_draft.append({'$sort': SON([('quantity', self.quan_sort_order)])})
                self.ver = ver
                client.close()
                
        def count(self, src_coll_name):
                '''
                Функция нахождения количества и
                частоты элементов поля одной коллекции,
                а также фильтрации полученных значений.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                src_coll_obj = src_db_obj[src_coll_name]
                
                #В связи с необходимостью внутрипроцессовой модификации общего выражения,
                #создаём отдельный объект с этим выражением, который точно не страшно
                #ковырять. Для получения возможности подсчёта частоты, вычисляем и
                #внедряем в копию пайплайна количество документов всей текущей коллекции.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                mongo_aggr_arg[self.addfields_ind]['$addFields']['frequency']['$divide'][1] = src_coll_obj.count_documents({})
                
                #Получаем имя конечного файла, правда, без .gz.
                #Оно же при необходимости - имя конечной коллекции.
                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'{src_coll_base}_count_res.tsv'
                
                #Этот большой блок осуществляет
                #запрос с выводом результатов в файл.
                if hasattr(self, 'trg_dir_path'):
                        
                        #Для выполнения пайплайна предусматриваем возможность
                        #откладывания промежуточных результатов во временные
                        #файлы. Так приходится делать из-за того, что стадия
                        #$group пренебрегает бесплатными услугами индексов.
                        curs_obj = src_coll_obj.aggregate(mongo_aggr_arg,
                                                          allowDiskUse=True)
                        
                        #Конструируем абсолютный путь к конечному архиву.
                        trg_file_path = os.path.join(self.trg_dir_path,
                                                     f'{trg_file_name}.gz')
                        
                        #Открытие конечного файла на запись.
                        with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##database={self.src_db_name}\n')
                                trg_file_opened.write(f'##collection={src_coll_name}\n')
                                trg_file_opened.write(f'##pipeline={mongo_aggr_arg}\n')
                                trg_file_opened.write(f'{self.field_name}\tquantity\tfrequency\n')
                                
                                #Извлечение из объекта курсора результатов, преобразование их значений
                                #в табулированные строки и прописывание последних в конечный файл. Если
                                #исследователь пережестил с порогом, то кроме метастрок в конечном
                                #файле ничего не окажется. Тогда файл будет направлен на удаление.
                                empty_res = True
                                for doc in curs_obj:
                                        trg_file_opened.write('\t'.join(map(str, doc.values())) + '\n')
                                        empty_res = False
                                        
                        #Удаление конечного файла, если в
                        #нём очутились только метастроки.
                        if empty_res:
                                os.remove(trg_file_path)
                                
                #Создание конечной базы и коллекции. Обогащение aggregation-инструкции
                #этапом вывода в конечную коллекцию. Последняя, если не пополнилась
                #результатами, удаляется. Для полей непустой коллекции создаются индексы.
                elif hasattr(self, 'trg_db_name'):
                        trg_db_obj = client[self.trg_db_name]
                        trg_coll_obj = trg_db_obj.create_collection(trg_file_name,
                                                                    storageEngine={'wiredTiger':
                                                                                   {'configString':
                                                                                    'block_compressor=zstd'}})
                        mongo_aggr_arg.append({'$out': {'db': self.trg_db_name,
                                                        'coll': trg_file_name}})
                        src_coll_obj.aggregate(mongo_aggr_arg,
                                               allowDiskUse=True)
                        if trg_coll_obj.count_documents({}) == 0:
                                trg_db_obj.drop_collection(trg_file_name)
                        else:
                                trg_coll_obj.create_indexes([IndexModel([(self.field_name, self.quan_sort_order)]),
                                                             IndexModel([('quantity', self.quan_sort_order)]),
                                                             IndexModel([('frequency', self.quan_sort_order)])])
                                
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки. Создание
#экземпляра содержащего ключевую функцию класса.
#Параллельный запуск расчёта. Замер времени
#выполнения вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nCounting values in {main.src_db_name} database')
        print(f'\tnumber of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.count, main.src_coll_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
