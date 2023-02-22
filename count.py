__version__ = 'v7.4'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2021-2023']

import sys, locale, os, copy, gzip
sys.dont_write_bytecode = True
from cli.count_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING, DESCENDING, IndexModel
from backend.common_errors import DbAlreadyExistsError, NoSuchFieldWarning
from backend.get_field_paths import parse_nested_objs
from backend.parallelize import parallelize
from bson.decimal128 import Decimal128
from bson.son import SON

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
        def __init__(self, args, version):
                '''
                Получение атрибутов как для основной функции программы,
                так и для блока многопроцессового запуска таковой. В
                ините собирается пайплайн, который при выполнении способен
                будет не только подсчитать количество и частоту каждого
                набора связанных значений нужных полей, но и поработать
                над этими результатами: отфильтровать по минимальному
                порогу, плюс отсортировать вверх или вниз. Чтобы избежать
                лишних вычислений, поле частоты формируется уже после
                возможной фильтрации абсолютных значений. Определение
                частоты допускается только в случае, если в базе одна
                коллекция. Иначе пришлось бы, дико усложнив синтаксис
                CLI, реализовавать инпут более одного знаменателя, так,
                чтобы каждый знаменатель соответствовал определённой
                коллекции. В обсчитываемых полях могут встречаться списки.
                Они мешают горизонтальному слиянию исходных значений,
                поэтому по умолчанию раскрываются. Но списки, которые
                находятся между первым и последним уровнями вложенности,
                не раскроются. Благо, такие хитровложенные структуры
                в биоинформатических данных вам вряд ли попадутся.
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                src_db_obj = client[self.src_db_name]
                self.src_coll_names = src_db_obj.list_collection_names()
                src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if '/' in args.trg_place:
                        self.trg_dir_path = os.path.normpath(args.trg_place)
                elif args.trg_place != self.src_db_name \
                     and (args.trg_place not in client.list_database_names() \
                          or args.rewrite_existing_db):
                        client.drop_database(args.trg_place)
                        self.trg_db_name = args.trg_place
                else:
                        raise DbAlreadyExistsError()
                src_colls_quan = len(self.src_coll_names)
                self.proc_quan = min(args.max_proc_quan,
                                     src_colls_quan,
                                     os.cpu_count())
                mongo_exclude_meta = {'meta': {'$exists': False}}
                src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(mongo_exclude_meta))
                if args.cnt_field_paths in [None, '']:
                        if src_coll_ext == 'vcf':
                                cnt_field_paths = ['ID']
                        elif src_coll_ext == 'bed':
                                cnt_field_paths = ['name']
                        else:
                                cnt_field_paths = [src_field_paths[1]]
                else:
                        cnt_field_paths = args.cnt_field_paths.split(',')
                        for cnt_field_path in cnt_field_paths:
                                if cnt_field_path not in src_field_paths:
                                        NoSuchFieldWarning(cnt_field_path)
                self.mongo_aggr_draft = [{'$match': mongo_exclude_meta}]
                if not args.not_unfold_arrays:
                        for cnt_field_path in cnt_field_paths:
                                if '.' in cnt_field_path:
                                        self.mongo_aggr_draft.append({'$unwind': f'${cnt_field_path.split(".")[0]}'})
                                self.mongo_aggr_draft.append({'$unwind': f'${cnt_field_path}'})
                cnt_field_paths_quan = len(cnt_field_paths)
                if args.sec_delimiter == 'colon':
                        sec_delimiter = ':'
                elif args.sec_delimiter == 'comma':
                        sec_delimiter = ','
                elif args.sec_delimiter == 'low_line':
                        sec_delimiter = '_'
                elif args.sec_delimiter == 'pipe':
                        sec_delimiter = '|'
                elif args.sec_delimiter == 'semicolon':
                        sec_delimiter = ';'
                if cnt_field_paths_quan > 1:
                        mongo_id = {'$concat': []}
                        for cnt_field_path_ind in range(cnt_field_paths_quan):
                                mongo_id['$concat'].append({'$toString': f'${cnt_field_paths[cnt_field_path_ind]}'})
                                if cnt_field_path_ind < cnt_field_paths_quan - 1:
                                        mongo_id['$concat'].append(sec_delimiter)
                else:
                        mongo_id = f'${cnt_field_paths[0]}'
                self.mongo_aggr_draft.append({'$group': {'_id': mongo_id,
                                                         'quantity': {'$sum': 1}}})
                self.trg_header = [sec_delimiter.join(cnt_field_paths),
                                   'quantity']
                if args.quan_thres > 1 and args.freq_thres not in [None, '']:
                        raise CombFiltersError()
                elif args.quan_thres > 1:
                        self.mongo_aggr_draft.append({'$match': {'quantity': {'$gte': args.quan_thres}}})
                if src_colls_quan == 1 and args.samp_quan not in [None, 0]:
                        self.mongo_aggr_draft.append({'$addFields': {'frequency': {'$divide': ['$quantity',
                                                                                               args.samp_quan]}}})
                        self.trg_header.append('frequency')
                        if args.freq_thres not in [None, '']:
                                self.mongo_aggr_draft.append({'$match': {'frequency': {'$gte': Decimal128(args.freq_thres)}}})
                if args.quan_sort_order == 'asc':
                        self.quan_sort_order = ASCENDING
                elif args.quan_sort_order == 'desc':
                        self.quan_sort_order = DESCENDING
                self.mongo_aggr_draft.append({'$sort': SON([('quantity',
                                                             self.quan_sort_order)])})
                self.version = version
                client.close()
                
        def count(self, src_coll_name):
                '''
                Функция нахождения количества и частоты
                связок элементов полей одной коллекции,
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
                
                #Запрос, вынесенный в отдельный объект,
                #можно будет спокойно модифицировать
                #внутри распараллеливаемой функции.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                
                #Получаем имя конечного файла, правда, без .gz.
                #Оно же при необходимости - имя конечной коллекции.
                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'coll-{src_coll_base}__quan.tsv'
                
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
                                trg_file_opened.write(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.version}>\n')
                                trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                                trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                                trg_file_opened.write(f'##mongo_aggr={mongo_aggr_arg}\n')
                                trg_file_opened.write('\t'.join(self.trg_header) + '\n')
                                
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
                                
                #Та же работа, но с выводом в БД. Опишу некоторые особенности.
                #Aggregation-инструкция обогащается этапом вывода в конечную
                #коллекцию. Метастроки складываются в список, а он, в свою
                #очередь, встраивается в первый документ коллекции. Для конечной
                #коллекции создаются раздельные индексы полей quantity и, если
                #сформировалось, frequency. К полю, подлежащему обсчёту, поскольку
                #оно выступает в роли _id, по-умолчанию добавляется unique-индекс.
                elif hasattr(self, 'trg_db_name'):
                        trg_db_obj = client[self.trg_db_name]
                        mongo_aggr_arg.append({'$merge': {'into': {'db': self.trg_db_name,
                                                                   'coll': trg_file_name}}})
                        trg_coll_obj = trg_db_obj.create_collection(trg_file_name,
                                                                    storageEngine={'wiredTiger':
                                                                                   {'configString':
                                                                                    'block_compressor=zstd'}})
                        meta_lines = {'meta': []}
                        meta_lines['meta'].append(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.version}>')
                        meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                        meta_lines['meta'].append(f'##src_coll_name={src_coll_name}')
                        meta_lines['meta'].append(f'##mongo_aggr={mongo_aggr_arg}')
                        trg_coll_obj.insert_one(meta_lines)
                        src_coll_obj.aggregate(mongo_aggr_arg, allowDiskUse=True)
                        trg_coll_obj.create_indexes([IndexModel([(ind_field_name,
                                                                  self.quan_sort_order)]) for ind_field_name in self.trg_header[1:]])
                        
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки. Создание
#экземпляра содержащего ключевую функцию класса.
#Параллельный запуск расчёта. Замер времени
#выполнения вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__,
                                   __authors__)
        else:
                args = add_args_en(__version__,
                                   __authors__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nCounting sets of related values in {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        exec_time = parallelize(proc_quan, main.count,
                                main.src_coll_names)
        print(f'\tparallel computation time: {exec_time}')
