__version__ = 'v1.0'

def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, считающая количество каждого значения
заданного поля в пределах каждой коллекции.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Обсчитываемая база должна быть создана с помощью create_db.

К вложенному полю обращайтесь через точку:
field_1.field_2.(...).field_N

Из-за ограничений со стороны MongoDB, программа не использует индексы.

Условные обозначения в справке по CLI:
[значение по умолчанию];
{{допустимые значения}}
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя анализируемой БД')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        man_grp.add_argument('-F', '--field-name', required=True, metavar='str', dest='field_name', type=str,
                             help='Имя поля коллекций БД, для которого считать количество каждого значения')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-b', '--quan-thres', metavar='[1]', default=1, dest='quan_thres', type=int,
                             help='Нижняя граница количества')
        opt_grp.add_argument('-o', '--quan-sort-order', metavar='[desc]', choices=['asc', 'desc'], default='desc', dest='quan_sort_order', type=str,
                             help='{asc, desc} Порядок сортировки по количеству')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обрабатываемых коллекций')
        args = arg_parser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под безопасный
        параллельный подсчёт элементарной статистики
        для определённого поля коллекций MongoDB.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной под многопроцессовое
                выполнение функции определения количества значений заданного поля.
                Атрибуты ни в коем случае не должны будут потом в параллельных
                процессах изменяться. Получаются они в основном из указанных
                исследователем аргументов. Также в ините собирается пайплайн,
                который при выполнении способен будет не только подсчитать
                количество каждого значения нужного поля, но и поработать
                над этими количествами: отфильтровать, если требуется,
                по минимальному порогу и отсортировать вверх или вниз.
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                if '/' in args.trg_place:
                        self.trg_dir_path = os.path.normpath(args.trg_place)
                elif args.trg_place != self.src_db_name:
                        self.trg_db_name = args.trg_place
                        resolve_db_existence(self.trg_db_name)
                else:
                        raise DbAlreadyExistsError()
                self.field_name = args.field_name
                self.mongo_aggr_draft = [{'$group': {'_id': f'${self.field_name}',
                                                     'quantity': {'$sum': 1}}}]
                if args.quan_thres > 1:
                        self.mongo_aggr_draft.append({'$match': {'quantity': {'$gte': args.quan_thres}}})
                if args.quan_sort_order == 'asc':
                        quan_sort_order = ASCENDING
                elif args.quan_sort_order == 'desc':
                        quan_sort_order = DESCENDING
                self.mongo_aggr_draft.append({'$sort': SON([('quantity',
                                                             quan_sort_order)])})
                self.ver = ver
                client.close()
                
        def count(self, src_coll_name):
                '''
                Функция нахождения количества
                элементов поля одной коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                src_coll_obj = src_db_obj[src_coll_name]
                
                #Aggregation-инструкция может быть потом дополнена
                #индивидуальным для текущей коллекции $out-этапом.
                #В связи с перспективой внутрипроцессовой модификации
                #общего выражения, создаём отдельный объект с этим
                #выражением, который точно не страшно ковырять.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                
                #Получаем имя конечного файла. Оно же при
                #необходимости - имя конечной коллекции.
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
                        
                        #Конструируем абсолютный путь к конечному файлу.
                        trg_file_path = os.path.join(self.trg_dir_path,
                                                     trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##database={self.src_db_name}\n')
                                trg_file_opened.write(f'##collection={src_coll_name}\n')
                                trg_file_opened.write(f'{self.field_name}\tquantity\n')
                                
                                #Извлечение из объекта курсора результатов, преобразование их значений
                                #в табулированные строки и прописывание последних в конечный файл. Если
                                #исследователь пережестил с нижним порогом количества, то кроме метастрок
                                #в конечном файле ничего не окажется. Тогда файл будет направлен на удаление.
                                empty_res = True
                                for doc in curs_obj:
                                        trg_file_opened.write('\t'.join(map(str, doc.values())) + '\n')
                                        empty_res = False
                                        
                        #Удаление конечного файла, если в
                        #нём очутились только метастроки.
                        if empty_res:
                                os.remove(trg_file_path)
                                
                #Создание конечной базы и коллекции.
                #Обогащение aggregation-инструкции
                #этапом вывода в конечную коллекцию.
                #Последняя, если не пополнилась результатами,
                #удаляется. Для количественного поля
                #непустой коллекции создаётся индекс.
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
                                trg_coll_obj.create_index('quantity')
                                
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime, copy

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING, DESCENDING
from backend.resolve_db_existence import resolve_db_existence, DbAlreadyExistsError
from bson.son import SON
from multiprocessing import Pool
from backend.create_index_models import create_index_models

#Подготовительный этап: обработка аргументов
#командной строки, создание экземпляра
#содержащего ключевую функцию класса, получение
#имён и количества перемалываемых коллекций,
#определение оптимального числа процессов.
args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args,
                                  __version__)
src_coll_names = prep_single_proc.src_coll_names
colls_quan = len(src_coll_names)
if max_proc_quan > colls_quan <= 8:
        proc_quan = colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nCounting values in {prep_single_proc.src_db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск расчёта. Замер времени
#выполнения вычислений с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.count, src_coll_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
