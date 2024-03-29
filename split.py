# autopep8: off
import sys; sys.dont_write_bytecode = True
# autopep8: on
import locale
import os
import copy
import gzip
from bson.son import SON
from pymongo.collation import Collation
from pymongo import (MongoClient,
                     ASCENDING,
                     DESCENDING,
                     IndexModel)
from backend.doc_to_line import restore_line
from backend.parallelize import parallelize
from backend.get_field_paths import parse_nested_objs
from backend.common_errors import (DbAlreadyExistsError,
                                   QueryKeysOverlapWarning,
                                   NoSuchFieldWarning)
from cli.split_cli import (add_args_ru,
                           add_args_en)

__version__ = 'v6.2'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2021-2023']


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
        так и для блока многопроцессового запуска таковой. Первые
        из перечисленных ни в коем случае не должны будут потом в
        параллельных процессах изменяться. Некоторые неочевидные,
        но важные детали об атрибутах. Квази-расширение коллекций.
        Оно нужно, как минимум, для определения правил форматирования
        конечных файлов. Пользовательский запрос. Его ключи, совпадающие
        с ключами встроенного запроса, пропадут. Исследователь может
        обойти эту проблему, упрятав своё выражение вовнутрь фиктивного
        $and. Сортировка. Без вмешательства исследователя она не
        производится. Я так сделал по той причине, что коллекции
        квази-форматов src-db-VCF и src-db-BED изначально,
        как и положено, отсортированы по координатам, и без
        добавления этапа $sort отобранные документы поступают
        в конечные файлы или коллекции в оригинальном порядке.
        Но если задан кастомный порядок сортировки, то результат
        будет уже не trg-(db-)VCF/BED. Проджекшен (отбор полей).
        Поля src-db-VCF я, скрепя сердце, позволил отбирать, но
        документы со вложенными объектами, как, например, в INFO,
        не сконвертируются в обычные строки, а выведутся как
        есть. Что касается и src-db-VCF, и src-db-BED, когда мы
        оставляем только часть полей, невозможно гарантировать
        соблюдение спецификаций соответствующих форматов,
        поэтому вывод будет формироваться не более,
        чем просто табулированным (trg-(db-)TSV).
        '''
        client = MongoClient()
        self.src_db_name = args.src_db_name
        src_db_obj = client[self.src_db_name]
        self.src_coll_names = src_db_obj.list_collection_names()
        src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
        self.trg_file_fmt = src_coll_ext
        if '/' in args.trg_place:
            self.trg_dir_path = os.path.normpath(args.trg_place)
        elif args.trg_place != self.src_db_name \
            and (args.trg_place not in client.list_database_names()
                 or args.rewrite_existing_db):
            client.drop_database(args.trg_place)
            self.trg_db_name = args.trg_place
        else:
            raise DbAlreadyExistsError()
        self.proc_quan = min(args.max_proc_quan,
                             len(self.src_coll_names),
                             os.cpu_count())
        if args.extra_query in ['{}', '']:
            extra_query = {}
        else:
            extra_query = eval(args.extra_query)
        mongo_exclude_meta = {'meta': {'$exists': False}}
        src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(mongo_exclude_meta))
        if args.spl_field_path in [None, '']:
            if src_coll_ext == 'vcf':
                self.spl_field_path = '#CHROM'
            elif src_coll_ext == 'bed':
                self.spl_field_path = 'chrom'
            else:
                self.spl_field_path = src_field_paths[1]
        else:
            if args.spl_field_path not in src_field_paths:
                NoSuchFieldWarning(args.spl_field_path)
            self.spl_field_path = args.spl_field_path
        if self.spl_field_path in extra_query.keys():
            QueryKeysOverlapWarning(self.spl_field_path)
        self.mongo_aggr_draft = [{'$match': extra_query}]
        if args.srt_field_group not in [None, '']:
            mongo_sort = SON([])
            if args.srt_order == 'asc':
                srt_order = ASCENDING
            elif args.srt_order == 'desc':
                srt_order = DESCENDING
            for srt_field_path in args.srt_field_group.split('+'):
                if srt_field_path not in src_field_paths:
                    NoSuchFieldWarning(srt_field_path)
                mongo_sort[srt_field_path] = srt_order
            self.mongo_aggr_draft.append({'$sort': mongo_sort})
            self.trg_file_fmt = 'tsv'
        if args.proj_field_names in [None, '']:
            self.mongo_findone_args = [mongo_exclude_meta, None]
        else:
            proj_field_names = args.proj_field_names.split(',')
            mongo_project = {}
            for proj_field_name in proj_field_names:
                if proj_field_name not in src_field_paths:
                    NoSuchFieldWarning(proj_field_name)
                mongo_project[proj_field_name] = 1
            self.mongo_aggr_draft.append({'$project': mongo_project})
            self.mongo_findone_args = [mongo_exclude_meta, mongo_project]
            self.trg_file_fmt = 'tsv'
        if args.sec_delimiter == 'colon':
            self.sec_delimiter = ':'
        elif args.sec_delimiter == 'comma':
            self.sec_delimiter = ','
        elif args.sec_delimiter == 'low_line':
            self.sec_delimiter = '_'
        elif args.sec_delimiter == 'pipe':
            self.sec_delimiter = '|'
        elif args.sec_delimiter == 'semicolon':
            self.sec_delimiter = ';'
        if args.ind_field_groups in [None, '']:
            if self.trg_file_fmt == 'vcf':
                self.index_models = [IndexModel([('#CHROM', ASCENDING),
                                                 ('POS', ASCENDING)]),
                                     IndexModel([('ID', ASCENDING)])]
            elif self.trg_file_fmt == 'bed':
                self.index_models = [IndexModel([('chrom', ASCENDING),
                                                 ('start', ASCENDING),
                                                 ('end', ASCENDING)]),
                                     IndexModel([('name', ASCENDING)])]
            elif args.proj_field_names in [None, '']:
                self.index_models = [IndexModel([(src_field_paths[1], ASCENDING)])]
            else:
                self.index_models = [IndexModel([(proj_field_names[0], ASCENDING)])]
        else:
            self.index_models = []
            for ind_field_group in args.ind_field_groups.split(','):
                index_tups = []
                for ind_field_path in ind_field_group.split('+'):
                    if ind_field_path not in src_field_paths:
                        NoSuchFieldWarning(ind_field_path)
                    index_tups.append((ind_field_path, ASCENDING))
                self.index_models.append(IndexModel(index_tups))
        self.version = version
        client.close()

    def split(self, src_coll_name):
        '''
        Функция дробления одной коллекции.
        '''

        # Набор MongoDB-объектов
        # должен быть строго
        # индивидуальным для
        # каждого процесса, иначе
        # возможны конфликты.
        client = MongoClient()
        src_db_obj = client[self.src_db_name]
        src_coll_obj = src_db_obj[src_coll_name]

        # Накопления уникализированных (далее - раздельных) значений обозначенного поля
        # коллекции. _id - null, т.к. нам не нужно растаскивание этих значений по каким-либо
        # группам. Операция не является частью пайплайна $match-$sort(-$project)(-$out).
        get_sep_vals = [{'$group': {'_id': 'null',
                                    'spl_field': {'$addToSet': f'${self.spl_field_path}'}}}]
        sep_vals = [doc for doc in src_coll_obj.aggregate(get_sep_vals)][0]['spl_field']
        if type(sep_vals[0]) is list:
            sep_vals = [sep_val for row in sep_vals for sep_val in row]

        # Запрос, вынесенный в отдельный объект,
        # можно будет спокойно модифицировать
        # внутри распараллеливаемой функции.
        mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)

        # Название исходной коллекции (без квазирасширения) потом
        # пригодится для построения имени конечного файла или коллекции.
        src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]

        # Этот большой блок осуществляет
        # запрос с выводом результатов в файлы.
        if hasattr(self, 'trg_dir_path'):

            # Чтобы шапка повторяла шапку той таблицы, по которой делалась
            # коллекция, создадим её из имён полей. Projection при этом учтём.
            # Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
            # есть поля с генотипами, то шапка дополнится элементом FORMAT.
            trg_col_names = list(src_coll_obj.find_one(*self.mongo_findone_args))[1:]
            if self.trg_file_fmt == 'vcf' and len(trg_col_names) > 8:
                trg_col_names.insert(8, 'FORMAT')

            # Один конечный файл будет
            # содержать данные, соответствующие
            # одному раздельному значению.
            for sep_val in sep_vals:

                # Внедряем в фильтрационную стадию
                # пайплайна текущее раздельное значение.
                mongo_aggr_arg[0]['$match'][self.spl_field_path] = sep_val

                # Создаём объект курсора. allowDiskUse пригодится для сортировки
                # больших непроиндексированных полей. numericOrdering нужен
                # для того, чтобы после условного rs19 не оказался rs2.
                curs_obj = src_coll_obj.aggregate(mongo_aggr_arg,
                                                  allowDiskUse=True,
                                                  collation=Collation(locale='en_US',
                                                                      numericOrdering=True))

                # Конструируем имя конечного архива и абсолютный путь.
                trg_file_name = f'coll-{src_coll_base}__{self.spl_field_path}-{str(sep_val).replace("/", "s")}.{self.trg_file_fmt}.gz'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)

                # Открытие конечного файла на запись.
                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:

                    # Формируем и прописываем метастроки, повествующие о
                    # происхождении конечного файла. Прописываем также табличную
                    # шапку. Стандартом BED шапка не приветствуется, поэтому
                    # для trg-BED она будет мимикрировать под метастроку.
                    if self.trg_file_fmt == 'vcf':
                        trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                    trg_file_opened.write(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.version}>\n')
                    trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                    trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                    trg_file_opened.write(f'##mongo_aggr={mongo_aggr_arg}\n')
                    if self.trg_file_fmt == 'bed':
                        trg_file_opened.write(f'##trg_col_names=<{",".join(trg_col_names)}>\n')
                    else:
                        trg_file_opened.write('\t'.join(trg_col_names) + '\n')

                    # Извлечение из объекта курсора отвечающих запросу
                    # документов, преобразование их значений в обычные
                    # строки и прописывание последних в конечный файл.
                    # Проверка, вылез ли по запросу хоть один документ.
                    empty_res = True
                    for doc in curs_obj:
                        trg_file_opened.write(restore_line(doc,
                                                           self.trg_file_fmt,
                                                           self.sec_delimiter))
                        empty_res = False

                # Удаление конечного файла, если в
                # нём очутились только метастроки.
                if empty_res:
                    os.remove(trg_file_path)

        # Та же работа, но с выводом в БД. Опишу некоторые
        # особенности. При работе с каждым раздельным значением
        # Aggregation-инструкция обогащается этапом вывода в
        # конечную коллекцию. Метастроки складываются в список,
        # а он, в свою очередь, встраивается в первый документ
        # коллекции. Если этот документ так и остаётся в гордом
        # одиночестве, коллекция удаляется. Для непустых конечных
        # коллекций создаются дефолтные или пользовательские индексы.
        elif hasattr(self, 'trg_db_name'):
            trg_db_obj = client[self.trg_db_name]
            mongo_aggr_arg.append({'$merge': {'into': {'db': self.trg_db_name,
                                                       'coll': None}}})
            for sep_val in sep_vals:
                mongo_aggr_arg[0]['$match'][self.spl_field_path] = sep_val
                trg_coll_name = f'coll-{src_coll_base}__{self.spl_field_path}-{str(sep_val).replace("/", "s")}.{self.trg_file_fmt}'
                mongo_aggr_arg[-1]['$merge']['into']['coll'] = trg_coll_name
                trg_coll_obj = trg_db_obj.create_collection(trg_coll_name,
                                                            storageEngine={'wiredTiger':
                                                                           {'configString':
                                                                            'block_compressor=zstd'}})
                meta_lines = {'meta': []}
                if self.trg_file_fmt == 'vcf':
                    meta_lines['meta'].append(f'##fileformat={self.trg_file_fmt.upper()}')
                meta_lines['meta'].append(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.version}>')
                meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                meta_lines['meta'].append(f'##src_coll_name={src_coll_name}')
                meta_lines['meta'].append(f'##mongo_aggr={mongo_aggr_arg}')
                trg_coll_obj.insert_one(meta_lines)
                src_coll_obj.aggregate(mongo_aggr_arg,
                                       allowDiskUse=True,
                                       collation=Collation(locale='en_US',
                                                           numericOrdering=True))
                if trg_coll_obj.count_documents({}) == 1:
                    trg_db_obj.drop_collection(trg_coll_name)
                else:
                    trg_coll_obj.create_indexes(self.index_models)

        # Дисконнект.
        client.close()


# Обработка аргументов командной строки.
# Создание экземпляра содержащего ключевую
# функцию класса. Параллельный запуск
# измельчения. Замер времени выполнения
# вычислений с точностью до микросекунды.
if __name__ == '__main__':
    if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__,
                           __authors__)
    else:
        args = add_args_en(__version__,
                           __authors__)
    main = Main(args, __version__)
    proc_quan = main.proc_quan
    print(f'\nSplitting collections of {main.src_db_name} DB')
    print(f'\tquantity of parallel processes: {proc_quan}')
    exec_time = parallelize(proc_quan, main.split,
                            main.src_coll_names)
    print(f'\tparallel computation time: {exec_time}')
