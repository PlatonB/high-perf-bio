__version__ = 'v4.2'

import sys, locale, datetime, copy, os
sys.dont_write_bytecode = True
from cli.concatenate_cli import add_args_ru, add_args_en
from pymongo import MongoClient, IndexModel, ASCENDING
from backend.common_errors import DbAlreadyExistsError, NoSuchFieldError
from backend.get_field_paths import parse_nested_objs

class Main():
        '''
        Основной класс. args, подаваемый иниту на вход, не обязательно
        должен формироваться argparse. Этим объектом может быть экземпляр
        класса из стороннего Python-модуля, в т.ч. имеющего отношение к GUI.
        Кстати, написание сообществом всевозможных графических интерфейсов
        к high-perf-bio люто, бешено приветствуется! В ините на основе args
        создаются как атрибуты, используемые главной функцией, так и атрибуты,
        нужные для кода, её запускающего. Что касается этой функции, её
        можно запросто пристроить в качестве коллбэка кнопки в GUI.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов как для основной функции программы, так и для
                блока запуска таковой. Некоторые неочевидные, но важные детали об
                этих атрибутах. Квази-расширение конечной коллекции. Оно нужно,
                чтобы потом парсеры знали, как её обрабатывать. Проджекшен (отбор
                полей). Что касается как src-db-VCF, так и src-db-BED, когда мы
                оставляем только часть полей, невозможно гарантировать соблюдение
                спецификаций этих форматов, поэтому вывод будет формироваться не
                более, чем просто табулированным (trg-db-TSV). Конкатенирующий запрос
                формируется тоже в __init__. Конкатенацию можно произвести в двух
                режимах - с сохранением повторяющихся конечных MongoDB-документов
                и без. Со вторым не так уж всё интуитивно понятно. 1. Сравнение идёт
                по документам как единому целому, т.е. несоответствие хотя бы по
                одному полю позволит обоим документам сосуществовать. 2. Уникальные
                идентификаторы документов при сопоставлении не учитываются. 3. Документы
                становятся неполными после применения проджекшена и сверяются уже в
                таком виде. 4. Unique-индексы конечной коллекции блокируют заливку
                вложенных структур, а значит, trg-db-VCF уникализировать нельзя.
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                src_db_obj = client[self.src_db_name]
                self.src_coll_names = src_db_obj.list_collection_names()
                src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if args.trg_db_name != self.src_db_name \
                     and (args.trg_db_name not in client.list_database_names() \
                          or args.rewrite_existing_db):
                        client.drop_database(args.trg_db_name)
                        self.trg_db_name = args.trg_db_name
                else:
                        raise DbAlreadyExistsError()
                mongo_exclude_meta = {'meta': {'$exists': False}}
                src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(mongo_exclude_meta))
                if args.proj_field_names in [None, '']:
                        mongo_project = {'_id': 0}
                        self.trg_coll_ext = src_coll_ext
                else:
                        proj_field_names = args.proj_field_names.split(',')
                        for proj_field_name in proj_field_names:
                                if proj_field_name not in src_field_paths:
                                        raise NoSuchFieldError(proj_field_name)
                        mongo_project = {proj_field_name: 1 for proj_field_name in proj_field_names}
                        mongo_project['_id'] = 0
                        self.trg_coll_ext = 'tsv'
                if not args.del_copies:
                        self.mongo_on = '_id'
                        self.mongo_when_match = 'keepExisting'
                else:
                        self.mongo_on = list(src_db_obj[self.src_coll_names[0]].find_one(mongo_exclude_meta,
                                                                                         mongo_project))
                        self.mongo_when_match = 'replace'
                self.trg_coll_name = f'db-{self.src_db_name}__wm-{self.mongo_when_match}.{self.trg_coll_ext}'
                mongo_merge = {'into': {'db': self.trg_db_name,
                                        'coll': self.trg_coll_name},
                               'on': self.mongo_on,
                               'whenMatched': self.mongo_when_match}
                self.mongo_aggr_draft = [{'$match': mongo_exclude_meta},
                                         {'$project': mongo_project},
                                         {'$merge': mongo_merge}]
                if args.ind_field_groups in [None, '']:
                        if self.trg_coll_ext == 'vcf':
                                self.index_models = [IndexModel([('#CHROM', ASCENDING),
                                                                 ('POS', ASCENDING)]),
                                                     IndexModel([('ID', ASCENDING)])]
                        elif self.trg_coll_ext == 'bed':
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
                                                raise NoSuchFieldError(ind_field_path)
                                        else:
                                                index_tups.append((ind_field_path, ASCENDING))
                                self.index_models.append(IndexModel(index_tups))
                self.ver = ver
                client.close()
                
        def concatenate(self):
                '''
                Функция объединения коллекций с размещением результатов
                в новую БД. Отдельный набор MongoDB-объектов, а также
                глубокая копия объекта запроса нужны для унификации
                структуры кода с остальными программами проекта.
                '''
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                trg_db_obj = client[self.trg_db_name]
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                trg_coll_obj = trg_db_obj.create_collection(self.trg_coll_name,
                                                            storageEngine={'wiredTiger':
                                                                           {'configString':
                                                                            'block_compressor=zstd'}})
                meta_lines = {'meta': []}
                if self.trg_coll_ext == 'vcf':
                        meta_lines['meta'].append(f'##fileformat={self.trg_coll_ext.upper()}')
                meta_lines['meta'].append(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>')
                meta_lines['meta'].append(f'##src_db_name={self.src_db_name}')
                meta_lines['meta'].append(f'##mongo_aggr={mongo_aggr_arg}')
                trg_coll_obj.insert_one(meta_lines)
                if self.mongo_when_match == 'replace':
                        trg_coll_obj.create_index([(field_name, ASCENDING) for field_name in self.mongo_on],
                                                  unique=True)
                for src_coll_name in self.src_coll_names:
                        src_db_obj[src_coll_name].aggregate(mongo_aggr_arg)
                trg_coll_obj.create_indexes(self.index_models)
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Запуск объединения.
#Замер времени выполнения вычислений
#с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        print(f'\nConcatenating {main.src_db_name} DB')
        exec_time_start = datetime.datetime.now()
        main.concatenate()
        exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tcomputation time: {exec_time}')
