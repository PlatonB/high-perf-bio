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
                     DESCENDING)
from backend.doc_to_line import restore_line
from backend.parallelize import parallelize
from backend.common_errors import (FormatIsNotSupportedError,
                                   NoSuchFieldWarning)
from backend.get_field_paths import parse_nested_objs
from cli.ljoin_cli import add_args_ru

__version__ = 'v12.0'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2020-2023']


class NotEnoughCollsError(Exception):
    '''
    В базе должно быть, как
    минимум, 2 коллекции, чтобы
    было, что с чем джойнить.
    '''

    def __init__(self):
        err_msg = '''\nAt least two collections are required
for intersection or subtraction'''
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
        так и для блока многопроцессового запуска таковой. Первые
        из перечисленных ни в коем случае не должны будут потом
        в параллельных процессах изменяться. Некоторые неочевидные,
        но важные детали об атрибутах. Квази-расширение коллекций.
        Оно нужно, как минимум, для определения правил сортировки
        и форматирования конечных файлов. Сортировка. Её лучше
        ставить ближе к началу пайплайна: она тогда задействует
        индекс сама и не отбирает эту возможность у других стадий.
        Набор правых коллекций. На этапе создания атрибутов он не
        окончательный. Впоследствии в каждом потоке отбирается свой
        список правых, не поглядывающих налево. Пересекаемое/вычитаемое
        поле по-умолчанию. Оно подобрано на основании здравого смысла.
        К примеру, вряд ли придёт в голову пересекать src-db-VCF по
        полю, отличному от ID. Проджекшен (отбор полей). Поля src-db-VCF
        я, скрепя сердце, позволил отбирать, но документы со вложенными
        объектами, как, например, в INFO, не сконвертируются в обычные
        строки, а сериализуются как есть. Реализован проджекшен несколько
        экзотически. Если в остальных компонентах тулкита напрямую
        отбираются указанные исследователем поля, то здесь применяется,
        наоборот, удаление всех неуказанных. Иначе потом после объединения
        левого документа с правыми, представления последних срежутся,
        и программе не удастся подсчитать совпадения (или несовпадения).
        Что касается и src-db-VCF, и src-db-BED, когда мы оставляем
        только часть полей, невозможно гарантировать соблюдение
        спецификаций соответствующих форматов, поэтому вывод будет
        формироваться не более, чем просто табулированным (trg-TSV).
        '''
        client = MongoClient()
        self.src_db_name = args.src_db_name
        src_db_obj = client[self.src_db_name]
        self.src_coll_names = src_db_obj.list_collection_names()
        if len(self.src_coll_names) < 2:
            raise NotEnoughCollsError()
        self.src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
        self.trg_file_fmt = self.src_coll_ext
        self.trg_dir_path = os.path.normpath(args.trg_dir_path)
        if args.left_coll_names in [None, '']:
            self.left_coll_names = set(self.src_coll_names)
        else:
            self.left_coll_names = set(args.left_coll_names.split(','))
        self.proc_quan = min(args.max_proc_quan,
                             len(self.left_coll_names),
                             os.cpu_count())
        if args.right_coll_names in [None, '']:
            self.right_coll_names = set(self.src_coll_names)
        else:
            self.right_coll_names = set(args.right_coll_names.split(','))
        right_colls_quan = len(self.right_coll_names)
        mongo_exclude_meta = {'meta': {'$exists': False}}
        if args.extra_query in ['{}', '']:
            extra_query = {}
        else:
            extra_query = eval(args.extra_query)
        mongo_match = mongo_exclude_meta | extra_query
        self.preset = args.preset
        src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(mongo_exclude_meta))
        if self.preset == 'by_location':
            if self.src_coll_ext not in ['vcf', 'bed']:
                raise FormatIsNotSupportedError('preset',
                                                self.src_coll_ext)
        elif self.preset == 'by_alleles':
            if self.src_coll_ext != 'vcf':
                raise FormatIsNotSupportedError('preset',
                                                self.src_coll_ext)
        elif args.lookup_field_path in [None, '']:
            if self.src_coll_ext == 'vcf':
                self.lookup_field_path = 'ID'
            elif self.src_coll_ext == 'bed':
                self.lookup_field_path = 'name'
            else:
                self.lookup_field_path = src_field_paths[1]
        else:
            if args.lookup_field_path not in src_field_paths:
                NoSuchFieldWarning(args.lookup_field_path)
            self.lookup_field_path = args.lookup_field_path
        self.action = args.action
        if args.coverage == 0 or args.coverage > right_colls_quan:
            self.coverage = right_colls_quan
        else:
            self.coverage = args.coverage
        if len(self.right_coll_names & self.left_coll_names) > 0 \
           and 1 < self.coverage == right_colls_quan:
            self.coverage -= 1
        self.mongo_aggr_draft = [{'$match': mongo_match}]
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
        elif self.src_coll_ext == 'vcf':
            self.mongo_aggr_draft.append({'$sort': SON([('#CHROM', ASCENDING),
                                                        ('POS', ASCENDING)])})
        elif self.src_coll_ext == 'bed':
            self.mongo_aggr_draft.append({'$sort': SON([('chrom', ASCENDING),
                                                        ('start', ASCENDING),
                                                        ('end', ASCENDING)])})
        if args.proj_field_names in [None, '']:
            self.mongo_findone_args = [mongo_exclude_meta, None]
        else:
            proj_field_names = args.proj_field_names.split(',')
            del_field_names = list(filter(lambda src_field_path: '.' not in src_field_path,
                                          src_field_paths))
            for proj_field_name in proj_field_names:
                if proj_field_name not in del_field_names:
                    NoSuchFieldWarning(proj_field_name)
                del_field_names.remove(proj_field_name)
            mongo_project = {del_field_name: 0 for del_field_name in del_field_names}
            mongo_project['_id'] = 1
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
        self.version = version
        client.close()

    def intersect_subtract(self, left_coll_name):
        '''
        Функция подготовки и запуска левостороннего
        внешнего объединения одной левой коллекции
        поочерёдно со всеми правыми, а также отбора
        полученных результатов в соответствии с задачей.
        '''

        # Набор MongoDB-объектов
        # должен быть строго
        # индивидуальным для
        # каждого процесса, иначе
        # возможны конфликты.
        client = MongoClient()
        src_db_obj = client[self.src_db_name]
        left_coll_obj = src_db_obj[left_coll_name]

        # Дальнейшее построение пайплайна будет вестись
        # в пределах каждого процесса по-своему.
        mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)

        # Предотвращение возможной попытки агрегации коллекции самой с собой. Сортировка имён правых
        # коллекций для большей читабельности MongoDB-запроса в метастроке будущего конечного файла.
        right_coll_names = sorted(filter(lambda right_coll_name: right_coll_name != left_coll_name,
                                         self.right_coll_names))

        # Если правая коллекция лишь одна
        # и при этом совпадает с текущей
        # левой, то процесс будет прерван.
        if right_coll_names != []:

            # Дефолтное либо кастомное поле, по которому потом выполнится left join, утверждено ещё
            # при инициализации атрибутов. В этом блоке кода находят своё место 4 возможных запроса.
            # Механизм пересечения и вычитания через левосторонее объединение я красочно описал в ридми.
            # Небольшая памятка: в let назначаются правые переменные, а сослаться на них можно через $$.
            if self.preset == 'by_location':
                if self.src_coll_ext == 'vcf':
                    mongo_aggr_arg += [{'$lookup': {'from': right_coll_name,
                                                    'let': {'chrom': '$#CHROM', 'pos': '$POS'},
                                                    'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$#CHROM', '$$chrom']},
                                                                                                {'$eq': ['$POS', '$$pos']}]}}}],
                                                    'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                elif self.src_coll_ext == 'bed':
                    mongo_aggr_arg += [{'$lookup': {'from': right_coll_name,
                                                    'let': {'chrom': '$chrom', 'start': '$start', 'end': '$end'},
                                                    'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$chrom', '$$chrom']},
                                                                                                {'$lt': ['$start', '$$end']},
                                                                                                {'$gt': ['$end', '$$start']}]}}}],
                                                    'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
            elif self.preset == 'by_alleles':
                mongo_aggr_arg += [{'$lookup': {'from': right_coll_name,
                                                'let': {'id': '$ID', 'ref': '$REF', 'alt': '$ALT'},
                                                'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$ID', '$$id']},
                                                                                            {'$eq': ['$REF', '$$ref']},
                                                                                            {'$eq': ['$ALT', '$$alt']}]}}}],
                                                'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
            else:
                mongo_aggr_arg += [{'$lookup': {'from': right_coll_name,
                                                'localField': self.lookup_field_path,
                                                'foreignField': self.lookup_field_path,
                                                'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]

            # Завершаем пайплайн этапом удаления ненужных или
            # переставших быть нужными исследователю полей.
            if self.mongo_findone_args[1] is not None:
                mongo_aggr_arg.append({'$project': self.mongo_findone_args[1]})

            # Выполняем пайплайн из скипа метадокумента, сортировки (кастомной,
            # дефолтной или вообще никакой), левостороннего объединения, и,
            # быть может, проджекшена. allowDiskUse подстрахует при сортировке
            # больших непроиндексированных полей. numericOrdering нужен
            # для того, чтобы после условного rs19 не оказался rs2.
            curs_obj = left_coll_obj.aggregate(mongo_aggr_arg,
                                               allowDiskUse=True,
                                               collation=Collation(locale='en_US',
                                                                   numericOrdering=True))

            # Чтобы шапка повторяла шапку той таблицы, по которой делалась
            # коллекция, создадим её из имён полей. Projection при этом учтём.
            # Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
            # есть поля с генотипами, то шапка дополнится элементом FORMAT.
            trg_col_names = list(left_coll_obj.find_one(*self.mongo_findone_args))[1:]
            if self.trg_file_fmt == 'vcf' and len(trg_col_names) > 8:
                trg_col_names.insert(8, 'FORMAT')

            # Конструируем имя конечного архива и абсолютный путь к этому файлу.
            # Происхождение имени файла от имени левой коллекции будет указывать на
            # то, что все данные, попадающие в файл, берутся исключительно из неё.
            left_coll_base = left_coll_name.rsplit('.', maxsplit=1)[0]
            trg_file_name = f'leftcoll-{left_coll_base}__act-{self.action}__cov-{self.coverage}.{self.trg_file_fmt}.gz'
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
                trg_file_opened.write(f'##left_coll_name={left_coll_name}\n')
                if len(mongo_aggr_arg) > 5:
                    mongo_aggr_arg = mongo_aggr_arg[:5] + ['...']
                trg_file_opened.write(f'##mongo_aggr={mongo_aggr_arg}\n')
                trg_file_opened.write(f'##action={self.action}\n')
                trg_file_opened.write(f'##coverage={self.coverage}\n')
                if self.trg_file_fmt == 'bed':
                    trg_file_opened.write(f'##trg_col_names=<{",".join(trg_col_names)}>\n')
                else:
                    trg_file_opened.write('\t'.join(trg_col_names) + '\n')

                # Создаём флаг, по которому далее будет
                # определено, оказались ли в конечном
                # файле строки, отличные от хэдеров.
                empty_res = True

                # Правила фильтрации результатов левостороннего внешнего
                # объединения должны были быть заданы исследователем. Первый
                # фильтр - само действие - пересечение или вычитание: судьба
                # левого документа будет определяться непустыми результирующими
                # списками при пересечении и пустыми в случае вычитания. Второй
                # фильтр - охват: левый документ получит приглашение в конечный
                # файл, только если будет достигнут порог количества непустых/пустых
                # результирующих списков. Подробности - в readme проекта.
                for doc in curs_obj:
                    cov_meter = 0
                    for right_coll_name in right_coll_names:
                        right_coll_alias = right_coll_name.replace('.', '_')
                        if (self.action == 'intersect' and doc[right_coll_alias] != []) or \
                           (self.action == 'subtract' and doc[right_coll_alias] == []):
                            cov_meter += 1
                        del doc[right_coll_alias]
                    if cov_meter >= self.coverage:
                        trg_file_opened.write(restore_line(doc,
                                                           self.trg_file_fmt,
                                                           self.sec_delimiter))
                        empty_res = False

            # Если флаг-индикатор так и
            # остался равен True, значит,
            # результатов пересечения/вычитания
            # для данной левой коллекции нет, и в
            # конечный файл попали только хэдеры.
            # Такие конечные файлы программа удалит.
            if empty_res:
                os.remove(trg_file_path)

        # Дисконнект.
        client.close()


# Обработка аргументов командной строки.
# Создание экземпляра содержащего ключевую
# функцию класса. Параллельный запуск пересечения
# или вычитания. Замер времени выполнения
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
    print(f'\n{main.action.capitalize()}ing collections of {main.src_db_name} DB')
    print(f'\tcoverage: {main.coverage}')
    print(f'\tquantity of parallel processes: {proc_quan}')
    exec_time = parallelize(proc_quan, main.intersect_subtract,
                            main.left_coll_names)
    print(f'\tparallel computation time: {exec_time}')
