# autopep8: off
import sys; sys.dont_write_bytecode = True
# autopep8: on
import locale
import os
import gzip
import copy
from pymongo import MongoClient
from backend.doc_to_line import restore_line
from backend.def_data_type import def_data_type
from backend.parallelize import parallelize
from backend.get_field_paths import parse_nested_objs
from backend.common_errors import (DifFmtsError,
                                   FormatIsNotSupportedError,
                                   QueryKeysOverlapWarning,
                                   NoSuchFieldWarning)
from cli.dock_cli import (add_args_ru,
                          add_args_en)

__version__ = 'v4.1'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2022-2023']


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
        Получение атрибутов как для основной функции программы, так и для блока
        многопроцессового запуска таковой. Первые из перечисленных ни в коем случае
        не должны будут потом в параллельных процессах изменяться. Немного о наиболее
        значимых атрибутах. Расширение исходных таблиц и квази-расширение коллекций
        нужны, как минимум, для выбора формат-ориентированного пересекательного запроса,
        и определения правил форматирования конечных файлов. Пользовательский запрос:
        его ключи, совпадающие с ключами встроенного запроса, пропадут. Исследователь
        может обойти эту проблему, упрятав своё выражение вовнутрь фиктивного $and.
        Умолчания по столбцам и полям выбраны на основе здравого смысла: к примеру,
        аннотировать src-VCF по src-db-VCF или src-db-BED логично, пересекая столбец
        и поле, оба из которых с идентификаторами вариантов. Сортировка программой
        никак не предусмотрена. Причина - в том, что конечные данные получаются не
        единым запросом (как в annotate), а множественными: сколько строк исходной
        таблицы, столько и запросов. Важные замечания по проджекшену + хорошая новость.
        В мае-2023 появилась возможность выковыривать из src-db-VCF как INFO, так и его
        отдельные элементы! Отсутствие в каком-либо документе значения проджектируемого
        INFO-подполя не разрушит форматирование конечной таблицы. К примеру, если из всего
        INFO выводить лишь INFO.GnomAD, а GnomAD-данных для текущего варианта нет, то в
        trg-TSV на место GnomAD=vals сместится ближайшая табуляция. Почему я только что
        написал trg-TSV? В этой программе, в отличие от других компонентов high-perf-bio,
        отбор полей src-db-VCF и src-db-BED не влияет на конечный формат. Получающиеся на
        выходе таблицы, независимо от применения/неприменения проджекшена - бесформатные
        (trg-TSV), поскольку они представляют собой гибрид исходной таблицы и коллекции.
        '''
        client = MongoClient()
        self.src_dir_path = os.path.normpath(args.src_dir_path)
        self.src_file_names = os.listdir(self.src_dir_path)
        src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                self.src_file_names))
        if len(src_file_fmts) > 1:
            raise DifFmtsError(src_file_fmts)
        self.src_file_fmt = list(src_file_fmts)[0]
        self.src_db_name = args.src_db_name
        src_db_obj = client[self.src_db_name]
        self.src_coll_names = src_db_obj.list_collection_names()
        self.src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
        self.trg_dir_path = os.path.normpath(args.trg_dir_path)
        self.proc_quan = min(args.max_proc_quan,
                             len(self.src_file_names),
                             os.cpu_count())
        self.meta_lines_quan = args.meta_lines_quan
        if args.extra_query in ['{}', '']:
            extra_query = {}
        else:
            extra_query = eval(args.extra_query)
        self.mongo_aggr_draft = [{'$match': extra_query},
                                 {'$addFields': None}]
        self.preset = args.preset
        self.mongo_exclude_meta = {'meta': {'$exists': False}}
        src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(self.mongo_exclude_meta))
        if self.preset == 'by_location':
            if self.src_file_fmt not in ['vcf', 'bed']:
                raise FormatIsNotSupportedError('preset',
                                                self.src_file_fmt)
            elif self.src_coll_ext not in ['vcf', 'bed']:
                raise FormatIsNotSupportedError('preset',
                                                self.src_coll_ext)
            if self.src_file_fmt == 'vcf':
                if self.src_coll_ext == 'vcf':
                    for extra_query_key in extra_query.keys():
                        if extra_query_key in ['#CHROM', 'POS']:
                            QueryKeysOverlapWarning(extra_query_key)
                    self.mongo_aggr_draft[0]['$match'] |= {'#CHROM': 'src_chrom_val',
                                                           'POS': 'src_pos_val'}
                elif self.src_coll_ext == 'bed':
                    for extra_query_key in extra_query.keys():
                        if extra_query_key in ['chrom', 'start', 'end']:
                            QueryKeysOverlapWarning(extra_query_key)
                    self.mongo_aggr_draft[0]['$match'] |= {'chrom': 'src_chrom_val',
                                                           'start': {'$lt': 'src_pos_val'},
                                                           'end': {'$gte': 'src_pos_val'}}
            elif self.src_file_fmt == 'bed':
                if self.src_coll_ext == 'vcf':
                    for extra_query_key in extra_query.keys():
                        if extra_query_key in ['#CHROM', 'POS']:
                            QueryKeysOverlapWarning(extra_query_key)
                    self.mongo_aggr_draft[0]['$match'] |= {'#CHROM': 'src_chrom_val',
                                                           'POS': {'$gt': 'src_start_val',
                                                                   '$lte': 'src_end_val'}}
                elif self.src_coll_ext == 'bed':
                    for extra_query_key in extra_query.keys():
                        if extra_query_key in ['chrom', 'start', 'end']:
                            QueryKeysOverlapWarning(extra_query_key)
                    self.mongo_aggr_draft[0]['$match'] |= {'chrom': 'src_chrom_val',
                                                           'start': {'$lt': 'src_end_val'},
                                                           'end': {'$gt': 'src_start_val'}}
        elif self.preset == 'by_alleles':
            if self.src_file_fmt != 'vcf':
                raise FormatIsNotSupportedError('preset',
                                                self.src_file_fmt)
            elif self.src_coll_ext != 'vcf':
                raise FormatIsNotSupportedError('preset',
                                                self.src_coll_ext)
            for extra_query_key in extra_query.keys():
                if extra_query_key in ['ID', 'REF', 'ALT']:
                    QueryKeysOverlapWarning(extra_query_key)
            self.mongo_aggr_draft[0]['$match'] |= {'ID': 'src_id_val',
                                                   'REF': 'src_ref_val',
                                                   'ALT': 'src_alt_val'}
        else:
            if args.ann_col_num in [None, 0]:
                if self.src_file_fmt == 'vcf':
                    self.ann_col_index = 2
                elif self.src_file_fmt == 'bed':
                    self.ann_col_index = 3
                else:
                    self.ann_col_index = 0
            else:
                self.ann_col_index = args.ann_col_num - 1
            if args.ann_field_path in [None, '']:
                if self.src_coll_ext == 'vcf':
                    self.ann_field_path = 'ID'
                elif self.src_coll_ext == 'bed':
                    self.ann_field_path = 'name'
                else:
                    self.ann_field_path = src_field_paths[1]
            else:
                if args.ann_field_path not in src_field_paths:
                    NoSuchFieldWarning(args.ann_field_path)
                self.ann_field_path = args.ann_field_path
            if self.ann_field_path in extra_query.keys():
                QueryKeysOverlapWarning(self.ann_field_path)
            self.mongo_aggr_draft[0]['$match'] |= {self.ann_field_path:
                                                   'src_val'}
        self.proj_field_toplvl_names = []
        if args.proj_field_names not in [None, '']:
            proj_field_names = args.proj_field_names.split(',')
            mongo_project = {}
            for proj_field_name in proj_field_names:
                self.proj_field_toplvl_names.append(proj_field_name.split('.')[0])
                mongo_project[proj_field_name] = 1
            self.mongo_aggr_draft.append({'$project':
                                          mongo_project})
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

    def dock(self, src_file_name):
        '''
        Функция пересечения одной таблицы поочерёдно со всеми коллекциями БД по заданной паре
        столбец-поле или по позиции в геноме. Главное отличие от аналогичной функции в annotate -
        невыкидывание столбцов аннотируемой таблицы. Набор MongoDB-объектов. Он должен быть строго
        индивидуальным для каждого процесса, иначе возможны конфликты. Считывание метастрок и работа
        с хэдером (шапкой). Комментирующие символы метастрок src-VCF - всегда ##. Для других, более
        вольных, форматов число метастрок поступает от исследователя. В src-BED-файлах, как правило,
        хэдер отсутствует, поэтому программа держит наготове шапку BED12, которая тут же обрезается
        в соответствии с реальным количеством столбцов. Исходно-табличная шапка обогащается суффиксом
        _f, чтобы в конечных файлах можно было легко отличить бывшие столбцы аннотируемой таблицы от
        бывших полей коллекции. Конечный хэдер - результат конкатенации набора имён полей и исходно-
        табличной шапки. Он может быть прорежен проджектом, но это не нарушает последовательность
        элементов. Что касается последовательности, из-за особенностей работы $addFields сохранившиеся
        столбцы таблицы идут после экс-полей коллекции, хотя мне бы хотелось оставить выбор расположения
        за исследователем. Обход коллекций. Обработка каждой исходной таблицы производится по всем
        коллекциям MongoDB-базы. Т.е. даже, если по одной из коллекций уже получились результаты,
        обход будет продолжаться и завершится лишь после обращения к последней коллекции. Конечные
        метастроки. Они повествуют о происхождении получающегося файла. Запросы. В annotate
        аннотируемый столбец (или координаты) запихивается в единый запрос. Здесь же каждый
        аннотируемый элемент идёт в запрос обособленный. Это позволяет достичь главной цели
        программы - охарактеризовать элемент, протащив в результаты хвост из его исходных
        аннотаций. Для координатных вычислений в коде заготовлены структуры запроса под
        все 4 возможных сочетания форматов VCF и BED. Политика по отношению к отсутствующим
        результатам. Если в конечном файле очутились только метастроки, он будет удалён.
        '''
        client = MongoClient()
        src_db_obj = client[self.src_db_name]
        mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
        src_file_base = src_file_name.rsplit('.', maxsplit=2)[0]
        with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
            if self.src_file_fmt == 'vcf':
                src_data_start = 0
                for line in src_file_opened:
                    src_data_start += len(line)
                    if not line.startswith('##'):
                        src_col_names = line.rstrip().split('\t')
                        break
            else:
                for meta_line_index in range(self.meta_lines_quan):
                    src_file_opened.readline()
                if self.src_file_fmt == 'bed':
                    src_data_start = src_file_opened.tell()
                    src_cols_quan = len(src_file_opened.readline().split('\t'))
                    src_file_opened.seek(src_data_start)
                    src_col_names = ['chrom', 'start', 'end', 'name',
                                     'score', 'strand', 'thickStart', 'thickEnd',
                                     'itemRgb', 'blockCount', 'blockSizes', 'blockStarts'][:src_cols_quan]
                else:
                    src_col_names = src_file_opened.readline().rstrip().split('\t')
                    src_data_start = src_file_opened.tell()
            src_col_names = list(map(lambda src_col_name:
                                     f'{src_col_name}_f',
                                     src_col_names))
            for src_coll_name in self.src_coll_names:
                src_coll_obj = src_db_obj[src_coll_name]
                src_field_names = list(src_coll_obj.find_one(self.mongo_exclude_meta))[1:]
                trg_col_names = src_field_names + src_col_names
                if self.proj_field_toplvl_names != []:
                    trg_col_names = list(filter(lambda trg_col_name:
                                                trg_col_name in self.proj_field_toplvl_names,
                                                trg_col_names))
                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'file-{src_file_base}__coll-{src_coll_base}.tsv.gz'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                    trg_file_opened.write(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.version}>\n')
                    trg_file_opened.write(f'##src_file_name={src_file_name}\n')
                    trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                    trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                    trg_file_opened.write(f'##mongo_aggr_draft={self.mongo_aggr_draft}\n')
                    trg_file_opened.write('\t'.join(trg_col_names) + '\n')
                    empty_res = True
                    for src_line in src_file_opened:
                        src_row = src_line.rstrip().split('\t')
                        if self.preset == 'by_location':
                            if self.src_file_fmt == 'vcf':
                                src_chrom_val = def_data_type(src_row[0].replace('chr', ''))
                                src_pos_val = int(src_row[1])
                                if self.src_coll_ext == 'vcf':
                                    mongo_aggr_arg[0]['$match']['#CHROM'] = src_chrom_val
                                    mongo_aggr_arg[0]['$match']['POS'] = src_pos_val
                                elif self.src_coll_ext == 'bed':
                                    mongo_aggr_arg[0]['$match']['chrom'] = src_chrom_val
                                    mongo_aggr_arg[0]['$match']['start']['$lt'] = src_pos_val
                                    mongo_aggr_arg[0]['$match']['end']['$gte'] = src_pos_val
                            elif self.src_file_fmt == 'bed':
                                src_chrom_val = def_data_type(src_row[0].replace('chr', ''))
                                src_start_val = int(src_row[1])
                                src_end_val = int(src_row[2])
                                if self.src_coll_ext == 'vcf':
                                    mongo_aggr_arg[0]['$match']['#CHROM'] = src_chrom_val
                                    mongo_aggr_arg[0]['$match']['POS']['$gt'] = src_start_val
                                    mongo_aggr_arg[0]['$match']['POS']['$lte'] = src_end_val
                                elif self.src_coll_ext == 'bed':
                                    mongo_aggr_arg[0]['$match']['chrom'] = src_chrom_val
                                    mongo_aggr_arg[0]['$match']['start']['$lt'] = src_end_val
                                    mongo_aggr_arg[0]['$match']['end']['$gt'] = src_start_val
                        elif self.preset == 'by_alleles':
                            src_id_val = src_row[2]
                            src_ref_val = src_row[3]
                            src_alt_val = src_row[4]
                            mongo_aggr_arg[0]['$match']['ID'] = src_id_val
                            mongo_aggr_arg[0]['$match']['REF'] = src_ref_val
                            mongo_aggr_arg[0]['$match']['ALT'] = src_alt_val
                        else:
                            src_val = def_data_type(src_row[self.ann_col_index])
                            mongo_aggr_arg[0]['$match'][self.ann_field_path] = src_val
                        mongo_aggr_arg[1]['$addFields'] = dict(zip(src_col_names,
                                                                   src_row))
                        curs_obj = src_coll_obj.aggregate(mongo_aggr_arg)
                        for doc in curs_obj:
                            trg_file_opened.write(restore_line(doc,
                                                               'tsv',
                                                               self.sec_delimiter))
                            empty_res = False
                if empty_res:
                    os.remove(trg_file_path)
                src_file_opened.seek(src_data_start)
        client.close()


# Обработка аргументов командной строки.
# Создание экземпляра содержащего ключевую
# функцию класса. Параллельный запуск
# аннотирования. Замер времени выполнения
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
    print(f'\nAnnotation by {main.src_db_name} DB')
    print(f'\tquantity of parallel processes: {proc_quan}')
    exec_time = parallelize(proc_quan, main.dock,
                            main.src_file_names)
    print(f'\tparallel computation time: {exec_time}')
