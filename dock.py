__version__ = 'v1.2'

import sys, locale, os, datetime, gzip, copy
sys.dont_write_bytecode = True
from cli.dock_cli import add_args_ru, add_args_en
from pymongo import MongoClient
from multiprocessing import Pool
from backend.common_errors import DifFmtsError, ByLocTsvError, NoSuchFieldError
from backend.get_field_paths import parse_nested_objs
from backend.def_data_type import def_data_type
from backend.doc_to_line import restore_line

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
                многопроцессового запуска таковой. Первые из перечисленных ни в коем случае
                не должны будут потом в параллельных процессах изменяться. Немного о наиболее
                значимых атрибутах. Расширение исходных таблиц и квази-расширение коллекций
                нужны, как минимум, для выбора формат-ориентированного пересекательного
                запроса, и определения правил форматирования конечных файлов. Умолчания по
                столбцам и полям выбраны на основе здравого смысла: к примеру, аннотировать
                src-VCF по src-db-VCF или src-db-BED логично, пересекая столбец и поле,
                оба из которых с идентификаторами вариантов. Сортировка программой никак
                не предусмотрена. Причина - в том, что конечные данные получаются не единым
                запросом (как в annotate), а множественными: сколько строк исходной таблицы,
                столько и запросов. Важные замечания по проджекшену. Поля src-db-VCF я,
                скрепя сердце, позволил отбирать, но документы со вложенными объектами,
                как, например, в INFO, не сконвертируются в обычные строки, а сериализуются
                как есть. В этой программе, в отличие от других компонентов high-perf-bio,
                отбор полей src-db-VCF и src-db-BED не влияет на конечный формат. Получающиеся
                на выходе таблицы, независимо от применения/неприменения проджекшена - бесформатные
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
                max_proc_quan = args.max_proc_quan
                src_files_quan = len(self.src_file_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > src_files_quan <= cpus_quan:
                        self.proc_quan = src_files_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                self.meta_lines_quan = args.meta_lines_quan
                self.by_loc = args.by_loc
                self.mongo_exclude_meta = {'meta': {'$exists': False}}
                src_field_paths = parse_nested_objs(src_db_obj[self.src_coll_names[0]].find_one(self.mongo_exclude_meta))
                if self.by_loc:
                        if self.src_file_fmt not in ['vcf', 'bed'] or self.src_coll_ext not in ['vcf', 'bed']:
                                raise ByLocTsvError()
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
                        elif args.ann_field_path not in src_field_paths:
                                raise NoSuchFieldError(args.ann_field_path)
                        else:
                                self.ann_field_path = args.ann_field_path
                self.mongo_aggr_draft = [{'$match': None}, {'$addFields': None}]
                self.mongo_project = {}
                if args.proj_field_names not in [None, '']:
                        proj_field_names = args.proj_field_names.split(',')
                        for proj_field_name in proj_field_names:
                                self.mongo_project[proj_field_name] = 1
                        self.mongo_aggr_draft.append({'$project': self.mongo_project})
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
                self.ver = ver
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
                        src_col_names = list(map(lambda src_col_name: f'{src_col_name}_f',
                                                 src_col_names))
                        for src_coll_name in self.src_coll_names:
                                src_coll_obj = src_db_obj[src_coll_name]
                                src_field_names = list(src_coll_obj.find_one(self.mongo_exclude_meta))[1:]
                                trg_col_names = src_field_names + src_col_names
                                if self.mongo_project != {}:
                                        trg_col_names = list(filter(lambda proj_field_name: proj_field_name in self.mongo_project,
                                                                    trg_col_names))
                                src_coll_base = src_coll_name.rsplit('.', maxsplit=1)[0]
                                trg_file_name = f'file-{src_file_base}__coll-{src_coll_base}.tsv.gz'
                                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                                with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                        trg_file_opened.write(f'##tool_name=<high-perf-bio,{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                        trg_file_opened.write(f'##src_file_name={src_file_name}\n')
                                        trg_file_opened.write(f'##src_db_name={self.src_db_name}\n')
                                        trg_file_opened.write(f'##src_coll_name={src_coll_name}\n')
                                        if not self.by_loc:
                                                trg_file_opened.write(f'##ann_field_path={self.ann_field_path}\n')
                                        if self.mongo_project != {}:
                                                trg_file_opened.write(f'##mongo_project={self.mongo_project}\n')
                                        trg_file_opened.write('\t'.join(trg_col_names) + '\n')
                                        empty_res = True
                                        for src_line in src_file_opened:
                                                src_row = src_line.rstrip().split('\t')
                                                if self.by_loc:
                                                        if self.src_file_fmt == 'vcf':
                                                                src_chrom, src_pos = def_data_type(src_row[0].replace('chr', '')), int(src_row[1])
                                                                if self.src_coll_ext == 'vcf':
                                                                        mongo_aggr_arg[0]['$match'] = {'#CHROM': src_chrom,
                                                                                                       'POS': src_pos}
                                                                elif self.src_coll_ext == 'bed':
                                                                        mongo_aggr_arg[0]['$match'] = {'chrom': src_chrom,
                                                                                                       'start': {'$lt': src_pos},
                                                                                                       'end': {'$gte': src_pos}}
                                                        elif self.src_file_fmt == 'bed':
                                                                src_chrom, src_start, src_end = def_data_type(src_row[0].replace('chr', '')), int(src_row[1]), int(src_row[2])
                                                                if self.src_coll_ext == 'vcf':
                                                                        mongo_aggr_arg[0]['$match'] = {'#CHROM': src_chrom,
                                                                                                       'POS': {'$gt': src_start,
                                                                                                               '$lte': src_end}}
                                                                elif self.src_coll_ext == 'bed':
                                                                        mongo_aggr_arg[0]['$match'] = {'chrom': src_chrom,
                                                                                                       'start': {'$lt': src_end},
                                                                                                       'end': {'$gt': src_start}}
                                                else:
                                                        mongo_aggr_arg[0]['$match'] = {self.ann_field_path: def_data_type(src_row[self.ann_col_index])}
                                                mongo_aggr_arg[1]['$addFields'] = dict(zip(src_col_names, src_row))
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
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#аннотирования. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nAnnotation by {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.dock, main.src_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
