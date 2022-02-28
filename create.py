__version__ = 'v6.0'

import sys, locale, os, re, datetime, gzip
sys.dont_write_bytecode = True
from cli.create_cli import add_args_ru, add_args_en
from pymongo import MongoClient, ASCENDING, IndexModel
from multiprocessing import Pool
from backend.common_errors import DifFmtsError
from backend.resolve_db_existence import resolve_db_existence
from backend.def_data_type import def_data_type

class NoDataToUploadError(Exception):
        '''
        Это исключение предусмотрено, в основном, на случай,
        если исследователь выбрал дозапись, но соответствующие
        всем исходным файлам коллекции уже наличествуют.
        '''
        def __init__(self):
                err_msg = f'\nSource files are absent or already uploaded'
                super().__init__(err_msg)
                
def process_chrom_cell(chrom_cell):
        '''
        Функция стандартизации обозначений хромосом. HGVS-идентификаторы
        (кроме NW_*) преобразуются в человеко-понятные имена хромосом.
        Если исходные обозначения - обычные, то очищаются от приставки chr.
        '''
        if 'NC' in chrom_cell:
                chrom_num_repr = int(chrom_cell.split('.')[0][3:])
                if chrom_num_repr in range(1, 23):
                        chrom = chrom_num_repr
                elif chrom_num_repr == 23:
                        chrom = 'X'
                elif chrom_num_repr == 24:
                        chrom = 'Y'
                else:
                        chrom = 'MT'
        else:
                chrom = def_data_type(chrom_cell.replace('chr', ''))
        return chrom

def process_info_cell(info_cell):
        '''
        Функция преобразования ячейки INFO-столбца VCF-таблицы в список
        из словаря и списка. У INFO есть такая особенность: одни элементы
        представляют собой пары ключ-значение, другие (далее - флаги)
        идут без ключей. Пары разместятся в словарь, а флаги - в список,
        расположенный на одинаковом со словарём уровне. Если у одного ключа
        несколько значений, программа положит их в подсписок. Поскольку
        официальные рекомендации по составу INFO-столбца не являются
        строгими, тип данных каждого элемента приходится подбирать.
        '''
        info_row, info_obj = info_cell.split(';'), [{}, []]
        for info_subcell in info_row:
                if '=' in info_subcell:
                        pair = info_subcell.split('=')
                        if ',' in pair[1]:
                                pair[1] = [def_data_type(val) for val in pair[1].split(',')]
                        else:
                                pair[1] = def_data_type(pair[1])
                        info_obj[0][pair[0]] = pair[1]
                else:
                        info_obj[1].append(def_data_type(info_subcell))
        return info_obj

def process_gt_cell(format_cell, gt_cell):
        '''
        Функция объединения ячеек FORMAT- и GT-столбца VCF-таблицы
        в словарь. Из-за непредсказуемости состава GT-столбца для
        каждого значения тип данных будет определяться подбором.
        '''
        format_row, gt_row, gt_two_dim = format_cell.split(':'), gt_cell.split(':'), []
        for gt_subcell in gt_row:
                if ',' in gt_subcell:
                        gt_two_dim.append([def_data_type(val) for val in gt_subcell.split(',')])
                else:
                        gt_two_dim.append(def_data_type(gt_subcell))
        gt_obj = dict(zip(format_row, gt_two_dim))
        return gt_obj

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
                случае не должны будут потом в параллельных процессах изменяться.
                Из набора имён заливаемых файлов исключаются имена возможных
                сопутствующих tbi/csi-индексов. Если исследователь не позволил
                производить дозапись, то, в случае существования БД с тем же именем,
                что и у создаваемой, вызывается функция разрешения конфликта.
                '''
                client = MongoClient()
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = set(filter(lambda src_file_name: re.search(r'\.tbi|\.csi',
                                                                                 src_file_name) is None,
                                                 os.listdir(self.src_dir_path)))
                src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                        self.src_file_names))
                if len(src_file_fmts) > 1:
                        raise DifFmtsError(src_file_fmts)
                self.src_file_fmt = list(src_file_fmts)[0]
                if args.trg_db_name is None:
                        self.trg_db_name = os.path.basename(self.src_dir_path)
                else:
                        self.trg_db_name = args.trg_db_name
                if not args.append:
                        resolve_db_existence(self.trg_db_name)
                else:
                        self.src_file_names -= set(map(lambda cur_coll_name: cur_coll_name + '.gz',
                                                       client[self.trg_db_name].list_collection_names()))
                if len(self.src_file_names) == 0:
                        raise NoDataToUploadError()
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
                self.minimal = args.minimal
                if args.sec_delimiter is None:
                        self.sec_delimiter = args.sec_delimiter
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'low_line':
                        self.sec_delimiter = '_'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                self.max_fragment_len = args.max_fragment_len
                if args.ind_field_groups is None:
                        if self.src_file_fmt == 'vcf':
                                self.index_models = [IndexModel([('#CHROM', ASCENDING),
                                                                 ('POS', ASCENDING)]),
                                                     IndexModel([('ID', ASCENDING)])]
                        elif self.src_file_fmt == 'bed':
                                self.index_models = [IndexModel([('chrom', ASCENDING),
                                                                 ('start', ASCENDING),
                                                                 ('end', ASCENDING)]),
                                                     IndexModel([('name', ASCENDING)])]
                else:
                        self.index_models = [IndexModel([(ind_field_path, ASCENDING) for ind_field_path in ind_field_group.split('+')]) \
                                             for ind_field_group in args.ind_field_groups.split(',')]
                self.ver = ver
                client.close()
                
        def create_collection(self, src_file_name):
                '''
                Функция создания и наполнения одной
                MongoDB-коллекции данными одного файла,
                а также индексации полей этой коллекции.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                trg_db_obj = client[self.trg_db_name]
                
                #Открытие исходной архивированной таблицы на чтение.
                with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
                        
                        #Комментирующие символы строк метаинформации (далее - метастрок) VCF - всегда ##.
                        #Для других, более вольных, форматов число метастрок поступает от исследователя.
                        #Метастроки добавляются в объект, становящийся в недалёком будущем первым документом
                        #коллекции. После метастрок, по-хорошему, должна следовать шапка, но во многих
                        #BED-файлах её нет. Для BED пришлось вручную вписывать в код референсную шапку.
                        meta_lines = {'meta': []}
                        if self.src_file_fmt == 'vcf':
                                for line in src_file_opened:
                                        if line.startswith('##'):
                                                meta_lines['meta'].append(line.rstrip())
                                        else:
                                                header_row = line.rstrip().split('\t')
                                                if len(header_row) > 8:
                                                        del header_row[8]
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        meta_lines['meta'].append(src_file_opened.readline().rstrip())
                                if self.src_file_fmt == 'bed':
                                        header_row = ['chrom', 'start', 'end', 'name',
                                                      'score', 'strand', 'thickStart', 'thickEnd',
                                                      'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
                                else:
                                        header_row = src_file_opened.readline().rstrip().split('\t')
                        meta_lines['meta'].append(f'##tool_name=<{os.path.basename(__file__)[:-3]},{self.ver}>')
                        
                        #Создание коллекции. Для оптимального соотношения
                        #скорости записи/извлечения с объёмом хранимых данных,
                        #я выбрал в качестве алгоритма сжатия Zstandard.
                        trg_coll_obj = trg_db_obj.create_collection(src_file_name[:-3],
                                                                    storageEngine={'wiredTiger':
                                                                                   {'configString':
                                                                                    'block_compressor=zstd'}})
                        
                        #Добавление в новоиспечённую
                        #коллекцию объекта с метастроками.
                        trg_coll_obj.insert_one(meta_lines)
                        
                        #Данные будут поступать в коллекцию
                        #базы одним или более фрагментами.
                        #Для контроля работы с фрагментами
                        #далее будет отмеряться их размер.
                        #Стартовое значение размера - 0 строк.
                        fragment, fragment_len = [], 0
                        
                        #Коллекция БД будет пополняться
                        #до тех пор, пока не закончится
                        #перебор строк исходной таблицы.
                        for line in src_file_opened:
                                
                                #Преобразование очередной строки
                                #исходной таблицы в список.
                                row = line.rstrip().split('\t')
                                
                                #MongoDB позволяет размещать в одну коллекцию документы с переменным
                                #количеством полей и разными типами данных значений. Воспользуемся такой
                                #гибкостью СУБД, создавая структуры, максимально заточенные под содержимое
                                #конкретной исходной строки. VCF и BED обрабатываются полностью автоматически:
                                #значениям определённых столбцов присваиваются типы данных int и decimal,
                                #где-то производится разбивка на списки, HGVS-IDs переделываются в имена
                                #хромосом, а INFO- и GT-ячейки конвертируются в многослойные структуры.
                                #Для кастомных табличных форматов типы данных определяются подбором
                                #по принципу "подходит - не подходит", а разбиение на списки делается
                                #при наличии в ячейке обозначенного исследователем разделителя.
                                if self.src_file_fmt == 'vcf':
                                        row[0] = process_chrom_cell(row[0])
                                        row[1] = int(row[1])
                                        if ';' in row[2]:
                                                row[2] = row[2].split(';')
                                        if ',' in row[4]:
                                                row[4] = row[4].split(',')
                                        row[5] = def_data_type(row[5])
                                        row[7] = process_info_cell(row[7])
                                        if self.minimal:
                                                row = row[:8]
                                        elif len(row) > 8:
                                                gt_objs = [process_gt_cell(row[8], gt_cell) for gt_cell in row[9:]]
                                                row = row[:8] + gt_objs
                                elif self.src_file_fmt == 'bed':
                                        row[0] = process_chrom_cell(row[0])
                                        row[1], row[2] = int(row[1]), int(row[2])
                                        if self.minimal:
                                                row = row[:3]
                                        elif len(row) > 4:
                                                row[4] = int(row[4])
                                                if len(row) > 6:
                                                        row[6], row[7], row[9] = int(row[6]), int(row[7]), int(row[9])
                                                        row[10] = list(map(int, row[10].rstrip(',').split(',')))
                                                        row[11] = list(map(int, row[11].rstrip(',').split(',')))
                                else:
                                        for cell_index in range(len(row)):
                                                if self.sec_delimiter is not None and self.sec_delimiter in row[cell_index]:
                                                        row[cell_index] = row[cell_index].split(self.sec_delimiter)
                                                        for subcell_index in range(len(row[cell_index])):
                                                                row[cell_index][subcell_index] = def_data_type(row[cell_index][subcell_index])
                                                else:
                                                        row[cell_index] = def_data_type(row[cell_index])
                                                        
                                #MongoDB - документоориентированная СУБД.
                                #Каждая коллекция строится из т.н. документов,
                                #Python-предшественниками которых могут быть
                                #только словари. Поэтому для подготовки размещаемого
                                #в базу фрагмента сшиваем из списка элементов
                                #шапки и списка, созданного из очередной строки,
                                #словарь, затем добавляем его в список таких словарей.
                                #Набор ключей любого словаря может получиться меньшим,
                                #чем шапка. Есть две возможные причины срезания: 1.
                                #Если реальный BED отстаёт по количеству элементов
                                #от ранее подготовленной стандартной 12-элементной
                                #шапки; 2. Когда по инициативе исследователя в коллекцию
                                #направляются лишь основополагающие столбцы VCF или BED.
                                fragment.append(dict(zip(header_row[:len(row)], row)))
                                
                                #Сразу после пополнения
                                #фрагмента регистрируем это
                                #событие с помощью счётчика.
                                fragment_len += 1
                                
                                #Исходная таблица ещё не до конца
                                #считалась, а фрагмент достиг порогового
                                #значения количества строк. Тогда
                                #прописываем фрагмент в коллекцию,
                                #очищаем его и обнуляем счётчик.
                                if fragment_len == self.max_fragment_len:
                                        trg_coll_obj.insert_many(fragment)
                                        fragment.clear()
                                        fragment_len = 0
                                        
                #Чтение исходной таблицы
                #завершилось, но остался
                #непрописанный фрагмент.
                #Исправляем ситуацию.
                if fragment_len > 0:
                        trg_coll_obj.insert_many(fragment)
                        
                #Создание дефолтных или пользовательских индексов.
                if hasattr(self, 'index_models'):
                        trg_coll_obj.create_indexes(self.index_models)
                        
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск конвертации
#таблиц в коллекции. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args, __version__)
        proc_quan = main.proc_quan
        print(f'\nReplenishment and indexing {main.trg_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.create_collection, main.src_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
