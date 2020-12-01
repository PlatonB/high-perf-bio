__version__ = 'v4.0'

class ByLocTsvError(Exception):
        '''
        В исследуемом TSV или основанной на TSV коллекции
        может не быть геномных координат. Ну или бывает,
        когда координатные столбцы располагаются, где
        попало. Поэтому нельзя, чтобы хоть в одном
        из этих двух мест был вольноформатный дух.
        '''
        def __init__(self):
                err_msg = 'Intersection by location is not possible for src-TSV or db-TSV'
                super().__init__(err_msg)
                
def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Аннотируемый столбец:
- должен занимать одно и то же
положение во всех исходных таблицах;
- целиком размещается в оперативную память,
что может замедлить работу компьютера.

Также в качестве эксперимента существует
возможность пересечения по координатам.
Поддерживаются все 4 комбинации VCF и BED.

Каждая аннотируемая таблица
обязана быть сжатой с помощью GZIP.

Источником характеристик должна быть
база данных, созданная с помощью create_db.

Чтобы программа работала быстро, нужны
индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-S', '--arc-dir-path', metavar='str', dest='arc_dir_path', type=str,
                               help='Путь к папке со сжатыми аннотируемыми таблицами')
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой аннотировать')
        argparser.add_argument('-t', '--trg-top-dir-path', metavar='[None]', dest='trg_top_dir_path', type=str,
                               help='Путь к папке для результатов ([путь к исходной папке])')
        argparser.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                               help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: следует включать шапку)')
        argparser.add_argument('-l', '--by-loc', dest='by_loc', action='store_true',
                               help='Пересекать по геномной локации (экспериментальная фича; src-TSV, db-TSV: не применяется)')
        argparser.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                               help='Номер аннотируемого столбца (применяется без -l; src-VCF: [3]; src-BED: [4]; src-TSV: [1])')
        argparser.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                               help='Имя поля БД, по которому аннотировать (применяется без -l; db-VCF: [ID]; db-BED: [name]; db-TSV: [rsID])')
        argparser.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                               help='Отбираемые поля (через запятую без пробела; db-VCF: не применяется; db-BED: получится TSV; поле _id не выведется')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (db-VCF, db-BED (trg-BED): не применяется)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно аннотируемых таблиц')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный
        под безопасное параллельное
        аннотирование набора таблиц.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной
                под многопроцессовое выполнение функции
                пересечения данных исходного файла с данными
                из базы. Атрибуты должны быть созданы единожды
                и далее ни в коем случае не изменяться. Получаются
                они в основном из указанных исследователем опций.
                '''
                self.arc_dir_path = os.path.normpath(args.arc_dir_path)
                self.db_name = args.db_name
                if args.trg_top_dir_path is None:
                        self.trg_top_dir_path = self.arc_dir_path
                else:
                        self.trg_top_dir_path = os.path.normpath(args.trg_top_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                self.by_loc = args.by_loc
                if args.ann_col_num is None:
                        self.ann_col_index = args.ann_col_num
                else:
                        self.ann_col_index = args.ann_col_num - 1
                self.ann_field_name = args.ann_field_name
                if args.proj_fields is None:
                        self.pymongo_project = args.proj_fields
                else:
                        self.pymongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        if '_id' in self.pymongo_project:
                                del self.pymongo_project['_id']
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                self.ver = ver
                
        def annotate(self, arc_file_name):
                '''
                Функция пересечения одной таблицы со
                всеми коллекциями БД по заданной паре
                столбец-поле или по позиции в геноме.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                db_obj = client[self.db_name]
                
                #Аннотирование предполагается
                #проводить по всем коллекциям
                #базы, поэтому потребуется
                #полный набор имён коллекций.
                coll_names = db_obj.list_collection_names()
                
                #Получение названия и расширения исходной таблицы, а также
                #единого расширения тех файлов, по которым создавались коллекции.
                #Последнее предусмотрительно сохранено в имени каждой коллекции.
                #Как пригодятся расширения? Увидим в следующем блоке кода.
                src_file_base, src_file_format = arc_file_name.rsplit('.', maxsplit=2)[:-1]
                coll_name_ext = coll_names[0].rsplit('.', maxsplit=1)[1]
                
                #В случае аннотирования по произвольному полю от форматов зависит выбор
                #пересекаемого поля по умолчанию. Как объяснить используемые в программе
                #дефолты? У VCF и BED наиболее важные для исследователя идентификаторы
                #обычно хранятся в ID и name, соответственно. С TSV ситуация драматичнее.
                #Популярная задача - обогатить характеристиками голый список rsIDs, поэтому
                #особое внимание программы - к нулевому столбцу src-TSV. Со стороны db-TSV
                #программа будет надеяться увидеть поле rsID, т.к. одноимённый столбец то
                #и дело мелькает в нестандартно отформатированных таблицах аннотаций коротких
                #вариантов. Теперь немного о режиме пересечения по хромосоме и позиции. Он опасен
                #для src- и db-TSV, т.к. вероятность угадать расположение этих столбцов или полей
                #невелика. Поэтому, наткнувшись на TSV или TSV-based коллекцию, программа предпочтёт
                #скопытиться. В этом блоке кода ещё под шумок составляются каркасы будущих запросов.
                if self.by_loc:
                        if src_file_format not in ['vcf', 'bed'] or coll_name_ext not in ['vcf', 'bed']:
                                raise ByLocTsvError()
                        pymongo_find_args = [{'$or': []}]
                else:
                        if self.ann_col_index is None:
                                if src_file_format == 'vcf':
                                        ann_col_index = 2
                                elif src_file_format == 'bed':
                                        ann_col_index = 3
                                else:
                                        ann_col_index = 0
                        else:
                                ann_col_index = self.ann_col_index
                        if self.ann_field_name is None:
                                if coll_name_ext == 'vcf':
                                        ann_field_name = 'ID'
                                elif coll_name_ext == 'bed':
                                        ann_field_name = 'name'
                                else:
                                        ann_field_name = 'rsID'
                        else:
                                ann_field_name = self.ann_field_name
                        pymongo_find_args = [{ann_field_name: {'$in': []}}]
                        
                #Открытие исходной архивированной таблицы на чтение, смещение курсора к её основной части.
                with gzip.open(os.path.join(self.arc_dir_path, arc_file_name), mode='rt') as arc_file_opened:
                        if src_file_format == 'vcf':
                                for line in arc_file_opened:
                                        if not line.startswith('##'):
                                                break
                        else:
                                for meta_line_index in range(self.meta_lines_quan):
                                        arc_file_opened.readline()
                                        
                        #Пополняем список, служащий основой будущего запроса. Для координатных вычислений
                        #предусматриваем структуры запроса под все 4 возможных сочетания форматов VCF и BED.
                        #Несмотря на угрозу перерасхода RAM, программа кладёт в один запрос сразу всё, что
                        #отобрано из исходного файла. Если запрашивать по одному элементу, то непонятно,
                        #как сортировать конечные данные. Неспособным на внешнюю сортировку питоновским
                        #sorted? А вдруг на запрос откликнется неприлично много документов?.. При создании
                        #БД для каждого значения устанавливался оптимальный тип данных. При работе с MongoDB
                        #важно соблюдать соответствие типа данных запрашиваемого слова и размещённых в базе
                        #значений. Для этого присвоим подходящий тип данных каждому аннотируемому элементу.
                        for line in arc_file_opened:
                                row = line.rstrip().split('\t')
                                if self.by_loc:
                                        if src_file_format == 'vcf':
                                                chrom, pos = def_data_type(row[0].replace('chr', '')), int(row[1])
                                                if coll_name_ext == 'vcf':
                                                        pymongo_find_args[0]['$or'].append({'#CHROM': chrom,
                                                                                            'POS': pos})
                                                elif coll_name_ext == 'bed':
                                                        pymongo_find_args[0]['$or'].append({'chrom': chrom,
                                                                                            'start': {'$lt': pos},
                                                                                            'end': {'$gte': pos}})
                                        elif src_file_format == 'bed':
                                                chrom, start, end = def_data_type(row[0].replace('chr', '')), int(row[1]), int(row[2])
                                                if coll_name_ext == 'vcf':
                                                        pymongo_find_args[0]['$or'].append({'#CHROM': chrom,
                                                                                            'POS': {'$gt': start,
                                                                                                    '$lte': end}})
                                                elif coll_name_ext == 'bed':
                                                        pymongo_find_args[0]['$or'].append({'chrom': chrom,
                                                                                            'start': {'$lt': end},
                                                                                            'end': {'$gt': start}})
                                else:
                                        pymongo_find_args[0][ann_field_name]['$in'].append(def_data_type(row[ann_col_index]))
                                        
                #Project в MongoDB - это условие, отбирающее данные лишь
                #определённых полей коллекции. PyMongo-методы find и find_one
                #принимают его отдельным от запроса аргументом. Когда мы
                #оставляем только часть полей db-BED, невозможно гарантировать
                #соблюдение спецификаций BED-формата, поэтому вывод будет
                #формироваться не более, чем просто табулированным (trg-TSV).
                if self.pymongo_project is None or coll_name_ext == 'vcf':
                        trg_file_format = coll_name_ext
                else:
                        pymongo_find_args.append(self.pymongo_project)
                        trg_file_format = 'tsv'
                        
                #Конкатенация пути к подпапке для результатов
                #работы над текущим исходным файлом.
                trg_dir_path = os.path.join(self.trg_top_dir_path,
                                            f'{src_file_base}_ann')
                
                #Обработка каждой исходной
                #таблицы производится по всем
                #коллекциям MongoDB-базы. Т.е.
                #даже, если по одной из коллекций
                #уже получились результаты, обход
                #будет продолжаться и завершится лишь
                #после обращения к последней коллекции.
                for coll_name in coll_names:
                        
                        #Создание двух объектов: текущей коллекции
                        #и курсора. В аннотируемый список затесались
                        #одинаковые элементы? Ничего страшного - СУБД
                        #подготовит результат только для одного из них.
                        coll_obj = db_obj[coll_name]
                        curs_obj = coll_obj.find(*pymongo_find_args)
                        
                        #Таблицы биоинформатических форматов
                        #нужно сортировать по хромосомам и позициям.
                        #Задаём правило сортировки будущего VCF
                        #или BED на уровне MongoDB-курсора.
                        if trg_file_format == 'vcf':
                                curs_obj.sort([('#CHROM', ASCENDING),
                                               ('POS', ASCENDING)])
                        elif trg_file_format == 'bed':
                                curs_obj.sort([('chrom', ASCENDING),
                                               ('start', ASCENDING),
                                               ('end', ASCENDING)])
                                
                        #Чтобы шапка повторяла шапку той таблицы, по которой
                        #делалась коллекция, создадим её из имён полей. Имя сугубо
                        #технического поля _id проигнорируется. Если в db-VCF есть
                        #поля с генотипами, то шапка дополнится элементом FORMAT.
                        try:
                                header_row = list(coll_obj.find_one(*pymongo_find_args))[1:]
                        except TypeError:
                                continue
                        if trg_file_format == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Создание конечной подпапки. Не факт,
                        #что она просуществует до окончания
                        #выполнения программы, т.к. за отсутствие
                        #в ней результатов полагается удаление.
                        if not os.path.exists(trg_dir_path):
                                os.mkdir(trg_dir_path)
                                
                        #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                        coll_name_base = coll_name.rsplit('.', maxsplit=1)[0]
                        trg_file_name = f'{src_file_base}_ann_by_{coll_name_base}.{trg_file_format}'
                        trg_file_path = os.path.join(trg_dir_path, trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##table={arc_file_name}\n')
                                trg_file_opened.write(f'##database={self.db_name}\n')
                                trg_file_opened.write(f'##collection={coll_name}\n')
                                if not self.by_loc:
                                        trg_file_opened.write(f'##field={ann_field_name}\n')
                                if self.pymongo_project is not None and trg_file_format != 'vcf':
                                        trg_file_opened.write(f'##project={self.pymongo_project}\n')
                                trg_file_opened.write(header_line + '\n')
                                
                                #Извлечение из объекта курсора отвечающих запросу
                                #документов, преобразование их значений в обычные
                                #строки и прописывание последних в конечный файл.
                                #Проверка, вылез ли по запросу хоть один документ.
                                empty_res = True
                                for doc in curs_obj:
                                        trg_file_opened.write(restore_line(doc,
                                                                           trg_file_format,
                                                                           self.sec_delimiter))
                                        empty_res = False
                                        
                        #Удаление конечного файла, если в
                        #нём очутились только метастроки.
                        if empty_res:
                                os.remove(trg_file_path)
                                
                #Дисконнект.
                client.close()
                
                #Удаление оставшейся пустой подпапки.
                try:
                        os.rmdir(trg_dir_path)
                except OSError:
                        pass
                
####################################################################################################

import sys, os, datetime, gzip

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool
from pymongo import MongoClient, ASCENDING
from backend.def_data_type import def_data_type
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#аннотируемых файлов, определение
#оптимального числа процессов.
args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args, __version__)
arc_file_names = os.listdir(prep_single_proc.arc_dir_path)
arc_files_quan = len(arc_file_names)
if max_proc_quan > arc_files_quan <= 8:
        proc_quan = arc_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nAnnotation by {prep_single_proc.db_name} DB')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск аннотирования. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.annotate, arc_file_names)
        exec_time = (datetime.datetime.now() - exec_time_start)
        
print(f'\tparallel computation time: {exec_time}')
