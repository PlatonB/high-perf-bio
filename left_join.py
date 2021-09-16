__version__ = 'v8.1'

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
                
class ByLocTsvError(Exception):
        '''
        В основанной на TSV коллекции может не быть
        геномных координат. Ну или бывает, когда
        координатные поля располагаются, где попало.
        Поэтому нельзя, чтобы при джойне по локации
        среди коллекций витал вольноформатный дух.
        '''
        def __init__(self):
                err_msg = '''\nIntersection or subtraction by
location is not possible for src-db-TSV'''
                super().__init__(err_msg)
                
class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасную параллельную агрегацию
        набора коллекций MongoDB.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной под многопроцессовое
                выполнение функции левостороннего объединения коллекций с последующей
                фильтрацией результатов. Атрибуты ни в коем случае не должны будут
                потом в параллельных процессах изменяться. Получаются они в основном
                из указанных исследователем аргументов. Некоторые неочевидные, но
                важные детали об атрибутах. Квази-расширение коллекций. Оно нужно,
                как минимум, для определения правил сортировки и форматирования
                конечных файлов. Сортировка. Её лучше ставить в начало пайплайна:
                она тогда задействует индекс сама и не отберает эту возможность у
                других стадий. Набор правых коллекций. На этапе создания атрибутов
                он не окончательный. Впоследствии в каждом потоке отбирается свой
                список правых, не поглядывающих налево. Пересекаемое/вычитаемое
                поле по-умолчанию. Оно подобрано на основании здравого смысла. К
                примеру, вряд ли придёт в голову пересекать VCF по полю, отличному
                от ID. Проджекшен (отбор полей). Для src-db-VCF его крайне трудно
                реализовать из-за наличия в соответствующих коллекциях разнообразных
                вложенных структур и запрета со стороны MongoDB на применение точечной
                формы обращения к отбираемым элементам массивов. Что касается
                src-db-BED, когда мы оставляем только часть полей, невозможно
                гарантировать соблюдение спецификаций BED-формата, поэтому вывод
                будет формироваться не более, чем просто табулированным (trg-TSV).
                '''
                client = MongoClient()
                self.src_db_name = args.src_db_name
                self.src_coll_names = client[self.src_db_name].list_collection_names()
                if len(self.src_coll_names) < 2:
                        raise NotEnoughCollsError()
                self.src_coll_ext = self.src_coll_names[0].rsplit('.', maxsplit=1)[1]
                if self.src_coll_ext == 'vcf':
                        self.mongo_aggr_draft = [{'$sort': SON([('#CHROM', ASCENDING),
                                                                ('POS', ASCENDING)])}]
                elif self.src_coll_ext == 'bed':
                        self.mongo_aggr_draft = [{'$sort': SON([('chrom', ASCENDING),
                                                                ('start', ASCENDING),
                                                                ('end', ASCENDING)])}]
                else:
                        self.mongo_aggr_draft = []
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                if args.left_coll_names is None:
                        self.left_coll_names = set(self.src_coll_names)
                else:
                        self.left_coll_names = set(args.left_coll_names.split(','))
                if args.right_coll_names is None:
                        self.right_coll_names = set(self.src_coll_names)
                else:
                        self.right_coll_names = set(args.right_coll_names.split(','))
                right_colls_quan = len(self.right_coll_names)
                self.by_loc = args.by_loc
                if self.by_loc:
                        if self.src_coll_ext not in ['vcf', 'bed']:
                                raise ByLocTsvError()
                elif args.field_name is None:
                        if self.src_coll_ext == 'vcf':
                                self.field_name = 'ID'
                        elif self.src_coll_ext == 'bed':
                                self.field_name = 'name'
                        else:
                                self.field_name = 'rsID'
                else:
                        self.field_name = args.field_name
                self.action = args.action
                if args.coverage == 0 or args.coverage > right_colls_quan:
                        self.coverage = right_colls_quan
                else:
                        self.coverage = args.coverage
                if len(self.right_coll_names & self.left_coll_names) > 0 \
                   and 1 < self.coverage == right_colls_quan:
                        self.coverage -= 1
                if args.proj_fields is None or self.src_coll_ext == 'vcf':
                        self.mongo_findone_args = [None, None]
                        self.trg_file_fmt = self.src_coll_ext
                else:
                        mongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                        self.mongo_findone_args = [None, mongo_project]
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
                self.ver = ver
                client.close()
                
        def intersect_subtract(self, left_coll_name):
                '''
                Функция агрегации одной левой
                коллекции со всеми правыми и
                отбора полученных результатов
                в соответствии с задачей.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_db_obj = client[self.src_db_name]
                left_coll_obj = src_db_obj[left_coll_name]
                
                #Дальнейшее построение пайплайна будет вестись
                #в пределах каждого процесса по-своему.
                mongo_aggr_arg = copy.deepcopy(self.mongo_aggr_draft)
                
                #Предотвращение возможной попытки агрегации коллекции самой с собой. Сортировка имён правых
                #коллекций для большей читабельности их списка в метастроке будущего конечного файла.
                right_coll_names = sorted(filter(lambda right_coll_name: right_coll_name != left_coll_name,
                                                 self.right_coll_names))
                
                #Если правая коллекция лишь одна
                #и при этом совпадает с текущей
                #левой, то процесс будет прерван.
                if right_coll_names != []:
                        
                        #Дефолтное либо кастомное поле, по которому потом выполнится left join, утверждено ещё
                        #при инициализации атрибутов. В этом блоке кода находят своё место 3 возможных запроса.
                        #Механизм пересечения и вычитания через левосторонее объединение я красочно описал в ридми.
                        #Небольшая памятка: в let назначаются правые переменные, а сослаться на них можно через $$.
                        if self.by_loc:
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
                        else:
                                mongo_aggr_arg += [{'$lookup': {'from': right_coll_name,
                                                                'localField': self.field_name,
                                                                'foreignField': self.field_name,
                                                                'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                                
                        #Выполняем пайплайн из сортировки (для src-db-VCF и src-db-BED)
                        #и левостороннего объединения. Проджекшен, если запрошен
                        #исследователем, будет потом организован отдельно -
                        #на этапе Python-фильтрации объединённых документов.
                        curs_obj = left_coll_obj.aggregate(mongo_aggr_arg)
                        
                        #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                        #коллекция, создадим её из имён полей. Projection при этом учтём.
                        #Имя сугубо технического поля _id проигнорируется. Если в src-db-VCF
                        #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                        header_row = list(left_coll_obj.find_one(*self.mongo_findone_args))[1:]
                        if self.trg_file_fmt == 'vcf' and len(header_row) > 8:
                                header_row.insert(8, 'FORMAT')
                        header_line = '\t'.join(header_row)
                        
                        #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                        #Происхождение имени файла от имени левой коллекции будет указывать на
                        #то, что все данные, попадающие в файл, берутся исключительно из неё.
                        left_coll_base = left_coll_name.rsplit('.', maxsplit=1)[0]
                        trg_file_name = f'{left_coll_base}_{self.action[:3]}_res_c_{self.coverage}.{self.trg_file_fmt}'
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        
                        #Открытие конечного файла на запись.
                        with open(trg_file_path, 'w') as trg_file_opened:
                                
                                #Формируем и прописываем метастроки,
                                #повествующие о происхождении конечного
                                #файла. Прописываем также табличную шапку.
                                if self.trg_file_fmt == 'vcf':
                                        trg_file_opened.write(f'##fileformat={self.trg_file_fmt.upper()}\n')
                                trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                                trg_file_opened.write(f'##database={self.src_db_name}\n')
                                trg_file_opened.write(f'##leftCollection={left_coll_name}\n')
                                trg_file_opened.write(f'##rightCollections=<{",".join(right_coll_names)}>\n')
                                if not self.by_loc:
                                        trg_file_opened.write(f'##field={self.field_name}\n')
                                trg_file_opened.write(f'##action={self.action}\n')
                                trg_file_opened.write(f'##coverage={self.coverage}\n')
                                if self.mongo_findone_args[1] is not None:
                                        trg_file_opened.write(f'##project={self.mongo_findone_args[1]}\n')
                                trg_file_opened.write(header_line + '\n')
                                
                                #Создаём флаг, по которому далее будет
                                #определено, оказались ли в конечном
                                #файле строки, отличные от хэдеров.
                                empty_res = True
                                
                                #Правила фильтрации результатов левостороннего внешнего
                                #объединения должны были быть заданы исследователем. Первый
                                #фильтр - само действие - пересечение или вычитание: судьба
                                #левого документа будет определяться непустыми результирующими
                                #списками при пересечении и пустыми в случае вычитания. Второй
                                #фильтр - охват: левый документ получит приглашение в конечный
                                #файл, только если будет достигнут порог количества непустых/пустых
                                #результирующих списков. Подробности - в readme проекта. Проджекшн
                                #реализован здесь же в кустарно-питоновском виде. Было бы ошибочно
                                #навешивать его на aggregation pipeline, т.к. это спровоцировало бы
                                #конфликт как раз сейчас - при фильтрации объединённых документов.
                                for doc in curs_obj:
                                        cov_meter = 0
                                        for right_coll_name in right_coll_names:
                                                right_coll_alias = right_coll_name.replace('.', '_')
                                                if (self.action == 'intersect' and doc[right_coll_alias] != []) or \
                                                   (self.action == 'subtract' and doc[right_coll_alias] == []):
                                                        cov_meter += 1
                                                del doc[right_coll_alias]
                                        if cov_meter >= self.coverage:
                                                if self.mongo_findone_args[1] is not None:
                                                        for field_name in list(doc):
                                                                if field_name not in self.mongo_findone_args[1]:
                                                                        del doc[field_name]
                                                trg_file_opened.write(restore_line(doc,
                                                                                   self.trg_file_fmt,
                                                                                   self.sec_delimiter))
                                                empty_res = False
                                                
                        #Если флаг-индикатор так и
                        #остался равен True, значит,
                        #результатов пересечения/вычитания
                        #для данной левой коллекции нет, и в
                        #конечный файл попали только хэдеры.
                        #Такие конечные файлы программа удалит.
                        if empty_res:
                                os.remove(trg_file_path)
                                
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, locale, datetime, os, copy
sys.dont_write_bytecode = True
from cli.left_join_cli import add_args_ru
from pymongo import MongoClient, ASCENDING
from multiprocessing import Pool
from bson.son import SON
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка аргументов командной
#строки, создание экземпляра содержащего ключевую
#функцию класса, получение имён и определение количества
#левых коллекций, расчёт оптимального числа процессов.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
prep_single_proc = PrepSingleProc(args,
                                  __version__)
max_proc_quan = args.max_proc_quan
left_coll_names = prep_single_proc.left_coll_names
left_colls_quan = len(left_coll_names)
if max_proc_quan > left_colls_quan <= 8:
        proc_quan = left_colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\n{prep_single_proc.action}ing collections of {prep_single_proc.src_db_name} database')
print(f'\tcoverage: {prep_single_proc.coverage}')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск агрегации + фильтрации.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.intersect_subtract,
                     left_coll_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
