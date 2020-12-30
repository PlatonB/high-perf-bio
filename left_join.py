__version__ = 'v6.0'

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
                
def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, выполняющая пересечение
или вычитание коллекций по выбранному
полю или по геномным координатам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Программу можно применять только для
баз, созданных с помощью create_db.

--------------------------------------------------

Вам необходимо условно отнести интересующие
коллекции к категориям "левые" и "правые".

Левые: в конечные файлы попадают
данные только из них. Одна левая
коллекция - один файл с результатами.

Правые: нужны для фильтрации левых.

--------------------------------------------------

Пересечение по одному полю.

Указанное поле *каждой* левой коллекции пересекается
с одноимённым полем *всех* правых коллекций.

Как работает настройка охвата пересечения?
*Остаются* только те значения поля левой коллекции,
для которых *есть совпадение* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром охвата.

--------------------------------------------------

Вычитание по одному полю.

Из указанного поля *каждой* левой коллекции
вычитается одноимённое поле *всех* правых коллекций.

Как работает настройка охвата вычитания?
*Остаются* только те значения поля левой коллекции,
для которых *нет совпадения* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром охвата.

--------------------------------------------------

Об охвате простым языком.

Больше охват - меньше результатов.

--------------------------------------------------

Пересечение и вычитание по геномной локации.
- актуальны все написанные выше разъяснения,
касающиеся работы с единичным полем;
- db-BED: стартовая координата каждого
интервала - 0-based, т.е. равна
истинному номеру нуклеотида минус 1;
- db-BED: левые интервалы попадают
в результаты в неизменном виде.
- db-BED: баг - неприлично низкая
скорость вычислений (Issue #7);

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию на этапе парсинга аргументов;
- в двойных квадратных скобках - конкретизированное значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой выполнять работу')
        argparser.add_argument('-T', '--trg-dir-path', metavar='str', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов')
        argparser.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                               help='Имена левых коллекций (через запятую без пробела; [[все коллекции БД]])')
        argparser.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                               help='Имена правых коллекций (через запятую без пробела; [[все коллекции БД]]; правая, совпадающая с текущей левой, проигнорируется)')
        argparser.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                               help='Пересекать или вычитать по геномной локации (экспериментальная фича; db-TSV: не применяется)')
        argparser.add_argument('-f', '--field-name', metavar='[None]', dest='field_name', type=str,
                               help='Имя поля, по которому пересекать или вычитать (применяется без -n; db-VCF: [[ID]]; db-BED: [[name]], db-TSV: [[rsID]])')
        argparser.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                               help='{intersect, subtract} Пересекать или вычитать')
        argparser.add_argument('-c', '--coverage', metavar='[1]', default=1, dest='coverage', type=int,
                               help='Охват (1 <= c <= количество правых коллекций; 0 - приравнять к количеству правых; уменьшится на 1 при любом совпадении правых и левых)')
        argparser.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                               help='Отбираемые поля (через запятую без пробела; db-VCF: не применяется; db-BED: trg-TSV; поле _id не выведется)')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (db-VCF, db-BED (trg-BED): не применяется)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно обрабатываемых левых коллекций')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасную параллельную агрегацию
        набора коллекций MongoDB.
        '''
        def __init__(self, args, coll_names, ver):
                '''
                Получение атрибутов, необходимых заточенной под
                многопроцессовое выполнение функции левостороннего
                объединения коллекций с последующей фильтрацией
                результатов. Атрибуты должны быть созданы единожды
                и далее ни в коем случае не изменяться. Получаются
                они в основном из указанных исследователем опций.
                '''
                self.db_name = args.db_name
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                if args.left_coll_names is None:
                        self.left_coll_names = set(coll_names)
                else:
                        self.left_coll_names = set(args.left_coll_names.split(','))
                if args.right_coll_names is None:
                        self.right_coll_names = set(coll_names)
                else:
                        self.right_coll_names = set(args.right_coll_names.split(','))
                if len(self.right_coll_names & self.left_coll_names) == 0:
                        right_colls_quan = len(self.right_coll_names)
                else:
                        right_colls_quan = len(self.right_coll_names) - 1
                self.by_loc = args.by_loc
                self.field_name = args.field_name
                self.action = args.action
                if args.coverage == 0 or args.coverage > right_colls_quan:
                        self.coverage = right_colls_quan
                else:
                        self.coverage = args.coverage
                if args.proj_fields is None:
                        self.pymongo_project = args.proj_fields
                else:
                        self.pymongo_project = {field_name: 1 for field_name in args.proj_fields.split(',')}
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                self.ver = ver
                
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
                db_obj = client[self.db_name]
                left_coll_obj = db_obj[left_coll_name]
                
                #Предотвращение возможной попытки агрегации коллекции самой с собой. Сортировка имён правых
                #коллекций для большей читабельности их списка в метастроке будущего конечного файла.
                right_coll_names = sorted(filter(lambda right_coll_name: right_coll_name != left_coll_name,
                                                 self.right_coll_names))
                
                #В имени коллекции предусмотрительно сохранено расширение того
                #файла, по которому коллекция создавалась. Расширение поможет далее
                #разобраться, как минимум, с сортировкой и форматированием результатов.
                left_coll_name_base, coll_name_ext = left_coll_name.rsplit('.', maxsplit=1)
                
                #В этом блоке кода окончательно утверждаются дефолтные либо кастомные поля, по которым
                #потом выполнится left join. Также тут находят своё место, собственно, запросы. В качестве
                #умолчаний взяты имена максимально значимых для биоинформатиков полей соответствующих
                #форматов. Подробнее о дефолтах - в комментарии к аналогичному коду программы annotate.
                #Механизм пересечения и вычитания через левосторонее объединение я красочно описал в ридми.
                #Небольшая памятка: в let назначаются правые переменные, а сослаться на них можно через $$.
                if self.by_loc:
                        if coll_name_ext == 'vcf':
                                pipeline = [{'$lookup': {'from': right_coll_name,
                                                         'let': {'chrom': '$#CHROM', 'pos': '$POS'},
                                                         'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$#CHROM', '$$chrom']},
                                                                                                     {'$eq': ['$POS', '$$pos']}]}}}],
                                                         'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                        elif coll_name_ext == 'bed':
                                pipeline = [{'$lookup': {'from': right_coll_name,
                                                         'let': {'chrom': '$chrom', 'start': '$start', 'end': '$end'},
                                                         'pipeline': [{'$match': {'$expr': {'$and': [{'$eq': ['$chrom', '$$chrom']},
                                                                                                     {'$lt': ['$start', '$$end']},
                                                                                                     {'$gt': ['$end', '$$start']}]}}}],
                                                         'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                        else:
                                sys.exit()
                else:
                        if self.field_name is None:
                                if coll_name_ext == 'vcf':
                                        field_name = 'ID'
                                elif coll_name_ext == 'bed':
                                        field_name = 'name'
                                else:
                                        field_name = 'rsID'
                        else:
                                field_name = self.field_name
                        pipeline = [{'$lookup': {'from': right_coll_name,
                                                 'localField': field_name,
                                                 'foreignField': field_name,
                                                 'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                        
                #Таблицы биоинформатических форматов нужно сортировать
                #по хромосомам и позициям. Задаём правило сортировки
                #будущего VCF или BED на уровне aggregation-пайплайна.
                #Стадия сортировки должна идти после всех стадий
                #объединения, иначе при выполнении джойнов будет
                #игнорироваться индекс, что в случае работы с более
                #менее крупными данными уничтожит производительность.
                if coll_name_ext == 'vcf':
                        pipeline.append({"$sort": SON([('#CHROM', ASCENDING),
                                                       ('POS', ASCENDING)])})
                elif coll_name_ext == 'bed':
                        pipeline.append({"$sort": SON([('chrom', ASCENDING),
                                                       ('start', ASCENDING),
                                                       ('end', ASCENDING)])})
                        
                #Project в MongoDB - это условие, отбирающее данные лишь определённых
                #полей коллекции. В данном случае его было бы ошибочно навешивать на
                #aggregation pipeline, т.к. возникнет конфликт на этапе Python-фильтрации
                #результатов. В описываемом блоке от наличия правила отбора полей может
                #поменяться конечный файловый формат. Когда мы оставляем только часть полей
                #db-BED, невозможно гарантировать соблюдение спецификаций BED-формата, поэтому
                #вывод будет формироваться не более, чем просто табулированным (trg-TSV).
                if self.pymongo_project is None or coll_name_ext == 'vcf':
                        trg_file_format = coll_name_ext
                else:
                        trg_file_format = 'tsv'
                        
                #Чтобы шапка повторяла шапку той таблицы, по которой делалась
                #коллекция, создадим её из имён полей. Projection при этом учтём.
                #Имя сугубо технического поля _id проигнорируется. Если в db-VCF
                #есть поля с генотипами, то шапка дополнится элементом FORMAT.
                header_row = list(left_coll_obj.find_one({}, self.pymongo_project))[1:]
                if trg_file_format == 'vcf' and len(header_row) > 8:
                        header_row.insert(8, 'FORMAT')
                header_line = '\t'.join(header_row)
                
                #Конструируем имя конечного файла и абсолютный путь к этому файлу.
                #Происхождение имени файла от имени левой коллекции будет указывать на
                #то, что все данные, попадающие в файл, берутся исключительно из неё.
                trg_file_name = f'{left_coll_name_base}_{self.action[:3]}_res_c_{self.coverage}.{trg_file_format}'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                
                #Открытие конечного файла на запись.
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Формируем и прописываем метастроки,
                        #повествующие о происхождении конечного
                        #файла. Прописываем также табличную шапку.
                        trg_file_opened.write(f'##tool=<{os.path.basename(__file__)[:-3]},{self.ver}>\n')
                        trg_file_opened.write(f'##database={self.db_name}\n')
                        trg_file_opened.write(f'##leftCollection={left_coll_name}\n')
                        trg_file_opened.write(f'##rightCollections=<{",".join(right_coll_names)}>\n')
                        if not self.by_loc:
                                trg_file_opened.write(f'##field={field_name}\n')
                        trg_file_opened.write(f'##action={self.action}\n')
                        trg_file_opened.write(f'##coverage={self.coverage}\n')
                        if self.pymongo_project is not None and trg_file_format != 'vcf':
                                trg_file_opened.write(f'##project={self.pymongo_project}\n')
                        trg_file_opened.write(header_line + '\n')
                        
                        #Выполняем описанный выше пайплайн из левостороннего
                        #объединения и (для BED/VCF-форматов) сортировки. MongoDB
                        #не может использовать индекс для оптимизации сортировки
                        #уже объединённых документов. Поэтому во избежание превышения
                        #лимита RAM разрешаем СУБД применять внешнюю сортировку.
                        curs_obj = left_coll_obj.aggregate(pipeline, allowDiskUse=True)
                        
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
                        #результирующих списков. Опять же, подробности - в readme проекта.
                        for doc in curs_obj:
                                cov_meter = 0
                                for right_coll_name in right_coll_names:
                                        right_coll_alias = right_coll_name.replace('.', '_')
                                        if (self.action == 'intersect' and doc[right_coll_alias] != []) or \
                                           (self.action == 'subtract' and doc[right_coll_alias] == []):
                                                cov_meter += 1
                                        del doc[right_coll_alias]
                                if cov_meter >= self.coverage:
                                        if self.pymongo_project is not None and trg_file_format != 'vcf':
                                                for field_name in list(doc):
                                                        if field_name not in self.pymongo_project:
                                                                del doc[field_name]
                                        trg_file_opened.write(restore_line(doc,
                                                                           trg_file_format,
                                                                           self.sec_delimiter))
                                        empty_res = False
                                        
                #Дисконнект.
                client.close()
                
                #Если флаг-индикатор так и
                #остался равен True, значит,
                #результатов пересечения/вычитания
                #для данной левой коллекции нет, и в
                #конечный файл попали только хэдеры.
                #Такие конечные файлы программа удалит.
                if empty_res:
                        os.remove(trg_file_path)
                        
####################################################################################################

import sys, datetime, os

#Подавление формирования питоновского кэша с
#целью предотвращения искажения результатов.
sys.dont_write_bytecode = True

from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from multiprocessing import Pool
from bson.son import SON
from backend.doc_to_line import restore_line

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса, получение
#имён всех и только левых коллекций,
#определение количества левых коллекций
#и оптимального числа процессов.
args, client = add_args(__version__), MongoClient()
coll_names = client[args.db_name].list_collection_names()
if len(coll_names) < 2:
        raise NotEnoughCollsError()
prep_single_proc = PrepSingleProc(args,
                                  coll_names,
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
        
print(f'\n{args.action}ing collections of {args.db_name} database')
print(f'\tcoverage: {prep_single_proc.coverage}')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск поиска.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.intersect_subtract,
                     left_coll_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
