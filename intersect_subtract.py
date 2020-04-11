__version__ = 'V3.1'

def add_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, выполняющая пересечение или
вычитание коллекций по выбранному полю.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Версия: {__version__}
Лицензия: GNU General Public License version 3
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Перед запуском программы нужно установить
MongoDB и PyMongo (см. README).

Программу можно применять только для
баз, созданных с помощью create_db.

--------------------------------------------------

Пересечение.

Указанное поле *каждой* левой коллекции пересекается
с одноимённым полем *всех* правых коллекций.

Как работает настройка глубины пересечения?
*Остаются* только те значения поля левой коллекции,
для которых *есть совпадение* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром глубины.

--------------------------------------------------

Вычитание.

Из указанного поля *каждой* левой коллекции
вычитается одноимённое поле *всех* правых коллекций.

Как работает настройка глубины вычитания?
*Остаются* только те значения поля левой коллекции,
для которых *нет совпадения* в соответствующем
поле, как минимум, того количества правых
коллекций, что задано параметром глубины.

--------------------------------------------------

О глубине простым языком.

Больше глубина - меньше результатов.

--------------------------------------------------

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-D', '--db-name', metavar='str', dest='db_name', type=str,
                               help='Имя БД, по которой выполнять работу')
        argparser.add_argument('-T', '--trg-dir-path', metavar='str', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов')
        argparser.add_argument('-l', '--left-coll-names', metavar='[None]', dest='left_coll_names', type=str,
                               help='Имена "левых" коллекций (через запятую без пробела; по умолчанию "левыми" считаются все коллекции БД)')
        argparser.add_argument('-r', '--right-coll-names', metavar='[None]', dest='right_coll_names', type=str,
                               help='Имена "правых" коллекций (через запятую без пробела; по умолчанию "правыми" считаются все коллекции БД)')
        argparser.add_argument('-f', '--field-name', metavar='[None]', dest='field_name', type=str,
                               help='Имя поля, по которому пересекать или вычитать (trg-VCF: ID по умолчанию; trg-BED: name по умолчанию; trg-TSV: ID по умолчанию)')
        argparser.add_argument('-a', '--action', metavar='[intersect]', choices=['intersect', 'subtract'], default='intersect', dest='action', type=str,
                               help='{intersect, subtract} Пересекать или вычитать')
        argparser.add_argument('-d', '--depth', metavar='[1]', default=1, dest='depth', type=int,
                               help='Глубина (см. описание этой опции выше)')
        argparser.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['comma', 'semicolon', 'colon', 'pipe'], default='comma', dest='sec_delimiter', type=str,
                               help='{comma, semicolon, colon, pipe} Знак препинания для восстановления ячейки из списка (trg-VCF, trg-BED: опция не применяется)')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно обрабатываемых "левых" коллекций')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под
        безопасную параллельную агрегацию
        набора коллекций MongoDB.
        '''
        def __init__(self, args, coll_names):
                '''
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции пересечения или
                вычитания по обозначенному полю.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.db_name = args.db_name
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                if args.left_coll_names == None:
                        self.left_coll_names = sorted(coll_names)
                else:
                        self.left_coll_names = sorted(args.left_coll_names.split(','))
                if args.right_coll_names == None:
                        self.right_coll_names = sorted(coll_names)
                else:
                        self.right_coll_names = sorted(args.right_coll_names.split(','))
                if len(set(self.right_coll_names) & set(self.left_coll_names)) == 0:
                        self.right_colls_quan = len(self.right_coll_names)
                else:
                        self.right_colls_quan = len(self.right_coll_names) - 1
                self.field_name = args.field_name
                self.action = args.action
                if self.action == 'intersect':
                        self.sign = '&'
                elif self.action == 'subtract':
                        self.sign = '-'
                if args.depth >= self.right_colls_quan:
                        self.depth = self.right_colls_quan
                elif args.depth < 1:
                        self.depth = 1
                else:
                        self.depth = args.depth
                if args.sec_delimiter == 'comma':
                        self.sec_delimiter = ','
                elif args.sec_delimiter == 'semicolon':
                        self.sec_delimiter = ';'
                elif args.sec_delimiter == 'colon':
                        self.sec_delimiter = ':'
                elif args.sec_delimiter == 'pipe':
                        self.sec_delimiter = '|'
                        
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
                
                #Предотвращение возможной попытки
                #агрегации коллекции самой с собой.
                right_coll_names = [right_coll_name for right_coll_name in self.right_coll_names if right_coll_name != left_coll_name]
                
                #В имени левой коллекции предусмотрительно
                #сохранено расширение того файла, по
                #которому эта коллекция создавалась.
                #Расширение поможет далее принять
                #решение о необходимости коррекции
                #шапки, задать обрабатываемое по
                #умолчанию поле, понять, что делать
                #с сортировкой, а также определить,
                #как форматировать конечные строки.
                trg_file_format = left_coll_name.split('.')[-1]
                
                #Чтобы шапка повторяла шапку
                #той таблицы, по которой делалась
                #коллекция, создадим её из имён полей.
                #Сугубо техническое поле _id проигнорируется.
                #Если в ex-VCF-коллекции есть поля с генотипами,
                #то шапка дополнится элементом FORMAT.
                header_row = list(left_coll_obj.find_one())[1:]
                if trg_file_format == 'vcf' and len(header_row) > 8:
                        header_row.insert(8, 'FORMAT')
                header_line = '\t'.join(header_row)
                
                #Исследователь может не выбрать
                #поле, по которому пересекать/вычитать.
                #Поскольку программа заточена в первую
                #очередь под генетику, дефолтным
                #полем будет считаться rsID-содержащее.
                if self.field_name == None:
                        if trg_file_format == 'vcf':
                                field_name = 'ID'
                        elif trg_file_format == 'bed':
                                field_name = 'name'
                        else:
                                field_name = 'ID'
                else:
                        field_name = self.field_name
                        
                #В результате планируемого выполнения
                #этой инструкции левая коллекция
                #объединяется со всеми правыми.
                #Каждый документ, получающийся в
                #результате объединения, содержит:
                #1. все поля документа левой коллекции;
                #2. поля с соответствиями из правых
                #коллекций (далее - результирующие).
                #Если для элемента выбранного поля
                #данного левого документа не нашлось
                #совпадений в одной из правых коллекций,
                #то в результирующем поле появится
                #пустой список (Python-представление
                #Null-значения из мира баз данных).
                #Если же совпадение имеется, то в
                #качестве значения результирующего
                #поля возвратится список с содержимым
                #соответствующего правого документа.
                #Если выявилось совпадение с несколькими
                #документами какой-то одной правой
                #коллекции, то они в полном составе
                #поступят в результирующее поле.
                #Из всего описанного следует, что
                #фильтровать потом результаты
                #пересечения или вычитания можно
                #будет по количеству пустых или
                #непустых результирующих списков.
                #Теперь про повторы: в конечный
                #файл направятся все дубли элемента,
                #находящиеся в пределах поля левой
                #коллекции, но от повторов
                #правых элементов копийность
                #результатов не зависит.
                pipeline = [{'$lookup': {'from': right_coll_name,
                                         'localField': field_name,
                                         'foreignField': field_name,
                                         'as': right_coll_name.replace('.', '_')}} for right_coll_name in right_coll_names]
                
                #Таблицы биоинформатических
                #форматов нужно сортировать
                #по хромосомам и позициям.
                #Задаём правило сортировки
                #будущего VCF или BED на
                #уровне aggregation-пайплайна.
                #Оператор сортировки должен
                #располагаться в начале пайплайна,
                #иначе потом при её выполнении
                #может проигнорироваться индекс,
                #что опасно появлением ошибки
                #"Sort exceeded memory limit".
                if trg_file_format == 'vcf':
                        pipeline.insert(0, {"$sort": SON([('#CHROM', ASCENDING),
                                                          ('POS', ASCENDING)])})
                elif trg_file_format == 'bed':
                        pipeline.insert(0, {"$sort": SON([('chrom', ASCENDING),
                                                          ('start', ASCENDING),
                                                          ('end', ASCENDING)])})
                        
                #Конструируем имя конечного файла
                #и абсолютный путь к этому файлу.
                #Начинаться имя будет с действия,
                #выбранного исследователем.
                #Происхождение имени файла
                #от имени левой коллекции
                #будет указывать на то, что
                #все данные, попадающие в файл,
                #берутся исключительно из неё.
                #Правые коллекции пригождаются
                #только для фильтрации левых.
                trg_file_name = f'{self.action[:3]}_res_{left_coll_name}'
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                
                #Открытие конечного файла на запись.
                with open(trg_file_path, 'w') as trg_file_opened:
                        
                        #Формируем и прописываем
                        #метастроки, повествующие о
                        #происхождении конечного файла.
                        #Прописываем также табличную шапку.
                        trg_file_opened.write(f'##Database: {self.db_name}\n')
                        trg_file_opened.write(f'##Collections: {left_coll_name} {self.sign} {", ".join(right_coll_names)}\n')
                        trg_file_opened.write(f'##Field: {field_name}\n')
                        trg_file_opened.write(f'##Depth: {self.depth}\n')
                        trg_file_opened.write(header_line + '\n')
                        
                        #Выполняем подробно описанный
                        #выше пайплайн из левостороннего
                        #объединения и, возможно, ещё
                        #сортировки, ему предшествующей.
                        curs_obj = left_coll_obj.aggregate(pipeline)
                        
                        #Создаём флаг, по которому далее будет
                        #определено, оказались ли в конечном
                        #файле строки, отличные от хэдеров.
                        empty_res = True
                        
                        #Правила фильтрации результатов
                        #левостороннего объединения должны
                        #были быть заданы исследователем.
                        #Первый фильтр - само действие -
                        #пересечение или вычитание.
                        #Судьба левого документа будет
                        #определяться непустыми результирующими
                        #списками при пересечении и
                        #пустыми в случае вычитания.
                        #Второй фильтр - глубина: левый
                        #документ получит приглашение
                        #в конечный файл, только если
                        #будет достигнут порог количества
                        #непустых/пустых результирующих списков.
                        for doc in curs_obj:
                                depth_meter = 0
                                for right_coll_name in right_coll_names:
                                        right_coll_alias = right_coll_name.replace('.', '_')
                                        if (self.action == 'intersect' and doc[right_coll_alias] != []) or \
                                           (self.action == 'subtract' and doc[right_coll_alias] == []):
                                                depth_meter += 1
                                        del doc[right_coll_alias]
                                if depth_meter >= self.depth:
                                        trg_file_opened.write(restore_line(doc, trg_file_format, self.sec_delimiter))
                                        empty_res = False
                                        
                #Если флаг-индикатор так и
                #остался равен True, значит,
                #результатов пересечения/вычитания
                #для данной левой коллекции нет, и в
                #конечный файл попали только хэдеры.
                #Такие конечные файлы программа удалит.
                if empty_res:
                        os.remove(trg_file_path)
                        
                client.close()
                
####################################################################################################

import sys, os

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
args, client = add_args(), MongoClient()
coll_names = client[args.db_name].list_collection_names()
if len(coll_names) < 2:
        print('''Для пересечения или вычитания
требуется не менее двух коллекций''')
        sys.exit()
prep_single_proc = PrepSingleProc(args, coll_names)
max_proc_quan = args.max_proc_quan
left_coll_names = prep_single_proc.left_coll_names
left_colls_quan = len(left_coll_names)
if max_proc_quan > left_colls_quan <= 8:
        proc_quan = left_colls_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nРабота с БД {prep_single_proc.db_name}')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Параллельный запуск поиска.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.intersect_subtract, left_coll_names)
