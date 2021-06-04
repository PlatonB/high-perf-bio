__version__ = 'v1.1'

def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, добавляющая rsIDs в столбец ID VCF-файлов.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Восстанавливаемыми файлами должны быть только сжатые VCF, а БД может содержать лишь одну
коллекцию: созданную с помощью create_db по VCF и содержащую rsIDs в поле ID. Рекомендуемый
источник данных для этой коллекции - NCBI dbSNP VCF с координатами подходящей сборки.

Чтобы программа работала быстро, нужны индексы полей #CHROM, POS, REF и ALT.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - модифицируемые таблицы определённого формата;
scr-db-FMT - исходная БД с коллекциями, соответствующими
по структуре таблицам определённого формата
''',
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми исходными таблицами (src-BED, src-TSV: не поддерживаются')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, содержащей коллекцию с (rs)ID-полем (src-db-BED, src-db-TSV: не поддерживаются)')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно обогащаемых таблиц')
        args = arg_parser.parse_args()
        return args

class DifFmtsError(Exception):
        '''
        Исходные таблицы могут быть только одноформатными.
        '''
        def __init__(self, src_file_fmts):
                err_msg = f'\nSource files are in different formats: {src_file_fmts}'
                super().__init__(err_msg)
                
class NotVcfError(Exception):
        '''
        Предотвращение попытки работать с
        данными отличных от VCF форматов.
        '''
        def __init__(self, src_fmt):
                err_msg = f'\nSource file or collection is in {src_fmt} format instead of VCF'
                super().__init__(err_msg)
                
class MoreThanOneCollError(Exception):
        '''
        rsIDs можно черпать только из одиночной коллекции.
        '''
        def __init__(self, src_colls_quan):
                err_msg = f'\nThere are {src_colls_quan} collections in the DB, but it must be 1'
                super().__init__(err_msg)
                
class PrepSingleProc():
        '''
        Класс, спроектированный под безопасное
        параллельное восстановление набора VCF.
        '''
        def __init__(self, args, ver):
                '''
                Получение атрибутов, необходимых заточенной под
                многопроцессовое выполнение функции наводнения
                ID-столбца rs-идентификаторами. Атрибуты ни в
                коем случае не должны будут потом в параллельных
                процессах изменяться. Получаются они в основном
                из указанных исследователем аргументов. Здесь же -
                в инициализирующем методе - выставлены несколько
                ограничений, обусловленных узкой специализицией
                программы. К примеру, произойдёт аварийное завершение,
                если на вход будет подан файл в не-VCF формате.
                '''
                client = MongoClient()
                self.src_dir_path = os.path.normpath(args.src_dir_path)
                self.src_file_names = os.listdir(self.src_dir_path)
                src_file_fmts = set(map(lambda src_file_name: src_file_name.rsplit('.', maxsplit=2)[1],
                                        self.src_file_names))
                if len(src_file_fmts) > 1:
                        raise DifFmtsError(src_file_fmts)
                src_file_fmt = list(src_file_fmts)[0]
                if src_file_fmt != 'vcf':
                        raise NotVcfError(src_file_fmt)
                self.src_db_name = args.src_db_name
                src_coll_names = client[self.src_db_name].list_collection_names()
                src_colls_quan = len(src_coll_names)
                if src_colls_quan > 1:
                        raise MoreThanOneCollError(src_colls_quan)
                self.src_coll_name = src_coll_names[0]
                src_coll_ext = self.src_coll_name.rsplit('.', maxsplit=1)[1]
                if src_coll_ext != 'vcf':
                        raise NotVcfError(src_coll_ext)
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                client.close()
                
        def revitalize(self, src_file_name):
                '''
                Функция заделывания столбца
                ID rs-идентификаторами.
                '''
                
                #Набор MongoDB-объектов
                #должен быть строго
                #индивидуальным для
                #каждого процесса, иначе
                #возможны конфликты.
                client = MongoClient()
                src_coll_obj = client[self.src_db_name][self.src_coll_name]
                
                #Поиск MongoDB-документа с rs-идентификатором делается по хромосоме, позиции и всем аллелям.
                #Вариант как из src-VCF, так и из src-db-VCF, может оказаться мультиаллельным. В одних VCF
                #альтернативные аллели перечислены через запятую, в других - разнесены по разным строкам, что
                #абсолютно легально. src-db-VCF-эквивалент записи через запятую - список. О подготовке этих
                #списков заботится отвественный за создание БД компонент high-perf-bio. Поиск обособленного
                #альтернативного аллеля по ALT-полю коллекции не требует каких-то плясок с бубном: MongoDB
                #автоматически чекает вхождение запрашиваемого слова в каждый существующий в поле список. Если
                #ALT-аллели src-VCF оформлены через запятую, то программа их дробит на списки и задействует
                #функциональность MongoDB искать точное соответствие целых списков. Сочетание неразбитых ALT
                #src-VCF с разбитыми в src-db-VCF не поддерживается, но и вряд ли встретится в реальной жизни.
                with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
                        src_file_base = src_file_name.rsplit('.', maxsplit=2)[0]
                        trg_file_name = f'{src_file_base}_rsIDs.vcf.gz'
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        with gzip.open(trg_file_path, mode='wt') as trg_file_opened:
                                for line in src_file_opened:
                                        trg_file_opened.write(line)
                                        if not line.startswith('##'):
                                                break
                                for line in src_file_opened:
                                        row = line.split('\t')
                                        chrom = def_data_type(row[0].replace('chr', ''))
                                        pos = int(row[1])
                                        ref = row[3]
                                        if ',' not in row[4]:
                                                alt = row[4]
                                        else:
                                                alt = row[4].split(',')
                                        doc = src_coll_obj.find_one({'#CHROM': chrom,
                                                                     'POS': pos,
                                                                     'REF': ref,
                                                                     'ALT': alt})
                                        if doc is not None:
                                                row[2] = doc['ID']
                                                trg_file_opened.write('\t'.join(row))
                                        else:
                                                trg_file_opened.write(line)
                                                
                #Дисконнект.
                client.close()
                
####################################################################################################

import sys, os, datetime, gzip
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'backend'))
from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient, ASCENDING
from multiprocessing import Pool
from def_data_type import def_data_type

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение имён и количества
#реставрируемых файлов, определение
#оптимального числа процессов.
args = add_args(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args,
                                  __version__)
src_file_names = prep_single_proc.src_file_names
src_files_quan = len(src_file_names)
if max_proc_quan > src_files_quan <= 8:
        proc_quan = src_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nID column reconstruction by {prep_single_proc.src_db_name} database')
print(f'\tnumber of parallel processes: {proc_quan}')

#Параллельный запуск реабилитации ID. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.revitalize, src_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
