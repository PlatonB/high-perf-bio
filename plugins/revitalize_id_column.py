__version__ = 'v3.1'

import sys, os, locale, datetime, gzip
sys.dont_write_bytecode = True
if __name__ == '__main__':
        sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                     'cli'))
        sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                     'backend'))
        from revitalize_id_column_cli import add_args_ru, add_args_en
else:
        sys.path.append(os.path.join(os.getcwd(),
                                     'backend'))
from pymongo import MongoClient, ASCENDING
from common_errors import DifFmtsError
from multiprocessing import Pool
from def_data_type import def_data_type

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
        def __init__(self, args):
                '''
                Получение атрибутов как для основной функции программы,
                так и для блока многопроцессового запуска таковой.
                Первые из перечисленных ни в коем случае не должны
                будут потом в параллельных процессах изменяться.
                Здесь же - в инициализирующем методе - выставлены
                несколько ограничений, обусловленных узкой специализицией
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
                max_proc_quan = args.max_proc_quan
                src_files_quan = len(self.src_file_names)
                cpus_quan = os.cpu_count()
                if max_proc_quan > src_files_quan <= cpus_quan:
                        self.proc_quan = src_files_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                self.ignore_unrev_lines = args.ignore_unrev_lines
                client.close()
                
        def revitalize(self, src_file_name):
                '''
                Функция заделывания столбца ID rs-идентификаторами.
                Расскажу о неочевидных моментах. Набор MongoDB-объектов
                должен быть строго индивидуальным для каждого процесса,
                иначе возможны конфликты. Дальше речь пойдёт о, собственно,
                получении и верификации rsIDs. Поиск MongoDB-документа с
                rs-идентификатором делается по хромосоме, позиции и всем
                аллелям. Вариант как из src-VCF, так и из src-db-VCF, может
                оказаться мультиаллельным. В одних VCF альтернативные аллели
                перечислены через запятую, в других - разнесены по разным
                строкам, что абсолютно легально. src-db-VCF-эквивалент
                записи через запятую - массив (список). О подготовке этих
                списков заботится отвественный за создание БД компонент
                high-perf-bio. Поиск обособленного альтернативного аллеля
                по ALT-полю коллекции не требует каких-то плясок с бубном:
                MongoDB автоматически чекает вхождение запрашиваемого
                слова в каждый существующий в поле список. Если ALT-аллели
                src-VCF оформлены через запятую, то программа их дробит
                на списки и задействует функциональность MongoDB искать
                точное соответствие целых списков. Сочетание неразбитых
                ALT src-VCF с разбитыми в src-db-VCF не поддерживается,
                но и вряд ли встретится в реальной жизни.
                '''
                client = MongoClient()
                src_coll_obj = client[self.src_db_name][self.src_coll_name]
                src_file_base = src_file_name.rsplit('.', maxsplit=2)[0]
                src_coll_base = self.src_coll_name.rsplit('.', maxsplit=1)[0]
                trg_file_name = f'file-{src_file_base}__coll-{src_coll_base}.vcf.gz'
                with gzip.open(os.path.join(self.src_dir_path, src_file_name), mode='rt') as src_file_opened:
                        with gzip.open(os.path.join(self.trg_dir_path, trg_file_name), mode='wt') as trg_file_opened:
                                for src_line in src_file_opened:
                                        trg_file_opened.write(src_line)
                                        if not src_line.startswith('##'):
                                                break
                                for src_line in src_file_opened:
                                        src_row = src_line.split('\t')
                                        src_chrom = def_data_type(src_row[0].replace('chr', ''))
                                        src_pos = int(src_row[1])
                                        src_ref = src_row[3]
                                        if ',' not in src_row[4]:
                                                src_alt = src_row[4]
                                        else:
                                                src_alt = src_row[4].split(',')
                                        doc = src_coll_obj.find_one({'#CHROM': src_chrom,
                                                                     'POS': src_pos,
                                                                     'REF': src_ref,
                                                                     'ALT': src_alt})
                                        if doc is not None:
                                                src_row[2] = doc['ID']
                                                trg_file_opened.write('\t'.join(src_row))
                                        elif not self.ignore_unrev_lines:
                                                trg_file_opened.write(src_line)
                                                
                #Дисконнект.
                client.close()
                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#реабилитации ID. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args)
        proc_quan = main.proc_quan
        print(f'\nID column reconstruction by {main.src_db_name} DB')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.revitalize, main.src_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
