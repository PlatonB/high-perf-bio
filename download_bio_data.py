__version__ = 'v1.0'

def download_bio_data(args, data_name, cat_url, regexp):
        '''
        Создание подпапки для скачиваемых данных выбранного проекта. Сбор ссылок на файлы
        с этими данными из соответствующего FTP-каталога. Подсчёт оптимального количества
        параллельно качаемых файлов. Многопроцессовое скачивание с подсчётом времени.
        '''
        prep_single_proc = PrepSingleProc(args)
        prep_single_proc.trg_dir_path = os.path.join(prep_single_proc.trg_top_dir_path,
                                                     data_name)
        trg_dir_path = prep_single_proc.trg_dir_path
        if args.redownload:
                shutil.rmtree(trg_dir_path,
                              ignore_errors=True)
        os.mkdir(trg_dir_path)
        with requests.get(cat_url, stream=True) as r:
                src_file_urls = []
                for line in r.iter_lines(decode_unicode=True):
                        try:
                                src_file_name = re.search(regexp, line).group()
                        except AttributeError:
                                pass
                        else:
                                src_file_urls.append(os.path.join(cat_url,
                                                                  src_file_name))
        max_proc_quan = args.max_proc_quan
        src_files_quan = len(src_file_urls)
        if max_proc_quan > src_files_quan <= 8:
                proc_quan = src_files_quan
        elif max_proc_quan > 8:
                proc_quan = 8
        else:
                proc_quan = max_proc_quan
                
        print(f'\nDownloading {data_name} data')
        print(f'\tnumber of parallel processes: {proc_quan}')
        
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(prep_single_proc.download, src_file_urls)
                exec_time = datetime.datetime.now() - exec_time_start
                
        print(f'\tparallel execution time: {exec_time}')
        
def add_args(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, позволяющая скачивать данные
известных биоинформатических ресурсов.

Версия: {ver}
Требуемые сторонние компоненты: requests
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

От версии к версии буду добавлять новые источники. Пишите в Issues,
какими публичными биоинформатическими данными больше всего пользуетесь.

Условные обозначения в справке по CLI:
[значение по умолчанию]
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-T', '--trg-top-dir-path', required=True, metavar='str', dest='trg_top_dir_path', type=str,
                             help='Путь к папке для скачиваемых данных')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-r', '--redownload', dest='redownload', action='store_true',
                             help='Заменить ранее скачанные данные в случае возникновения конфликта')
        opt_grp.add_argument('--ciseqtls-gtex', dest='ciseqtls_gtex', action='store_true',
                             help='Скачать сильные пары вариант-ген (cis-eQTL, GTEx)')
        opt_grp.add_argument('--vars-1000g', dest='vars_1000g', action='store_true',
                             help='Скачать варианты 1000 Genomes (NYGC)')
        opt_grp.add_argument('--vars-dbsnp', dest='vars_dbsnp', action='store_true',
                             help='Скачать варианты dbSNP (Ensembl)')
        opt_grp.add_argument('-i', '--download-indexes', dest='download_indexes', action='store_true',
                             help='Скачать tbi/csi-индексы, если они предоставлены')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно качаемых файлов')
        args = arg_parser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный под безопасное
        параллельное скачивание данных.
        '''
        def __init__(self, args):
                '''
                Единственным атрибутом, объявляемым в __init__,
                будет путь к папке, в которую планируется размещать
                папки для данных конкретных ресурсов. Атрибуты с
                путями к последним будут добавляться тоже потом -
                при работе с экземплярами описываемого класса.
                '''
                self.trg_top_dir_path = os.path.normpath(args.trg_top_dir_path)
                
        def download(self, url):
                '''
                Функция скачивания одного файла.
                Размер фрагмента - 128 МБ.
                '''
                with requests.Session() as s:
                        with s.get(url, stream=True) as r:
                                trg_file_path = os.path.join(self.trg_dir_path,
                                                             url.split('/')[-1])
                                with open(trg_file_path, 'wb') as trg_file_opened:
                                        for chunk in r.iter_content(chunk_size=134217728):
                                                trg_file_opened.write(chunk)
                                                
####################################################################################################

import os, shutil, datetime, requests, re
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool

#CLI.
args = add_args(__version__)

#cis-eQTL-таблицы проекта GTEx. Поставляются
#в виде единого tar-архива, из-за чего
#необходимо нудное копошение с путями.
if args.ciseqtls_gtex:
        ciseqtls_gtex_data_name = 'Cis_eQTLs_signif_GTEx'
        ciseqtls_gtex_arc_url = 'https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar'
        prep_single_proc = PrepSingleProc(args)
        prep_single_proc.trg_dir_path = os.path.join(prep_single_proc.trg_top_dir_path,
                                                     ciseqtls_gtex_data_name)
        trg_dir_path = prep_single_proc.trg_dir_path
        if args.redownload:
                shutil.rmtree(trg_dir_path,
                              ignore_errors=True)
        os.mkdir(trg_dir_path)
        print(f'\nDownloading {ciseqtls_gtex_data_name} data')
        exec_time_start = datetime.datetime.now()
        prep_single_proc.download(ciseqtls_gtex_arc_url)
        exec_time = datetime.datetime.now() - exec_time_start
        print(f'\texecution time: {exec_time}')
        ciseqtls_gtex_arc_path = os.path.join(trg_dir_path,
                                              os.listdir(trg_dir_path)[0])
        shutil.unpack_archive(ciseqtls_gtex_arc_path,
                              trg_dir_path)
        os.remove(ciseqtls_gtex_arc_path)
        trg_sub_dir_path = ciseqtls_gtex_arc_path[:-4]
        for trg_file_name in os.listdir(trg_sub_dir_path):
                if 'signif' in trg_file_name:
                        os.rename(os.path.join(trg_sub_dir_path,
                                               trg_file_name),
                                  os.path.join(trg_dir_path,
                                               trg_file_name))
        shutil.rmtree(trg_sub_dir_path)
        
#Коллсеты 1000 Genomes, сформированные с
#нуля Нью-Йоркским Центром Генома в 2020 году.
if args.vars_1000g:
        vars_1000g_data_name = 'Variants_1000_Genomes'
        vars_1000g_cat_url = 'http://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/working/20201028_3202_phased/'
        if not args.download_indexes:
                vars_1000g_regexp = r'CCDG\S+\.gz(?=")'
        else:
                vars_1000g_regexp = r'CCDG\S+\.gz(?:\.tbi)*(?=")'
        download_bio_data(args,
                          vars_1000g_data_name,
                          vars_1000g_cat_url,
                          vars_1000g_regexp)
        
#Версия dbSNP, размещённая проектом
#Ensembl. Отличается от апстрима, как
#минимум, классической номенклатурой имён
#хромосом и похромосомным разбиением.
if args.vars_dbsnp:
        vars_dbsnp_data_name = 'Variants_dbSNP'
        vars_dbsnp_cat_url = 'http://ftp.ensembl.org/pub/release-103/variation/vcf/homo_sapiens/'
        if not args.download_indexes:
                vars_dbsnp_regexp = r'(?<=>)homo_sapiens-chr\S+\.gz(?=<)'
        else:
                vars_dbsnp_regexp = r'(?<=>)homo_sapiens-chr\S+\.gz(?:\.csi)*(?=<)'
        download_bio_data(args,
                          vars_dbsnp_data_name,
                          vars_dbsnp_cat_url,
                          vars_dbsnp_regexp)
