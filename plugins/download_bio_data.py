__version__ = 'v2.1'

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
                Размер фрагмента - 100 МБ.
                '''
                with requests.Session() as s:
                        with s.get(url, stream=True) as r:
                                trg_file_path = os.path.join(self.trg_dir_path,
                                                             url.split('/')[-1])
                                with open(trg_file_path, 'wb') as trg_file_opened:
                                        for chunk in r.iter_content(chunk_size=100000000):
                                                trg_file_opened.write(chunk)
                                                
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
        
####################################################################################################

import sys, locale, os, shutil, datetime, requests, re
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'cli'))
from download_bio_data_cli_ru import add_args_ru
from multiprocessing import Pool

#CLI.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
        
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
        vars_1000g_cat_url = 'https://ftp.1000genomes.ebi.ac.uk/vol1/ftp/data_collections/1000G_2504_high_coverage/working/20201028_3202_phased/'
        if not args.download_indexes:
                vars_1000g_regexp = r'CCDG\S+\.gz(?=")'
        else:
                vars_1000g_regexp = r'CCDG\S+\.gz(?:\.tbi)*(?=")'
        download_bio_data(args,
                          vars_1000g_data_name,
                          vars_1000g_cat_url,
                          vars_1000g_regexp)
        
#Версия dbSNP, размещённая проектом Ensembl. Отличается от апстрима,
#как минимум, классической номенклатурой имён хромосом, похромосомным
#разбиением и смещением координат инделей на единичку вправо.
if args.vars_dbsnpens:
        vars_dbsnpens_data_name = 'Variants_dbSNP_Ensembl'
        vars_dbsnpens_cat_url = 'http://ftp.ensembl.org/pub/release-104/variation/vcf/homo_sapiens/'
        if not args.download_indexes:
                vars_dbsnpens_regexp = r'(?<=>)homo_sapiens-chr\S+\.gz(?=<)'
        else:
                vars_dbsnpens_regexp = r'(?<=>)homo_sapiens-chr\S+\.gz(?:\.csi)*(?=<)'
        download_bio_data(args,
                          vars_dbsnpens_data_name,
                          vars_dbsnpens_cat_url,
                          vars_dbsnpens_regexp)
        
#Основная версия dbSNP.
if args.vars_dbsnpncbi:
        vars_dbsnpncbi_data_name = 'Variants_dbSNP_NCBI'
        vars_dbsnpncbi_cat_url = 'https://ftp.ncbi.nih.gov/snp/latest_release/VCF/'
        if not args.download_indexes:
                vars_dbsnpncbi_regexp = r'(?<=>)GCF_000001405\.39\.gz(?=<)'
        else:
                vars_dbsnpncbi_regexp = r'(?<=>)GCF_000001405\.39\.gz(?:\.tbi)*(?=<)'
        download_bio_data(args,
                          vars_dbsnpncbi_data_name,
                          vars_dbsnpncbi_cat_url,
                          vars_dbsnpncbi_regexp)
