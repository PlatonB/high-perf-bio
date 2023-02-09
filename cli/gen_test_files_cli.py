__version__ = 'v3.1'

import sys, os
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'descriptions'))
from argparse import ArgumentParser, RawTextHelpFormatter
from gen_test_files_descr import GenTestFilesDescr

def add_args_ru(version, authors):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=GenTestFilesDescr(version, authors).ru,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-file-path', required=True, metavar='str', dest='src_file_path', type=str,
                             help='Путь к сжатому файлу')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
        opt_grp.add_argument('-r', '--thinning-lvl', metavar='[10]', default=10, dest='thinning_lvl', type=int,
                             help='Жёсткость прореживания (чем она больше, тем меньше останется строк)')
        opt_grp.add_argument('-n', '--trg-files-quan', metavar='[4]', default=4, dest='trg_files_quan', type=int,
                             help='Количество файлов, по которым разнести результаты')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно генерируемых файлов')
        args = arg_parser.parse_args()
        return args

def add_args_en(version, authors):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=GenTestFilesDescr(version, authors).en,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-file-path', required=True, metavar='str', dest='src_file_path', type=str,
                             help='Path to gzipped file')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Quantity of metainformation lines (src-VCF: not applicable; src-BED, src-TSV: include a header)')
        opt_grp.add_argument('-r', '--thinning-lvl', metavar='[10]', default=10, dest='thinning_lvl', type=int,
                             help='Thinning level (the higher it is, the fewer lines will be remain)')
        opt_grp.add_argument('-n', '--trg-files-quan', metavar='[4]', default=4, dest='trg_files_quan', type=int,
                             help='Quantity of files by which to distribute the results')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of files generated in parallel')
        args = arg_parser.parse_args()
        return args
