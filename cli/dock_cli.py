__version__ = 'v3.3'

from argparse import ArgumentParser, RawTextHelpFormatter
from descriptions.dock_descr import DockDescr

def add_args_ru(version, authors):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=DockDescr(version, authors).ru,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми аннотируемыми таблицами')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, по которой аннотировать')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Путь к папке для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно аннотируемых таблиц')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку)')
        opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                             help='''{by_location, by_alleles} Аннотировать, пересекая по геномной локации (src-TSV, src-db-TSV: не применяется).
Аннотировать ID по ID, уточняя по аллелям совпадения ID (src-TSV/BED, src-db-TSV/BED: не применяется)''')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Номер аннотируемого столбца (применяется без -0; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                             help='Точечный путь к полю, по которому аннотировать (применяется без -0; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Отбираемые поля верхнего уровня и/или столбцы (через запятую без пробела; имя_столбца_f; поле _id не выведется)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка')
        args = arg_parser.parse_args()
        return args

def add_args_en(version, authors):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=DockDescr(version, authors).en,
                                    formatter_class=RawTextHelpFormatter,
                                    add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Path to directory with gzipped tables, which will be annotated')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Name of the DB by which to annotate')
        man_grp.add_argument('-T', '--trg-dir-path', required=True, metavar='str', dest='trg_dir_path', type=str,
                             help='Path to directory for results')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum quantity of parallel annotated tables')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Quantity of metainformation lines of annotated tables (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header)')
        opt_grp.add_argument('-0', '--preset', metavar='[None]', choices=['by_location', 'by_alleles'], dest='preset', type=str,
                             help='''{by_location, by_alleles} Annotate via intersection by genomic location (src-TSV, src-db-TSV: not applicable).
Annotate ID by ID, verifying by alleles ID matches (src-TSV/BED, src-db-TSV/BED: not applicable)''')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Number of the annotated column (applicable without -0; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-path', metavar='[None]', dest='ann_field_path', type=str,
                             help='Dot path to the field by which to annotate (applicable without -0; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])')
        opt_grp.add_argument('-k', '--proj-field-names', metavar='[None]', dest='proj_field_names', type=str,
                             help='Selected top level fields and/or columns (comma separated without spaces; column_name_f; _id field will not be output)')
        opt_grp.add_argument('-,', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark to restore a cell from a list')
        args = arg_parser.parse_args()
        return args
