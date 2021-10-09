__version__ = 'v2.1'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, создающая MongoDB-базу данных по VCF, BED или любым другим таблицам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

tbi- и csi-индексы при сканировании исходной папки игнорируются.

Формат исходных таблиц:
- Должен быть одинаковым для всех.
- Определяется программой по расширению.

TSV: так будет условно обозначаться неопределённый табличный формат.

Требования к наличию табличной шапки (набора имён столбцов)
и подсчёту количества нечитаемых программой строк:

Формат     |  Наличие         |  Модификация значения
src-файла  |  шапки           |  -m при наличии шапки
-----------------------------------------------------
VCF        |  Обязательно     |  Аргумент не применим
BED        |  Не обязательно  |  Прибавьте 1
TSV        |  Обязательно     |  Не прибавляйте 1

HGVS accession numbers конвертируются в обычные хромосомные имена
(для патчей и альтернативных локусов программа этого не делает).

Каждая исходная таблица должна быть сжата с помощью GZIP.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - исходные таблицы определённого формата (VCF, BED, TSV);
trg-db-FMT - конечная БД с коллекциями, соответствующими
по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля коллекций БД с составным индексом
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Путь к папке со сжатыми таблицами, преобразуемыми в коллекции MongoDB-базы')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-d', '--trg-db-name', metavar='[None]', dest='trg_db_name', type=str,
                             help='Имя пополняемой базы данных ([[имя папки со сжатыми таблицами]])')
        opt_grp.add_argument('-a', '--append', dest='append', action='store_true',
                             help='Разрешить дозаписывать данные в имеющуюся базу (не допускайте смешивания форматов в одной БД; зальются только новые файлы папки)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку)')
        opt_grp.add_argument('-r', '--minimal', dest='minimal', action='store_true',
                             help='Загружать только минимально допустимый форматом набор столбцов (src-VCF: 1-ые 8; src-BED: 1-ые 3; src-TSV: не применяется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[None]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для разбиения ячейки на список (src-VCF, src-BED: не применяется)')
        opt_grp.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                             help='Максимальное количество строк фрагмента заливаемой таблицы')
        opt_grp.add_argument('-i', '--ind-col-names', metavar='[None]', dest='ind_col_names', type=str,
                             help='Имена индексируемых полей (через запятую без пробела; trg-db-VCF: проиндексируются #CHROM+POS и ID; trg-db-BED: проиндексируются chrom+start+end и name)')
        args = arg_parser.parse_args()
        return args

def add_args_en(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
A program that creates a MongoDB database from VCF, BED or any other tables.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2021
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

tbi- and csi-indexes are ignored when scanning the source directory.

The format of the source tables:
- Must be the same for all.
- The program recognizes it by extension.

TSV: this will be the conventional term for an undefined table format.

Requirements for table header (set of column names) and
for counting the number of rows unreadable by the program:

Format of  |  Presence of   |  Modification of -m value
src file   |  header        |  if header is present
---------------------------------------------------------
VCF        |  Required      |  Argument is not applicable
BED        |  Not required  |  Plus 1
TSV        |  Required      |  Do not plus 1

HGVS accession numbers are converted to regular chromosome names
(the program does not do this for patches and alternative loci).

Each source table must be compressed using GZIP.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - source tables in a certain format (VCF, BED, TSV);
trg-db-FMT - target DB with collections matching
by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - fields of the DB collections with a compound index
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Showing help argument')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Show this help message and exit')
        man_grp = arg_parser.add_argument_group('Mandatory arguments')
        man_grp.add_argument('-S', '--src-dir-path', required=True, metavar='str', dest='src_dir_path', type=str,
                             help='Path to directory with gzipped tables which will be converted to collections of MongoDB database')
        opt_grp = arg_parser.add_argument_group('Optional arguments')
        opt_grp.add_argument('-d', '--trg-db-name', metavar='[None]', dest='trg_db_name', type=str,
                             help='Name of the replenished DB ([[compressed tables directory name]])')
        opt_grp.add_argument('-a', '--append', dest='append', action='store_true',
                             help='Permit addition of data to the existing DB (do not allow mixing of formats in the same DB; only new files of the directory will be uploaded)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Maximum number of parallel uploaded tables/indexed collections')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Number of metainformation lines (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header)')
        opt_grp.add_argument('-r', '--minimal', dest='minimal', action='store_true',
                             help='Upload only the minimum set of columns allowed by the format (src-VCF: 1st 8; src-BED: 1st 3; src-TSV: not applicable)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[None]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Punctuation mark for dividing a cell to a list (src-VCF, src-BED: not applicable)')
        opt_grp.add_argument('-c', '--max-fragment-len', metavar='[100000]', default=100000, dest='max_fragment_len', type=int,
                             help='Maximum number of rows of uploaded table fragment')
        opt_grp.add_argument('-i', '--ind-col-names', metavar='[None]', dest='ind_col_names', type=str,
                             help='Names of indexed fields (comma separated without spaces; trg-db-VCF: #CHROM+POS and ID will be indexed); trg-db-BED: chrom+start+end and name will be indexed)')
        args = arg_parser.parse_args()
        return args
