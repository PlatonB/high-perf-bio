__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, создающая MongoDB-базу данных
по VCF, BED или любым другим таблицам.

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
                             help='Разрешить дозаписывать данные в имеющуюся базу (не допускайте смешивания форматов в одной БД)')
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
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
        args = arg_parser.parse_args()
        return args
