__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, позволяющая разбить каждую
коллекцию MongoDB-базы по хромосомам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Источником разделяемых данных должна быть база, созданная с помощью create_db.

Чтобы программа работала быстро, нужен индекс поля, в котором имена хромосом.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
scr/trg-db-FMT - исходная/конечная БД с коллекциями,
соответствующими по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля коллекций БД с составным индексом.
''',
                                   formatter_class=RawTextHelpFormatter,
                                   add_help=False)
        hlp_grp = arg_parser.add_argument_group('Аргумент вывода справки')
        hlp_grp.add_argument('-h', '--help', action='help',
                             help='Вывести справку и выйти')
        man_grp = arg_parser.add_argument_group('Обязательные аргументы')
        man_grp.add_argument('-D', '--src-db-name', required=True, metavar='str', dest='src_db_name', type=str,
                             help='Имя БД, коллекции которой разделять')
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-f', '--chrom-field-name', metavar='[None]', dest='chrom_field_name', type=str,
                             help='Имя хромосомного поля БД (src-db-VCF, src-db-BED: не применяется, src-db-TSV: [[первое после _id поле]])')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; src-db-VCF: не применяется; src-db-BED: trg-(db-)TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
        opt_grp.add_argument('-i', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Имена индексируемых полей (через запятую без пробела; trg-db-VCF: проиндексируются #CHROM+POS и ID; trg-db-BED: проиндексируются chrom+start+end и name)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно делимых коллекций')
        args = arg_parser.parse_args()
        return args
