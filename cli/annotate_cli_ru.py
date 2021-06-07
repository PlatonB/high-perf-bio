__version__ = 'v1.0'

from argparse import ArgumentParser, RawTextHelpFormatter

def add_args_ru(ver):
        '''
        Работа с аргументами командной строки.
        '''
        arg_parser = ArgumentParser(description=f'''
Программа, получающая характеристики
элементов выбранного столбца по MongoDB-базе.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2021
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Аннотируемый столбец:
- должен занимать одно и то же положение во всех исходных таблицах;
- целиком размещается в оперативную память, что может замедлить работу компьютера.

Также в качестве эксперимента существует возможность пересечения
по координатам. Поддерживаются все 4 комбинации VCF и BED.

Каждая аннотируемая таблица обязана быть сжатой с помощью GZIP.

Источником характеристик должна быть база данных, созданная с помощью create_db.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - аннотируемые таблицы определённого формата;
scr/trg-db-FMT - исходная/конечная БД с коллекциями,
соответствующими по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку
''',
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
        man_grp.add_argument('-T', '--trg-place', required=True, metavar='str', dest='trg_place', type=str,
                             help='Путь к папке или имя БД для результатов')
        opt_grp = arg_parser.add_argument_group('Необязательные аргументы')
        opt_grp.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                             help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
        opt_grp.add_argument('-n', '--by-loc', dest='by_loc', action='store_true',
                             help='Пересекать по геномной локации (экспериментальная фича; src-TSV, src-db-TSV: не применяется)')
        opt_grp.add_argument('-c', '--ann-col-num', metavar='[None]', dest='ann_col_num', type=int,
                             help='Номер аннотируемого столбца (применяется без -n; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
        opt_grp.add_argument('-f', '--ann-field-name', metavar='[None]', dest='ann_field_name', type=str,
                             help='Имя поля БД, по которому аннотировать (применяется без -n; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[rsID]])')
        opt_grp.add_argument('-k', '--proj-fields', metavar='[None]', dest='proj_fields', type=str,
                             help='Отбираемые поля (через запятую без пробела; src-db-VCF: не применяется; src-db-BED: trg-(db-)TSV; поле _id не выведется)')
        opt_grp.add_argument('-s', '--sec-delimiter', metavar='[comma]', choices=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], default='comma', dest='sec_delimiter', type=str,
                             help='{colon, comma, low_line, pipe, semicolon} Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
        opt_grp.add_argument('-i', '--ind-field-names', metavar='[None]', dest='ind_field_names', type=str,
                             help='Имена индексируемых полей (через запятую без пробела; trg-db-VCF: проиндексируются #CHROM+POS и ID; trg-db-BED: проиндексируются chrom+start+end и name)')
        opt_grp.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                             help='Максимальное количество параллельно аннотируемых таблиц')
        args = arg_parser.parse_args()
        return args
