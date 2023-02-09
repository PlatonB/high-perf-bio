__version__ = 'v1.1'

class CountLinesDescr():
        def __init__(self, version, authors):
                self.ru = f'''
Программа, считающая непустые строки
таблиц, расположенных в дереве папок.

Версия: {version}
Требуемые сторонние компоненты: -
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Исходные файлы должны быть сжаты с помощью GZIP.

Подсчёт строк без учёта дублей может переполнить RAM.

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
src-FMT - обсчитываемые таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
'''
                self.en = f'''
A program that counts non-empty rows
of tables located in a directory tree.

Version: {version}
Dependencies: -
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The source files must be compressed by GZIP.

Counting rows without considering duplicates may overflow RAM.

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]];
src-FMT - calculated tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
'''
