__version__ = 'v1.1'


class GenTestFilesDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, создающая наборы тестировочных файлов из случайных строк исходного.

Версия: {version}
Требуемые сторонние компоненты: -
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Исходный файл должен быть сжат с помощью GZIP.

Вероятность попадания строки в конечный файл = 1 / жёсткость прореживания.

Конечные файлы будут схожи по размеру и частично совпадать по составу строк.

Условные обозначения в справке по CLI:
[значение по умолчанию];
src-FMT - разреживаемая таблица определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку.
'''
        self.en = f'''
A program that creates sets of test files from random strings of the source file.

Version: {version}
Dependencies: -
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The source file must be compressed by GZIP.

Probability of hitting a line into the target file = 1 / thinning level.

The target files will be similar in size and partially overlap in the content.

The notation in the CLI help:
[default value];
src-FMT - thinned table in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error.
'''
