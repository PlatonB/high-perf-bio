__version__ = 'v1.1'


class ConcatenateDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, объединяющая все коллекции
одной MongoDB-базы с выводом в другую.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Конкатенируемая база должна быть создана с помощью
create или других инструментов high-perf-bio.

Набор полей объединяемых коллекций должен быть одинаковым.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию];
scr/trg-db-FMT - коллекции исходной/конечной БД,
соответствующие по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля, для которых создавать составной индекс.
'''
        self.en = f'''
A program that merges all collections of
one MongoDB database with output to another.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Concatenated DB must be produced by "create" or other high-perf-bio tools.

The set of fields of merged collections must be the same.

Notation in the CLI help and partially in the GUI help:
[default value];
scr/trg-db-FMT - collections of source/target DB,
matching by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - fields, for which to create a compound index.
'''
