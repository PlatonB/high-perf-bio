__version__ = 'v1.2'

class CountDescr():
        def __init__(self, version, authors):
                self.ru = f'''
Программа, считающая количество и, опционально, частоту каждого
набора соответствующих значений заданных полей в пределах коллекции.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Основное применение программы - определение количества и частот вариантов.
Предполагается, что обрабатываемая коллекция была получена конкатенацией
многочисленных VCF. Разумеется, возможны и другие сценарии работы.

Обсчитываемая база должна быть создана с помощью create_db или concatenate.

К вложенному полю обращайтесь через точку:
field_1.field_2.(...).field_N

Из-за ограничений со стороны MongoDB, программа не использует индексы.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}}.
'''
                self.en = f'''
A program that counts the quantity and, optionally, the frequency of
each set of corresponding values of given fields within collection.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The main use of the program is to determine the quantity and frequencies of
variants. It is assumed that processed collection was obtained by concatenation
of multiple VCFs. Of course, other work scenarios are also possible.

The counted DB must be produced by "create_db" or "concatenate".

Call the nested field using a dot:
field_1.field_2.(...).field_N

Due to limitations on the MongoDB side, the program does not use indexes.

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}}.
'''
