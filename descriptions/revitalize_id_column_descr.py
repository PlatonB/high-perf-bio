__version__ = 'v1.0'

class RevitalizeIdColumnDescr():
        def __init__(self, ver):
                self.ru = f'''
Программа, добавляющая rsIDs в столбец ID VCF-файлов.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2021-2022
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Восстанавливаемыми файлами должны быть только сжатые VCF, а БД может содержать лишь одну
коллекцию: созданную с помощью create по VCF и содержащую rsIDs в поле ID. Рекомендуемый
источник данных для этой коллекции - NCBI dbSNP VCF с координатами подходящей сборки.

Чтобы программа работала быстро, нужен индекс полей #CHROM и POS.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию];
src-FMT - модифицируемые таблицы определённого формата;
src-db-FMT - коллекции исходной БД, соответствующие
по структуре таблицам определённого формата.
'''
                self.en = f'''
A program that adds rsIDs into ID column of VCF files.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2021-2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Only compressed VCFs must be restorable files, and DB can contain only one collection:
produced by "create" from VCF and contained rsIDs in the ID field. The recommended data
source for this collection is NCBI dbSNP VCF with the coordinates of a suitable assembly.

For the program to work fast, there needs an index of #CHROM and POS fields.

Notation in the CLI help and partially in the GUI help:
[default value];
src-FMT - modified tables in a certain format;
src-db-FMT - collections of source DB, matching
by structure to the tables in a certain format.
'''
