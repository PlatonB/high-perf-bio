__version__ = 'v1.0'

class CreateDescr():
        def __init__(self, ver):
                self.ru = f'''
Программа, создающая MongoDB-базу данных
по VCF, BED или любым другим таблицам.

Версия: {ver}
Требуемые сторонние компоненты: MongoDB, PyMongo
Автор: Платон Быкадоров (platon.work@gmail.com), 2020-2022
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

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]];
{{допустимые значения}};
src-FMT - исходные таблицы определённого формата (VCF, BED, TSV);
trg-db-FMT - коллекции конечной БД, соответствующие
по структуре таблицам определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - поля, для которых создавать составной индекс.
'''
                self.en = f'''
A program that creates a MongoDB database
from VCF, BED or any other tables.

Version: {ver}
Dependencies: MongoDB, PyMongo
Author: Platon Bykadorov (platon.work@gmail.com), 2020-2022
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

Notation in the CLI help and partially in the GUI help:
[default value in the argument parsing step];
[[final default value]];
{{permissible values}};
src-FMT - source tables in a certain format (VCF, BED, TSV);
trg-db-FMT - collections of target DB, matching
by structure to the tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - fields, for which to create a compound index.
'''
