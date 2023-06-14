__version__ = 'v1.1'


class QueryDescr():
    def __init__(self, version, authors):
        self.ru = f'''
Программа, выполняющая наборы запросов по всем коллекциям MongoDB-базы.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

В исходных файлах каждый запрос должен располагаться в отдельной строке.
Допустимы пустые строки (например, для повышения читаемости наборов запросов).

Источником отбираемых данных должна быть база, созданная
с помощью create_db или других инструментов high-perf-bio.

Чтобы программа работала быстро, нужны индексы вовлечённых в запрос полей.

Поддерживается только Python-диалект языка запросов
MongoDB (нажмите Select your language --> Python):
https://docs.mongodb.com/manual/tutorial/query-documents/

Допустимые MongoDB-операторы:
https://docs.mongodb.com/manual/reference/operator/query/

Примеры указания типа данных:
"any_str"
Decimal128("any_str")

К вложенному полю обращайтесь через точку:
"field_1.field_2.(...).field_N"

Пример запроса:
{{"$or": [{{"INFO.0.AF_AFR": {{"$gte": Decimal128("0.02")}}}}, {{"INFO.0.AF_EUR": {{"$lte": Decimal128("0.3")}}}}, {{"INFO.0.AF_EAS": {{"$lte": Decimal128("0.3")}}}}]}}

Не добавляйте в запрос условие
{{'meta': {{'$exists': False}}}}.
Программа сама сделает это.

Условные обозначения в справке по CLI и частично GUI:
[значение по умолчанию];
{{допустимые значения}};
scr/trg-db-FMT - коллекции исходной/конечной БД,
соответствующие по структуре таблицам определённого формата;
trg-FMT - конечные таблицы определённого формата;
не применяется - при обозначенных условиях
аргумент проигнорируется или вызовет ошибку;
f1+f2+f3 - сортируемые поля, а также поля,
для которых создавать составной индекс.
'''
        self.en = f'''
A program that runs query sets to all collections of MongoDB database.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

In the source files, each query must be on a separate line.
Blank lines are allowed (e.g., to make query sets more human readable).

The source of the retrieved data should be the DB
produced by "create_db" or other high-perf-bio tools.

For the program to work fast, it needs indexes of the fields involved in the query.

Only Python dialect of MongoDB query language is
supported (press "Select your language" --> "Python"):
https://docs.mongodb.com/manual/tutorial/query-documents/

Allowed MongoDB operators:
https://docs.mongodb.com/manual/reference/operator/query/

Examples of specifying data type:
"any_str"
Decimal128("any_str")

Call the nested field using a point:
"field_1.field_2.(...).field_N"

Example of query:
{{"$or": [{{"INFO.0.AF_AFR": {{"$gte": Decimal128("0.02")}}}}, {{"INFO.0.AF_EUR": {{"$lte": Decimal128("0.3")}}}}, {{"INFO.0.AF_EAS": {{"$lte": Decimal128("0.3")}}}}]}}

Do not add to the query condition
{{'meta': {{'$exists': False}}}}.
The program will do this itself.

Notation in the CLI help and partially in the GUI help:
[default value];
{{permissible values}};
scr/trg-db-FMT - collections of source/target DB,
matching by structure to the tables in a certain format;
trg-FMT - target tables in a certain format;
not applicable - under the specified conditions
the argument is ignored or causes an error;
f1+f2+f3 - sorted fields, as well as fields,
for which to create a compound index.
'''
