__version__ = 'v1.1'

class ReindexDescr():
        def __init__(self, version, authors):
                self.ru = f'''
Программа, способная удалять имеющиеся индексы MongoDB-базы и добавлять новые.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Не путайте понятия имени поля и имени индекса.

Для вывода имён баз данных, индексов и полей рекомендую
использовать info из состава high-perf-bio, либо MongoDB Compass.

Поддерживается создание/удаление как одиночных, так и составных индексов.

Условные обозначения в справке по CLI:
[значение по умолчанию]
'''
                self.en = f'''
A program that can delete existing indexes of MongoDB database and add new ones.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Do not confuse the concepts of field name and index name.

To print names of databases, indexes and fields I recommend
to use "info" from high-perf-bio or MongoDB Compass.

The creation/deletion of both single and compound indexes is supported.

Notation in the CLI help:
[default value]
'''
