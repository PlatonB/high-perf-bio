__version__ = 'v1.2'

class InfoDescr():
        def __init__(self, version, authors):
                self.ru = f'''
Программа, позволяющая вывести имена всех баз
данных или ключевую информацию об определённой БД.

Версия: {version}
Требуемые сторонние компоненты: MongoDB, PyMongo
Авторы: {chr(10).join(authors)}
Лицензия: GNU General Public License version 3
Поддержать проект: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Этот инструмент полезен, если вы используете Linux без графической оболочки.
Если же у вас дистрибутив с DE, предпросматривайте БД в MongoDB Compass.

Информацию о БД можно вывести не только на экран, но и в файл:
python3 info.py -d DB_name > path/to/DB_info.txt

Условные обозначения в справке по CLI:
[значение по умолчанию на этапе парсинга аргументов];
[[конкретизированное значение по умолчанию]].
'''
                self.en = f'''
A program that allows to print the names of
all DBs or key information about a certain DB.

Version: {version}
Dependencies: MongoDB, PyMongo
Authors: {chr(10).join(authors)}
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

This tool is good if you use Linux without a graphical shell. If you
work on a distribution with DE, you can preview DB in MongoDB Compass.

Information about DB can be not only printed, but also redirected to file:
python3 info.py -d DB_name > path/to/DB_info.txt

The notation in the CLI help:
[default value in the argument parsing step];
[[final default value]].
'''
