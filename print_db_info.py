__version__ = 'V1.0'

print('''
Программа, позволяющая вывести
список всех баз данных или ключевую
информацию об определённой БД.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V1.0.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues
Справка по CLI: python3 print_db_info.py -h

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).

В качестве информации о БД будет
выводиться несколько характеристик
наименьшей по числу полей коллекции.
''')

def add_main_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description='''
Краткая форма с большой буквы - обязательный аргумент.
В квадратных скобках - значение по умолчанию.
В фигурных скобках - перечисление возможных значений.
''')
        argparser.add_argument('-d', '--db-name', metavar='[None]', dest='db_name', type=str,
                               help='Имя БД, для которой надо вывести информацию (если не указать - выведутся имена всех БД)')
        args = argparser.parse_args()
        return args

def print_db_info(db_name):
        '''
        Функция выведет такую
        информацию о БД: имена
        всех полей, только
        проиндексированных
        полей и индексов.
        Но следует учитывать
        одну особенность MongoDB:
        эта СУБД позволяет держать
        в составе базы коллекции с
        разным количеством полей.
        Поэтому надо определиться,
        по какой из коллекций
        давать исследователю
        представление о базе.
        Правильно будет взять
        коллекцию с минимальным
        во всей БД числом полей.
        В частности, таким образом
        можно избежать создания
        индексов существующих не
        во всех коллекциях полей
        и обречённых на OperationFailure
        попыток удаления отсутствующих в
        той или иной коллекции индексов.
        '''
        client = MongoClient()
        db_obj = client[db_name]
        coll_names = db_obj.list_collection_names()
        min_fields_quan = len(db_obj[coll_names[0]].find_one())
        thin_coll_name = coll_names[0]
        if len(coll_names) > 1:
                for coll_name in coll_names[1:]:
                        fields_quan = len(db_obj[coll_name].find_one())
                        if fields_quan < min_fields_quan:
                                min_fields_quan = copy.deepcopy(fields_quan)
                                thin_coll_name = copy.deepcopy(coll_name)
        thin_coll_obj = db_obj[thin_coll_name]
        field_names = list(thin_coll_obj.find_one().keys())[1:]
        indices = list(thin_coll_obj.list_indexes())[1:]
        ind_field_names, ind_names = [], []
        for dct in indices:
                ind_field_names.append(dct.values()[1].keys()[0])
                ind_names.append(dct['name'])
        print(f'\nХарактеристики коллекции {thin_coll_name} БД {db_name}:')
        print('\nИмена всех полей:\n', field_names)
        print('\nИмена проиндексированных полей:\n', ind_field_names)
        print('\nИмена индексов:\n', ind_names)
        client.close()
        
####################################################################################################

import copy
from argparse import ArgumentParser
from pymongo import MongoClient

#Обработка аргументов
#командной строки.
args = add_main_args()

#Если исследователь не задал ни
#одного аргумента, выведутся имена
#всех имеющихся на компе MongoDB-баз.
#Если же он указал имя конкретной
#базы, появятся её характеристики.
if args.db_name == None:
        print('\nВсе MongoDB-базы:\n', MongoClient().list_database_names())
else:
        print_db_info(args.db_name)
