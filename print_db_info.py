__version__ = 'V2.3'

def add_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description='''
Программа, позволяющая вывести
имена всех баз данных или ключевую
информацию об определённой БД.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020.
Версия: V2.3.
Лицензия: GNU General Public License version 3.
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Перед запуском программы нужно установить
MongoDB и PyMongo (см. документацию).

Всё, что имеет отношение к полю
_id, в результаты попадать не будет.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-d', '--db-name', metavar='[None]', dest='db_name', type=str,
                               help='Имя БД, для которой надо вывести информацию (если не указать - выведутся имена всех БД)')
        args = argparser.parse_args()
        return args

def print_db_info(db_name):
        '''
        Функция выведет как общие сведения о БД,
        так и информацию, характеризующую базу
        на уровне составляющих её коллекций.
        Что касается второй части вывода,
        важно определиться, по какой из
        коллекций программа должна давать
        исследователю представление о базе.
        Правильно будет взять коллекцию с
        минимальным во всей БД числом полей.
        В частности, таким образом можно подсказать
        исследователю не индексировать поля,
        существующие не во всех коллекциях,
        а также обходиться без обречённых на
        OperationFailure попыток удаления индексов,
        отсутствующих в той или иной коллекции.
        '''
        client = MongoClient()
        db_obj = client[db_name]
        storage_byte_size = db_obj.command('dbstats')['storageSize']
        if storage_byte_size < 1000:
                storage_size = f'{storage_byte_size} Б'
        elif storage_byte_size < 1000000:
                storage_size = f'{round(storage_byte_size/1000, 1)} КБ'
        elif storage_byte_size < 1000000000:
                storage_size = f'{round(storage_byte_size/1000000, 1)} МБ'
        elif storage_byte_size < 1000000000000:
                storage_size = f'{round(storage_byte_size/1000000000, 1)} ГБ'
        else:
                storage_size = f'{round(storage_byte_size/1000000000000, 1)} ТБ'
        coll_names = sorted(db_obj.list_collection_names())
        min_fields_quan = len(db_obj[coll_names[0]].find_one())
        thin_coll_name = coll_names[0]
        if len(coll_names) > 1:
                for coll_name in coll_names[1:]:
                        fields_quan = len(db_obj[coll_name].find_one())
                        if fields_quan < min_fields_quan:
                                min_fields_quan = copy.deepcopy(fields_quan)
                                thin_coll_name = copy.deepcopy(coll_name)
        thin_coll_obj = db_obj[thin_coll_name]
        first_doc = thin_coll_obj.find_one()
        del first_doc['_id']
        field_names = first_doc.keys()
        indices = list(thin_coll_obj.list_indexes())[1:]
        ind_field_names, ind_names = [], []
        for dct in indices:
                ind_field_names.append(dct.values()[1].keys()[0])
                ind_names.append(dct['name'])
        print('\nОбщие характеристики БД:')
        print('\n\tИмя:\n\t', db_name)
        print('\n\tРазмер (storageSize):\n\t', storage_size)
        print('\n\tИмена коллекций:\n\t', ', '.join(coll_names))
        print('\nХарактеристики наименьшей по числу полей коллекции:')
        print('\n\tИмя:\n\t', thin_coll_name)
        print('\n\tПервый документ:\n\t', first_doc)
        print('\n\tИмена полей первого документа:\n\t', ', '.join(field_names))
        print('\n\tИмена проиндексированных полей:\n\t', ', '.join(ind_field_names))
        print('\n\tИмена индексов:\n\t', ', '.join(ind_names))
        client.close()
        
####################################################################################################

import copy
from argparse import ArgumentParser, RawTextHelpFormatter
from pymongo import MongoClient

#Обработка аргументов
#командной строки.
args = add_args()

#Если исследователь не задал ни
#одного аргумента, выведутся имена
#всех имеющихся на компе MongoDB-баз.
#Если же он указал имя конкретной
#базы, появятся её характеристики.
if args.db_name == None:
        print('\nИмена всех MongoDB-баз:\n', ', '.join(sorted(MongoClient().list_database_names())))
else:
        print_db_info(args.db_name)
