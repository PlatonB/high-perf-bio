import copy
import re

__version__ = 'v3.0'


def parse_nested_objs(obj, parent_field_path=None, all_field_paths=None):
    '''
    Функция, собирающая пути ко всем полям документа. Пути оформляются в виде поддерживаемой
    MongoDB/PyMongo точечной нотации. Если поле соответствует элементу массива, то в качестве
    имени поля будет числовой индекс. Но для путей типа ключ1.индекс.ключ2 функция подготовит
    в том числе и сокращённый вариант написания: ключ1.ключ2. Урезанный синтаксис может
    пригодиться, например, когда нужно отбирать документы по значениям нескольких полей,
    обладающих одним и тем же именем и находящихся в пределах одного массива. Или, когда
    вложенное в массив поле требуется проиндексировать. Ну либо, если следует обозначить
    ссылку на такое поле внутри expression, ибо там явное указание индексов не поддерживается
    (см. mongodb.com/community/forums/t/group-addtoset-dont-work-with-nested-fields/138677).
    Кстати, в этом же форумном треде я приводил пример того, что неявная форма пути
    не работает для доступа к полям, вложенным в дву-и-более-мерный массив. Поэтому
    описываемая функция не генерит безындексные пути при таком экзотическом случае.
    '''
    if type(obj) is dict:
        for key, val in obj.items():
            if parent_field_path is None:
                child_field_path = copy.deepcopy(key)
            else:
                child_field_path = f'{parent_field_path}.{key}'
            if all_field_paths is None:
                all_field_paths = [child_field_path]
            else:
                all_field_paths.append(child_field_path)
            if re.search(r'^\w+\.\d+\.\w+$', child_field_path) is not None:
                all_field_paths.append(re.sub(r'\.\d+',
                                              '',
                                              child_field_path))
            if type(val) in [dict, list]:
                parse_nested_objs(val,
                                  parent_field_path=child_field_path,
                                  all_field_paths=all_field_paths)
    elif type(obj) is list:
        for idx in range(len(obj)):
            child_field_path = f'{parent_field_path}.{idx}'
            all_field_paths.append(child_field_path)
            if type(obj[idx]) in [dict, list]:
                parse_nested_objs(obj[idx],
                                  parent_field_path=child_field_path,
                                  all_field_paths=all_field_paths)
    return all_field_paths


'''
Тест.
print(parse_nested_objs({'q': 1,
                         'w': {},
                         'e': {'a': 10,
                               'b': 11,
                               'c': {'d': -11,
                                     'e': -10,
                                     'f': -9}},
                         'r': '.',
                         't': [],
                         'y': [100,
                               101,
                               {'l': 0.01,
                                'm': 0.02,
                                'n': 0.03},
                               [{'h': 0,
                                 'z': 0},
                                   {'m': 0,
                                    'z': 0}]]}))
'''
