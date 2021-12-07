__version__ = 'v1.0'

import copy

def parse_nested_objs(obj, parent_field_path=None, all_field_paths=[]):
        '''
        Функция, собирающая пути ко всем полям документа. Пути оформляются
        в виде поддерживаемой PyMongo точечной нотации. Если поле соответствует
        элементу массива, то в качестве имени поля будет числовой индекс.
        '''
        if type(obj) is dict:
                for key, val in obj.items():
                        if parent_field_path is None:
                                child_field_path = copy.deepcopy(key)
                        else:
                                child_field_path = f'{parent_field_path}.{key}'
                        all_field_paths.append(child_field_path)
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
