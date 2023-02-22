__version__ = 'v5.0'

import warnings

class DifFmtsError(Exception):
        '''
        Если конкретной программой
        поддерживаются только
        одноформатные таблицы.
        '''
        def __init__(self, file_fmts):
                err_msg = f'\nSource files are in different formats: {file_fmts}'
                super().__init__(err_msg)
                
class DbAlreadyExistsError(Exception):
        '''
        high-perf-bio не любит путаницу,
        поэтому за крайне редким исключением
        не позволяет редактировать имеющиеся БД.
        На сегодняшний день актуально правило:
        новый результат - в новый файл или базу.
        '''
        def __init__(self):
                err_msg = '''\nUploading data to the source DB is
strictly denied, whereas uploading to
another existing DB is possible only if
explicitly specified corresponding argument'''
                super().__init__(err_msg)
                
class IncompatibleArgsError(Exception):
        '''
        Комфликт двух или более аргументов.
        '''
        def __init__(self, *args):
                err_msg = f'\n{" and ".join(args)} arguments are incompatible'
                super().__init__(err_msg)
                
class FormatIsNotSupportedError(Exception):
        '''
        Неприменимость аргумента
        к src-FMT или src-db-FMT.
        '''
        def __init__(self, arg, src_fmt):
                err_msg = f"\n'{arg}' argument doesn't applicable to {src_fmt} format"
                super().__init__(err_msg)
                
class NoSuchFieldWarning():
        '''
        Указанного исследователем поля,
        возможно, нет в коллекциях.
        '''
        def __init__(self, field_path):
                print('')
                warnings.warn(f'\nThe field {field_path} probably does not exist')
