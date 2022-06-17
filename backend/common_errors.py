__version__ = 'v4.0'

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
                
#DEPRECATED.
class ByLocTsvError(Exception):
        '''
        В исследуемом TSV или основанной на TSV
        коллекции может не быть геномных координат.
        Ну или бывает, когда координатные столбцы
        располагаются, где попало. Поэтому нельзя,
        чтобы при пересечении по локации хоть в одном
        из этих двух мест витал вольноформатный дух.
        '''
        def __init__(self):
                err_msg = '\nIntersection by location is not possible for src-TSV or src-db-TSV'
                super().__init__(err_msg)
                
class NoSuchFieldError(Exception):
        '''
        Если исследователь, допустим, опечатавшись,
        указал поле, которого нет в коллекциях.
        '''
        def __init__(self, field_path):
                err_msg = f'\nThe field {field_path} does not exist'
                super().__init__(err_msg)
