__version__ = 'v1.0'

class NoSuchFieldError(Exception):
        '''
        Если исследователь, допустим, опечатавшись,
        указал поле, которого нет в коллекциях.
        '''
        def __init__(self, field_path):
                err_msg = f'\nThe field {field_path} does not exist'
                super().__init__(err_msg)
                
class DifFmtsError(Exception):
        '''
        Поддерживаются только одноформатные таблицы.
        '''
        def __init__(self, file_fmts):
                err_msg = f'\nSource files are in different formats: {file_fmts}'
                super().__init__(err_msg)
                
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
