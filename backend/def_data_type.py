__version__ = 'V1.0'

from bson.decimal128 import Decimal128
from decimal import InvalidOperation

def def_data_type(string):
        '''
        Функция, подбирающая подходящий
        тип данных какого-либо значения.
        Если ничего не подходит, то
        значение останется строкой.
        '''
        try:
                result = int(string)
        except ValueError:
                try:
                        result = Decimal128(string)
                except InvalidOperation:
                        result = string
        return result
