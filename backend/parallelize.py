import multiprocessing
import datetime

__version__ = 'v1.0'


def parallelize(proc_quan, main_func, storage_unit_names):
    '''
    Функция, позволяющая работать с несколькими таблицами или
    коллекциями в несколько соответствующих параллельных процессов.
    '''
    with multiprocessing.Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(main_func, storage_unit_names)
        exec_time = datetime.datetime.now() - exec_time_start
    return exec_time
