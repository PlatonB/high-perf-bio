__version__ = 'V1.0'

def add_args():
        '''
        Работа с аргументами командной строки.
        '''
        argparser = ArgumentParser(description=f'''
Программа, создающая наборы
тестировочных файлов из
случайных строк исходного.

Автор: Платон Быкадоров (platon.work@gmail.com), 2020
Версия: {__version__}
Лицензия: GNU General Public License version 3
Поддержать проект: https://money.yandex.ru/to/41001832285976
Документация: https://github.com/PlatonB/high-perf-bio/blob/master/README.md
Багрепорты/пожелания/общение: https://github.com/PlatonB/high-perf-bio/issues

Исходный файл должен
быть сжат с помощью GZIP.

Вероятность попадания строки в конечный
файл = 1 / жёсткость прореживания.

Условные обозначения в справке по CLI:
- краткая форма с большой буквы - обязательный аргумент;
- в квадратных скобках - значение по умолчанию;
- в фигурных скобках - перечисление возможных значений.
''',
                                   formatter_class=RawTextHelpFormatter)
        argparser.add_argument('-S', '--arc-file-path', metavar='str', dest='arc_file_path', type=str,
                               help='Путь к сжатому файлу')
        argparser.add_argument('-t', '--trg-dir-path', metavar='[None]', dest='trg_dir_path', type=str,
                               help='Путь к папке для результатов (по умолчанию - путь к папке с исходным файлом)')
        argparser.add_argument('-m', '--meta-lines-quan', metavar='[0]', default=0, dest='meta_lines_quan', type=int,
                               help='Количество строк метаинформации (src-VCF: опция не применяется; src-BED, src-TSV: следует включать шапку)')
        argparser.add_argument('-r', '--thinning-lvl', metavar='[10]', default=10, dest='thinning_lvl', type=int,
                               help='Жёсткость прореживания (чем она больше, тем меньше останется строк)')
        argparser.add_argument('-f', '--trg-files-quan', metavar='[4]', default=4, dest='trg_files_quan', type=int,
                               help='Количество файлов, по которым разнести результаты')
        argparser.add_argument('-p', '--max-proc-quan', metavar='[4]', default=4, dest='max_proc_quan', type=int,
                               help='Максимальное количество параллельно генерируемых файлов')
        args = argparser.parse_args()
        return args

class PrepSingleProc():
        '''
        Класс, спроектированный
        под безопасное параллельное
        распределение случайных
        строк исходного файла
        по конечным файлам.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов, необходимых
                заточенной под многопроцессовое
                выполнение функции, с определённой
                вероятностью сохраняющей строки.
                Атрибуты должны быть созданы
                единожды и далее ни в
                коем случае не изменяться.
                Получаются они в основном из
                указанных исследователем опций.
                '''
                self.arc_file_path = os.path.normpath(args.arc_file_path)
                self.arc_file_name = os.path.basename(self.arc_file_path)
                if args.trg_dir_path == None:
                        self.trg_dir_path = os.path.dirname(self.arc_file_path)
                else:
                        self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                if args.thinning_lvl > 1:
                        self.thinning_lvl = args.thinning_lvl
                else:
                        self.thinning_lvl = 10
                self.pos_outcomes = list(range(self.thinning_lvl))
                self.fav_outcome = choice(self.pos_outcomes)
                
        def thin(self, trg_file_name):
                '''
                Функция рандомного отбора
                строк одного текста.
                '''
                
                #Определение формата исходной сжатой таблицы.
                #Поможет выбору политики обработки хэдеров.
                src_file_format = self.arc_file_name.split('.')[-2]
                
                #Открытие исходного файла
                #на чтение и конечного
                #файла на запись.
                #Прописывание метастрок.
                trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                with gzip.open(self.arc_file_path, mode='rt') as arc_file_opened:
                        with gzip.open(trg_file_path, 'wt') as trg_file_opened:
                                if src_file_format == 'vcf':
                                        for line in arc_file_opened:
                                                if line.startswith('##'):
                                                        trg_file_opened.write(line)
                                                else:
                                                        trg_file_opened.write(line)
                                                        break
                                else:
                                        for meta_line_index in range(self.meta_lines_quan):
                                                trg_file_opened.write(arc_file_opened.readline())
                                                
                                #Если строке повезёт, она
                                #пропишется в конечный файл:).
                                for line in arc_file_opened:
                                        if choice(self.pos_outcomes) == self.fav_outcome:
                                                trg_file_opened.write(line)
                                                
####################################################################################################

import os, gzip
from argparse import ArgumentParser, RawTextHelpFormatter
from multiprocessing import Pool
from random import choice

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение количества и имён
#конечных файлов, определение
#оптимального числа процессов.
args = add_args()
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
arc_file_name = prep_single_proc.arc_file_name
trg_files_quan = args.trg_files_quan
trg_file_names = [str(num + 1) + '_' +
                  arc_file_name for num in range(trg_files_quan)]
if max_proc_quan > trg_files_quan <= 8:
        proc_quan = trg_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nПрореживание {arc_file_name}')
print(f'\tжёсткость: {prep_single_proc.thinning_lvl}')
print(f'\tколичество параллельных процессов: {proc_quan}')

#Параллельный запуск отбора.
with Pool(proc_quan) as pool_obj:
        pool_obj.map(prep_single_proc.thin, trg_file_names)
