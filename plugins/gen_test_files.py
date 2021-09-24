__version__ = 'v3.2'

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
                Получение атрибутов, необходимых заточенной под многопроцессовое
                выполнение функции, с определённой вероятностью сохраняющей строки
                в файл. Атрибуты ни в коем случае не должны будут потом в параллельных
                процессах изменяться. Получаются они в основном из указанных исследователем
                аргументов. Генерация набора чисел от 0 до (жёсткость - 1) в качестве
                равновозможных исходов. Случайный выбор числа из этого набора как
                единственного исхода, благоприятствующего прописыванию строки.
                '''
                self.src_file_path = os.path.normpath(args.src_file_path)
                self.src_file_name = os.path.basename(self.src_file_path)
                self.src_file_fmt = self.src_file_name.rsplit('.', maxsplit=2)[1]
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                self.thinning_lvl = args.thinning_lvl
                self.pos_outcomes = list(range(self.thinning_lvl))
                self.fav_outcome = random.choice(self.pos_outcomes)
                
        def thin(self, trg_file_name):
                '''
                Функция рандомного отбора
                строк одного текста.
                '''
                
                #Открытие исходного файла на чтение и конечного
                #файла на запись. Прописывание метастрок.
                with gzip.open(self.src_file_path, mode='rt') as src_file_opened:
                        trg_file_path = os.path.join(self.trg_dir_path, trg_file_name)
                        with gzip.open(trg_file_path, 'wt') as trg_file_opened:
                                if self.src_file_fmt == 'vcf':
                                        for line in src_file_opened:
                                                trg_file_opened.write(line)
                                                if not line.startswith('##'):
                                                        break
                                else:
                                        for meta_line_index in range(self.meta_lines_quan):
                                                trg_file_opened.write(src_file_opened.readline())
                                                
                                #Если строке повезёт, она пропишется в конечный файл.
                                for line in src_file_opened:
                                        if random.choice(self.pos_outcomes) == self.fav_outcome:
                                                trg_file_opened.write(line)
                                                
####################################################################################################

import sys, locale, os, random, datetime, gzip
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                             'cli'))
from gen_test_files_cli import add_args_ru, add_args_en
from multiprocessing import Pool

#Подготовительный этап: обработка
#аргументов командной строки,
#создание экземпляра содержащего
#ключевую функцию класса,
#получение количества и имён
#конечных файлов, определение
#оптимального числа процессов.
if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__)
else:
        args = add_args_en(__version__)
max_proc_quan = args.max_proc_quan
prep_single_proc = PrepSingleProc(args)
src_file_name = prep_single_proc.src_file_name
trg_files_quan = args.trg_files_quan
trg_file_names = [str(num + 1) + '_' +
                  src_file_name for num in range(trg_files_quan)]
if max_proc_quan > trg_files_quan <= 8:
        proc_quan = trg_files_quan
elif max_proc_quan > 8:
        proc_quan = 8
else:
        proc_quan = max_proc_quan
        
print(f'\nGenerating test files based on {src_file_name}')
print(f'\tnumber of parallel processes: {proc_quan}')
print(f'\trigidity: {prep_single_proc.thinning_lvl}')

#Параллельный запуск отбора. Замер времени
#выполнения этого кода с точностью до микросекунды.
with Pool(proc_quan) as pool_obj:
        exec_time_start = datetime.datetime.now()
        pool_obj.map(prep_single_proc.thin, trg_file_names)
        exec_time = datetime.datetime.now() - exec_time_start
        
print(f'\tparallel computation time: {exec_time}')
