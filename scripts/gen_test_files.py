__version__ = 'v4.1'

import sys, os, locale, random, datetime, gzip
sys.dont_write_bytecode = True
if __name__ == '__main__':
        sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                     'cli'))
        sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                     'backend'))
        from gen_test_files_cli import add_args_ru, add_args_en
else:
        sys.path.append(os.path.join(os.getcwd(),
                                     'backend'))
from multiprocessing import Pool

class Main():
        '''
        Основной класс. args, подаваемый иниту на вход, не обязательно
        должен формироваться argparse. Этим объектом может быть экземпляр
        класса из стороннего Python-модуля, в т.ч. имеющего отношение к GUI.
        Кстати, написание сообществом всевозможных графических интерфейсов
        к high-perf-bio люто, бешено приветствуется! В ините на основе args
        создаются как атрибуты, используемые распараллеливаемой функцией,
        так и атрибуты, нужные для кода, её запускающего. Что касается этой
        функции, её можно запросто пристроить в качестве коллбэка кнопки в GUI.
        '''
        def __init__(self, args):
                '''
                Получение атрибутов как для основной функции программы,
                так и для блока многопроцессового запуска таковой. Первые
                из перечисленных ни в коем случае не должны будут потом в
                параллельных процессах изменяться. Генерация набора чисел
                от 0 до (жёсткость - 1) в качестве равновозможных исходов.
                Случайный выбор числа из этого набора как единственного
                исхода, благоприятствующего прописыванию строки.
                '''
                self.src_file_path = os.path.normpath(args.src_file_path)
                self.src_file_name = os.path.basename(self.src_file_path)
                self.src_file_fmt = self.src_file_name.rsplit('.', maxsplit=2)[1]
                self.trg_dir_path = os.path.normpath(args.trg_dir_path)
                self.meta_lines_quan = args.meta_lines_quan
                self.thinning_lvl = args.thinning_lvl
                self.pos_outcomes = list(range(self.thinning_lvl))
                self.fav_outcome = random.choice(self.pos_outcomes)
                max_proc_quan = args.max_proc_quan
                trg_files_quan = args.trg_files_quan
                cpus_quan = os.cpu_count()
                if max_proc_quan > trg_files_quan <= cpus_quan:
                        self.proc_quan = trg_files_quan
                elif max_proc_quan > cpus_quan:
                        self.proc_quan = cpus_quan
                else:
                        self.proc_quan = max_proc_quan
                self.trg_file_names = [str(num + 1) + '_' +
                                       self.src_file_name for num in range(trg_files_quan)]
                
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
                                                
#Обработка аргументов командной строки.
#Создание экземпляра содержащего ключевую
#функцию класса. Параллельный запуск
#отбора строк. Замер времени выполнения
#вычислений с точностью до микросекунды.
if __name__ == '__main__':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = add_args_ru(__version__)
        else:
                args = add_args_en(__version__)
        main = Main(args)
        proc_quan = main.proc_quan
        print(f'\nGenerating test files based on {main.src_file_name}')
        print(f'\trigidity: {main.thinning_lvl}')
        print(f'\tquantity of parallel processes: {proc_quan}')
        with Pool(proc_quan) as pool_obj:
                exec_time_start = datetime.datetime.now()
                pool_obj.map(main.thin, main.trg_file_names)
                exec_time = datetime.datetime.now() - exec_time_start
        print(f'\tparallel computation time: {exec_time}')
