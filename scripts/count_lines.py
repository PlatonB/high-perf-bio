from common_errors import DifFmtsError
__version__ = 'v1.1'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2022']

import sys
import os
import locale
import datetime
import gzip
import json
sys.dont_write_bytecode = True
if __name__ == '__main__':
    sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                 'cli'))
    sys.path.append(os.path.join(os.path.dirname(os.getcwd()),
                                 'backend'))
    from count_lines_cli import add_args_ru, add_args_en
else:
    sys.path.append(os.path.join(os.getcwd(),
                                 'backend'))


class Main():
    '''
    Основной класс. args, подаваемый иниту на вход, не обязательно
    должен формироваться argparse. Этим объектом может быть экземпляр
    класса из стороннего Python-модуля, в т.ч. имеющего отношение к GUI.
    Кстати, написание сообществом всевозможных графических интерфейсов
    к high-perf-bio люто, бешено приветствуется! В ините на основе args
    создаются как атрибуты, используемые главной функцией, так и атрибуты,
    нужные для кода, её запускающего. Что касается этой функции, её
    можно запросто пристроить в качестве коллбэка кнопки в GUI.
    '''

    def __init__(self, args, version):
        '''
        Получение атрибутов как для основной функции программы,
        так и для блока запуска таковой. Некоторые неочевидные,
        но важные детали об этих атрибутах. Дерево исходных папок
        и файлов. Здесь, в отличие от других компонентов тулкита,
        собираются не файлы одной папки, а вся иерархия вложенных
        в неё объектов файловой системы. Для каждой папки дерева
        формируется кортеж, состоящий из пути к этой папки, списка
        имён подпапок и списка имён файлов. Программа не ориентирована
        на подсчёт папок, поэтому понадобятся только первый и послений
        элементы кортежей. Расширение исходных таблиц. Ещё в ините
        проверяется, едино ли оно для всех них. Далее расширение
        пригодится для определения политики скипа метастрок и хэдера.
        '''
        self.src_dir_tree = sorted(os.walk(os.path.normpath(args.src_dir_path)))
        src_file_fmts = set(src_file_name.rsplit('.', maxsplit=2)[1]
                            for src_file_grp in [src_dir_info[2] for src_dir_info in self.src_dir_tree]
                            for src_file_name in src_file_grp)
        if len(src_file_fmts) > 1:
            raise DifFmtsError(src_file_fmts)
        self.src_file_fmt = list(src_file_fmts)[0]
        self.trg_dir_path = os.path.normpath(args.trg_dir_path)
        self.meta_lines_quan = args.meta_lines_quan
        if args.sel_col_nums not in [None, '']:
            self.sel_col_nums = list(map(lambda sel_col_num: int(sel_col_num),
                                         args.sel_col_nums.split(',')))
            self.sel_col_indexes = list(map(lambda sel_col_num: sel_col_num - 1,
                                            self.sel_col_nums))
        self.version = version

    def count_lines(self):
        '''
        Функция подсчёта строк таблиц, разбросанных по дереву папок.
        Не содержащие ни одного файла папки игнорируются. Результат
        вычисляется как для всех вместе файлов текущей папки, так и
        для каждого файла этой папки по-отдельности. Уникализация делается
        за счёт накопления строк, полных или частичных, во множества.
        '''
        trg_obj = {'tool_name': f'{os.path.basename(__file__)[:-3]},{self.version}'}
        if hasattr(self, 'sel_col_nums'):
            trg_obj['selected_column_nums'] = self.sel_col_nums
        trg_obj['dirs'] = {}
        for src_dir_info in self.src_dir_tree:
            src_dir_path = src_dir_info[0]
            src_file_grp = sorted(src_dir_info[2])
            if src_file_grp == []:
                continue
            src_files_quan = len(src_file_grp)
            trg_obj['dirs'][src_dir_path] = {'summary': {'files_quan': src_files_quan,
                                                         'lines_quan': 0,
                                                         'file_level_uniquized_lines_quan': 0,
                                                         'dir_level_uniquized_lines_quan': 0},
                                             'files': dict(zip(src_file_grp,
                                                               [{'lines_quan': 0,
                                                                 'uniquized_lines_quan': 0} for src_file_idx in range(src_files_quan)]))}
            dirlevel_uniquized_lines = set()
            for src_file_name in src_file_grp:
                src_file_path = os.path.join(src_dir_path,
                                             src_file_name)
                with gzip.open(src_file_path, mode='rt') as src_file_opened:
                    if self.src_file_fmt == 'vcf':
                        for line in src_file_opened:
                            if not line.startswith('##'):
                                break
                    else:
                        for meta_line_index in range(self.meta_lines_quan):
                            src_file_opened.readline()
                    filelevel_uniquized_lines = set()
                    src_lines_quan = 0
                    for src_line in src_file_opened:
                        if src_line == '\n':
                            continue
                        if hasattr(self, 'sel_col_indexes'):
                            src_row = src_line.rstrip().split('\t')
                            trg_line = '\t'.join([src_row[sel_col_idx] for sel_col_idx in self.sel_col_indexes])
                        else:
                            trg_line = src_line.rstrip()
                        filelevel_uniquized_lines.add(trg_line)
                        dirlevel_uniquized_lines.add(trg_line)
                        src_lines_quan += 1
                filelevel_uniquized_lines_quan = len(filelevel_uniquized_lines)
                trg_obj['dirs'][src_dir_path]['summary']['lines_quan'] += src_lines_quan
                trg_obj['dirs'][src_dir_path]['summary']['file_level_uniquized_lines_quan'] += filelevel_uniquized_lines_quan
                trg_obj['dirs'][src_dir_path]['files'][src_file_name]['lines_quan'] = src_lines_quan
                trg_obj['dirs'][src_dir_path]['files'][src_file_name]['uniquized_lines_quan'] = filelevel_uniquized_lines_quan
            trg_obj['dirs'][src_dir_path]['summary']['dir_level_uniquized_lines_quan'] = len(dirlevel_uniquized_lines)
        trg_file_name = f'dirtree-{os.path.basename(self.src_dir_tree[0][0])}__quan.json'
        with open(os.path.join(self.trg_dir_path, trg_file_name), 'w', encoding='utf-8') as trg_file_opened:
            json.dump(trg_obj, trg_file_opened, ensure_ascii=False, indent=4)


# Обработка аргументов командной строки. Создание
# экземпляра содержащего ключевую функцию класса.
# Запуск подсчёта. Замер времени выполнения
# вычислений с точностью до микросекунды.
if __name__ == '__main__':
    if locale.getdefaultlocale()[0][:2] == 'ru':
        args = add_args_ru(__version__,
                           __authors__)
    else:
        args = add_args_en(__version__,
                           __authors__)
    main = Main(args, __version__)
    print(f'\nCounting lines of tables from {os.path.basename(main.src_dir_tree[0][0])} dir tree')
    exec_time_start = datetime.datetime.now()
    main.count_lines()
    exec_time = datetime.datetime.now() - exec_time_start
    print(f'\tcomputation time: {exec_time}')
