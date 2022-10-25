__version__ = 'v2.0'

import sys, os, locale, datetime
sys.dont_write_bytecode = True
hpb_dir_path = os.getcwd()
sys.path.append(os.path.join(hpb_dir_path,
                             'gui_streamlit'))
sys.path.append(os.path.join(hpb_dir_path,
                             'plugins'))
sys.path.append(os.path.join(hpb_dir_path,
                             'scripts'))
import streamlit as st
import annotate_gui, concatenate_gui, count_gui, create_gui, \
       dock_gui, ljoin_gui, query_gui, reindex_gui, split_gui, \
       revitalize_id_column_gui, count_lines_gui, gen_test_files_gui
import annotate, concatenate, count, create, \
       dock, ljoin, query, reindex, split, \
       revitalize_id_column, count_lines, gen_test_files
from multiprocessing import Pool

#Кастомизируем заголовок окна
#браузера и 3 пункта гамбургер-меню.
st.set_page_config(page_title='high-perf-bio',
                   page_icon=':dna:',
                   menu_items={'Report a bug': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'Get help': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'About': '*high-perf-bio*: open-source toolkit that simplifies and speeds up work with bioinformatics data'})

#В боковой панели - тайтл для тех, кто ещё не понял, как тулкит называется,
#неприметная надпись с версией этого GUI, а также выпадающие списки с
#категориями (главная, плагины, скрипты) и компонентами. Т.к. работа
#должна начинаться с создания БД, по умолчанию отображаются главная
#категория, пункт create и соответствующая последнему псевдо-вкладка.
with st.sidebar:
        st.title(body='high-perf-bio')
        st.caption(body=f'GUI {__version__}')
        tool_category = st.selectbox(label='tool-category',
                                     options=['core', 'plugins', 'scripts'])
        if tool_category == 'core':
                tool_names = ['annotate', 'concatenate', 'count',
                              'create', 'dock', 'ljoin',
                              'query', 'reindex', 'split']
                default_tool_idx = 3
        elif tool_category == 'plugins':
                tool_names = ['revitalize_id_column']
                default_tool_idx = 0
        elif tool_category == 'scripts':
                tool_names = ['count_lines', 'gen_test_files']
                default_tool_idx = 0
        tool_name = st.selectbox(label='tool-name',
                                 options=tool_names,
                                 index=default_tool_idx)
        
#Создание экземпляра класса, поглощающего сигналы
#от виджетов выбранного тула. Набор атрибутов почти
#полностью аналогичен таковому у объекта, хранящего
#командные агрументы. Подача GUI-объекта в инит
#основного класса соответствующего инструмента.
#Запуск параллельных вычислений, вывод уведомлений
#о ходе работы. В конце - приятная пасхалочка:).
if tool_name == 'annotate':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = annotate_gui.AddWidgetsRu(annotate.__version__)
        else:
                args = annotate_gui.AddWidgetsEn(annotate.__version__)
        if args.submit:
                main = annotate.Main(args, annotate.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Annotation by {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.annotate, main.src_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'concatenate':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = concatenate_gui.AddWidgetsRu(concatenate.__version__)
        else:
                args = concatenate_gui.AddWidgetsEn(concatenate.__version__)
        if args.submit:
                main = concatenate.Main(args, concatenate.__version__)
                with st.spinner(f'Concatenating {main.src_db_name} DB'):
                        exec_time_start = datetime.datetime.now()
                        main.concatenate()
                        exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'computation time: {exec_time}')
                st.balloons()
if tool_name == 'count':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = count_gui.AddWidgetsRu(count.__version__)
        else:
                args = count_gui.AddWidgetsEn(count.__version__)
        if args.submit:
                main = count.Main(args, count.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Counting sets of related values in {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.count, main.src_coll_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'create':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = create_gui.AddWidgetsRu(create.__version__)
        else:
                args = create_gui.AddWidgetsEn(create.__version__)
        if args.submit:
                main = create.Main(args, create.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Replenishment and indexing {main.trg_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.create_collection, main.src_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'dock':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = dock_gui.AddWidgetsRu(dock.__version__)
        else:
                args = dock_gui.AddWidgetsEn(dock.__version__)
        if args.submit:
                main = dock.Main(args, dock.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Annotation by {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.dock, main.src_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'ljoin':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = ljoin_gui.AddWidgetsRu(ljoin.__version__)
        else:
                args = ljoin_gui.AddWidgetsEn(ljoin.__version__)
        if args.submit:
                main = ljoin.Main(args, ljoin.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'{main.action.capitalize()}ing collections of {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        st.text(f'coverage: {main.coverage}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.intersect_subtract, main.left_coll_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'query':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = query_gui.AddWidgetsRu(query.__version__)
        else:
                args = query_gui.AddWidgetsEn(query.__version__)
        if args.submit:
                main = query.Main(args, query.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Queriing by {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.search, main.src_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'reindex':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = reindex_gui.AddWidgetsRu(reindex.__version__)
        else:
                args = reindex_gui.AddWidgetsEn(reindex.__version__)
        if args.submit:
                main = reindex.Main(args)
                src_db_name = main.src_db_name
                if main.del_ind_names not in [None, '']:
                        with st.spinner(f'Removing indexes from {src_db_name} DB'):
                                main.del_indices()
                if main.index_models not in [None, '']:
                        proc_quan = main.proc_quan
                        with st.spinner(f'Indexing {src_db_name} DB'):
                                st.text(f'quantity of parallel processes: {proc_quan}')
                                with Pool(proc_quan) as pool_obj:
                                        exec_time_start = datetime.datetime.now()
                                        pool_obj.map(main.add_indices, main.src_coll_names)
                                        exec_time = datetime.datetime.now() - exec_time_start
                        st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'split':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = split_gui.AddWidgetsRu(split.__version__)
        else:
                args = split_gui.AddWidgetsEn(split.__version__)
        if args.submit:
                main = split.Main(args, split.__version__)
                proc_quan = main.proc_quan
                with st.spinner(f'Splitting collections of {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.split, main.src_coll_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'revitalize_id_column':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = revitalize_id_column_gui.AddWidgetsRu(revitalize_id_column.__version__)
        else:
                args = revitalize_id_column_gui.AddWidgetsEn(revitalize_id_column.__version__)
        if args.submit:
                main = revitalize_id_column.Main(args)
                proc_quan = main.proc_quan
                with st.spinner(f'ID column reconstruction by {main.src_db_name} DB'):
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.revitalize, main.src_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
if tool_name == 'count_lines':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = count_lines_gui.AddWidgetsRu(count_lines.__version__)
        else:
                args = count_lines_gui.AddWidgetsEn(count_lines.__version__)
        if args.submit:
                main = count_lines.Main(args, count_lines.__version__)
                with st.spinner(f'Counting lines of tables from {os.path.basename(main.src_dir_tree[0][0])} dir tree'):
                        exec_time_start = datetime.datetime.now()
                        main.count_lines()
                        exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'computation time: {exec_time}')
                st.balloons()
if tool_name == 'gen_test_files':
        if locale.getdefaultlocale()[0][:2] == 'ru':
                args = gen_test_files_gui.AddWidgetsRu(gen_test_files.__version__)
        else:
                args = gen_test_files_gui.AddWidgetsEn(gen_test_files.__version__)
        if args.submit:
                main = gen_test_files.Main(args)
                proc_quan = main.proc_quan
                with st.spinner(f'Generating test files based on {main.src_file_name}'):
                        st.text(f'rigidity: {main.thinning_lvl}')
                        st.text(f'quantity of parallel processes: {proc_quan}')
                        with Pool(proc_quan) as pool_obj:
                                exec_time_start = datetime.datetime.now()
                                pool_obj.map(main.thin, main.trg_file_names)
                                exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'parallel computation time: {exec_time}')
                st.balloons()
