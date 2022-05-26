__version__ = 'v0.5-beta'

import sys, os, locale, datetime
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.getcwd(),
                             'gui_streamlit'))
import streamlit as st
import annotate_gui, concatenate_gui, count_gui, create_gui, \
       dock_gui, ljoin_gui, query_gui, split_gui
import annotate, concatenate, count, create, \
       dock, ljoin, query, split
from multiprocessing import Pool

#Кастомизируем заголовок окна
#браузера и 3 пункта гамбургер-меню.
st.set_page_config(page_title='high-perf-bio',
                   page_icon=':dna:',
                   menu_items={'Report a bug': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'Get help': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'About': '*high-perf-bio*: open-source toolkit that simplifies and speeds up work with bioinformatics data'})

#Вдруг кто до сих пор ещё не
#понял, как тулкит называется?:)
st.title(body='high-perf-bio')

#Неприметная надпись с версией этого GUI.
st.caption(body=f'GUI {__version__}')

#Создаём меню выбора компонента тулкита.
#По умолчанию отображается пункт create
#и соответствующая псевдо-вкладка, т.к.
#работа должна начинаться с создания БД.
tool_name = st.selectbox(label='tool-name',
                         options=['annotate', 'concatenate',
                                  'count', 'create',
                                  'dock', 'ljoin',
                                  'query', 'split'],
                         index=3)

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
                with st.spinner(f'Counting sets of related values in {main.src_db_name} DB'):
                        exec_time_start = datetime.datetime.now()
                        main.count()
                        exec_time = datetime.datetime.now() - exec_time_start
                st.success(f'computation time: {exec_time}')
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
