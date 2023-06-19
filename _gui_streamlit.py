# autopep8: off
import sys
import pathlib
import os
import locale
import datetime
sys.dont_write_bytecode = True
hpb_dir_path = pathlib.Path(__file__).parent
sys.path.append(hpb_dir_path.joinpath('gui_streamlit').as_posix())
sys.path.append(hpb_dir_path.joinpath('plugins').as_posix())
sys.path.append(hpb_dir_path.joinpath('scripts').as_posix())
sys.path.append(hpb_dir_path.joinpath('backend').as_posix())
import streamlit as st
import annotate_gui
import concatenate_gui
import count_gui
import create_gui
import dock_gui
import ljoin_gui
import query_gui
import reindex_gui
import split_gui
import revitalize_id_column_gui
import count_lines_gui
import gen_test_files_gui
import annotate
import concatenate
import count
import create
import dock
import ljoin
import query
import reindex
import split
import revitalize_id_column
import count_lines
import gen_test_files
from parallelize import parallelize
# autopep8: on

__version__ = 'v2.3'
__authors__ = ['Platon Bykadorov (platon.work@gmail.com), 2022-2023']

# Кастомизируем заголовок окна
# браузера и 3 пункта гамбургер-меню.
st.set_page_config(page_title='high-perf-bio',
                   page_icon=':dna:',
                   menu_items={'Report a bug': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'Get help': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'About': '*high-perf-bio*: open-source toolkit that simplifies and speeds up work with bioinformatics data'})

# В боковой панели - тайтл для тех, кто ещё не понял, как тулкит называется,
# неприметная надпись с версией этого GUI, а также выпадающие списки с
# категориями (главная, плагины, скрипты) и компонентами. Т.к. работа
# должна начинаться с создания БД, по умолчанию отображаются главная
# категория, пункт create и соответствующая последнему псевдо-вкладка.
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

# Создание экземпляра класса, поглощающего сигналы
# от виджетов выбранного тула. Набор атрибутов почти
# полностью аналогичен таковому у объекта, хранящего
# командные агрументы. Подача GUI-объекта в инит
# основного класса соответствующего инструмента.
# Запуск параллельных вычислений, вывод уведомлений
# о ходе работы. В конце - приятная пасхалочка:).
if tool_name == 'annotate':
    version, authors = annotate.__version__, annotate.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = annotate_gui.AddWidgetsRu(version, authors)
    else:
        inp = annotate_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = annotate.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Annotation by {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.annotate,
                                    main.src_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'concatenate':
    version, authors = concatenate.__version__, concatenate.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = concatenate_gui.AddWidgetsRu(version, authors)
    else:
        inp = concatenate_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = concatenate.Main(inp, version)
        with st.spinner(f'Concatenating {main.src_db_name} DB'):
            exec_time_start = datetime.datetime.now()
            main.concatenate()
            exec_time = datetime.datetime.now() - exec_time_start
        st.success(f'computation time: {exec_time}')
        st.balloons()
if tool_name == 'count':
    version, authors = count.__version__, count.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = count_gui.AddWidgetsRu(version, authors)
    else:
        inp = count_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = count.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Counting sets of related values in {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.count,
                                    main.src_coll_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'create':
    version, authors = create.__version__, create.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = create_gui.AddWidgetsRu(version, authors)
    else:
        inp = create_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = create.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Replenishment and indexing {main.trg_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.create_collection,
                                    main.src_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'dock':
    version, authors = dock.__version__, dock.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = dock_gui.AddWidgetsRu(version, authors)
    else:
        inp = dock_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = dock.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Annotation by {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.dock,
                                    main.src_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'ljoin':
    version, authors = ljoin.__version__, ljoin.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = ljoin_gui.AddWidgetsRu(version, authors)
    else:
        inp = ljoin_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = ljoin.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'{main.action.capitalize()}ing collections of {main.src_db_name} DB'):
            st.text(f'coverage: {main.coverage}')
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.intersect_subtract,
                                    main.left_coll_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'query':
    version, authors = query.__version__, query.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = query_gui.AddWidgetsRu(version, authors)
    else:
        inp = query_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = query.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Queriing by {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.search,
                                    main.src_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'reindex':
    version, authors = reindex.__version__, reindex.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = reindex_gui.AddWidgetsRu(version, authors)
    else:
        inp = reindex_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = reindex.Main(inp)
        src_db_name = main.src_db_name
        if main.del_ind_names not in [None, '']:
            with st.spinner(f'Removing indexes from {src_db_name} DB'):
                main.del_indices()
        if main.index_models not in [None, '']:
            proc_quan = main.proc_quan
            with st.spinner(f'Indexing {src_db_name} DB'):
                st.text(f'quantity of parallel processes: {proc_quan}')
                exec_time = parallelize(proc_quan, main.add_indices,
                                        main.src_coll_names)
            st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'split':
    version, authors = split.__version__, split.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = split_gui.AddWidgetsRu(version, authors)
    else:
        inp = split_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = split.Main(inp, version)
        proc_quan = main.proc_quan
        with st.spinner(f'Splitting collections of {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.split,
                                    main.src_coll_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'revitalize_id_column':
    version, authors = revitalize_id_column.__version__, revitalize_id_column.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = revitalize_id_column_gui.AddWidgetsRu(version, authors)
    else:
        inp = revitalize_id_column_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = revitalize_id_column.Main(inp)
        proc_quan = main.proc_quan
        with st.spinner(f'ID column reconstruction by {main.src_db_name} DB'):
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.revitalize,
                                    main.src_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
if tool_name == 'count_lines':
    version, authors = count_lines.__version__, count_lines.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = count_lines_gui.AddWidgetsRu(version, authors)
    else:
        inp = count_lines_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = count_lines.Main(inp, version)
        with st.spinner(f'Counting lines of tables from {os.path.basename(main.src_dir_tree[0][0])} dir tree'):
            exec_time_start = datetime.datetime.now()
            main.count_lines()
            exec_time = datetime.datetime.now() - exec_time_start
        st.success(f'computation time: {exec_time}')
        st.balloons()
if tool_name == 'gen_test_files':
    version, authors = gen_test_files.__version__, gen_test_files.__authors__
    if locale.getdefaultlocale()[0][:2] == 'ru':
        inp = gen_test_files_gui.AddWidgetsRu(version, authors)
    else:
        inp = gen_test_files_gui.AddWidgetsEn(version, authors)
    if inp.submit:
        main = gen_test_files.Main(inp)
        proc_quan = main.proc_quan
        with st.spinner(f'Generating test files based on {main.src_file_name}'):
            st.text(f'rigidity: {main.thinning_lvl}')
            st.text(f'quantity of parallel processes: {proc_quan}')
            exec_time = parallelize(proc_quan, main.thin,
                                    main.trg_file_names)
        st.success(f'parallel computation time: {exec_time}')
        st.balloons()
