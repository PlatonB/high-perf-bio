__version__ = 'v0.1-alpha'

import sys, os, locale, datetime
sys.dont_write_bytecode = True
sys.path.append(os.path.join(os.getcwd(),
                             'gui_streamlit'))
import streamlit as st
import annotate_gui, create_gui
import annotate, create
from multiprocessing import Pool

#Кастомизируем заголовок окна
#браузера и 3 пункта гамбургер-меню.
st.set_page_config(page_title='high-perf-bio',
                   page_icon=':dna:',
                   menu_items={'Report a bug': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'Get help': 'https://github.com/PlatonB/high-perf-bio/issues',
                               'About': '*high-perf-bio*: open-source toolkit that simplifies and speeds up work with bioinformatics data'})

#Создаём меню выбора компонента тулкита.
#По умолчанию отображается пункт create
#и соответствующая псевдо-вкладка, т.к.
#работа должна начинаться с создания БД.
tool_name = st.selectbox(label='tool-name',
                         options=['annotate',
                                  'create'],
                         index=1)

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
