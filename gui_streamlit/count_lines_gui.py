__version__ = 'v1.1'

import streamlit as st
from descriptions.count_lines_descr import CountLinesDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='count_lines'):
                        st.header(body='count_lines')
                        with st.expander(label='description'):
                                st.text(body=CountLinesDescr(version, authors).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Путь к корню дерева папок со сжатыми таблицами')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Путь к папке для конечного JSON')
                        st.subheader(body='Необязательные виджеты')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Количество строк метаинформации (src-VCF: не применяется; src-BED, src-TSV: включите шапку, если она есть)')
                        self.sel_col_nums = st.text_input(label='sel-col-nums',
                                                          help='Номера отбираемых столбцов (через запятую без пробела; [[строка целиком]]; влияет только на результат подсчёта без дублей)')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='count_lines'):
                        st.header(body='count_lines')
                        with st.expander(label='description'):
                                st.text(body=CountLinesDescr(version, authors).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Path to root of directory tree with gzipped tables')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Path to directory for target JSON')
                        st.subheader(body='Optional widgets')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Quantity of metainformation lines (src-VCF: not applicable; src-BED, src-TSV: include a header, if there is one)')
                        self.sel_col_nums = st.text_input(label='sel-col-nums',
                                                          help='Numbers of selected columns (comma separated without spaces; [[whole line]]; affects only count result without duplicates)')
                        self.submit = st.form_submit_button(label='run')
