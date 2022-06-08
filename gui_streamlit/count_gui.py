__version__ = 'v1.1'

import streamlit as st
from descriptions.count_descr import CountDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='count'):
                        st.header(body='count')
                        with st.expander(label='description'):
                                st.text(body=CountDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Имя анализируемой БД')
                        self.trg_place = st.text_input(label='trg-place',
                                                       help='Путь к папке или имя БД для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                               help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
                        self.cnt_field_paths = st.text_input(label='cnt-field-paths',
                                                             help='''Точечные пути к полям, для которых считать количество каждого набора взаимосвязанных значений (через запятую без пробела;
src-db-VCF: [[ID,REF,ALT]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])''')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=2,
                                                      help='Знак препинания для восстановления ячейки из списка')
                        self.quan_thres = st.number_input(label='quan-thres', min_value=1, format='%d',
                                                          help='Нижняя граница количества каждого набора (применяется без freq-thres)')
                        self.samp_quan = st.number_input(label='samp-quan', min_value=0, format='%d',
                                                         help='Количество образцов (нужно для расчёта частоты каждого набора)')
                        self.freq_thres = st.text_input(label='freq-thres',
                                                        help='Нижняя граница частоты каждого набора (применяется с samp-quan; применяется без quan-thres)')
                        self.quan_sort_order = st.radio(label='quan-sort-order', options=['asc', 'desc'], index=1,
                                                        help='Порядок сортировки по количеству каждого набора')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='count'):
                        st.header(body='count')
                        with st.expander(label='description'):
                                st.text(body=CountDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Name of the analyzed DB')
                        self.trg_place = st.text_input(label='trg-place',
                                                       help='Path to directory or name of the DB for results')
                        st.subheader(body='Optional widgets')
                        self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                               help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
                        self.cnt_field_paths = st.text_input(label='cnt-field-paths',
                                                             help='''Dot paths to fields for which to count the quantity of each set of related values (comma separated without spaces;
src-db-VCF: [[ID,REF,ALT]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])''')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=2,
                                                      help='Punctuation mark to restore a cell from a list')
                        self.quan_thres = st.number_input(label='quan-thres', min_value=1, format='%d',
                                                          help='Lower threshold of quantity of each set (applicable without freq-thres)')
                        self.samp_quan = st.number_input(label='samp-quan', min_value=0, format='%d',
                                                         help='Quantity of samples (required to calculate the frequency of each set)')
                        self.freq_thres = st.text_input(label='freq-thres',
                                                        help='Lower threshold of frequency of each set (applicable with samp-quan; applicable without quan-thres)')
                        self.quan_sort_order = st.radio(label='quan-sort-order', options=['asc', 'desc'], index=1,
                                                        help='Order of sorting by quantity of each set')
                        self.submit = st.form_submit_button(label='run')
