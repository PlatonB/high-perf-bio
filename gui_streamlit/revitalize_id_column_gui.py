__version__ = 'v1.0'

import streamlit as st
from descriptions.revitalize_id_column_descr import RevitalizeIdColumnDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='revitalize_id_column'):
                        st.header(body='revitalize_id_column')
                        with st.expander(label='description'):
                                st.text(body=RevitalizeIdColumnDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Путь к папке со сжатыми исходными таблицами (src-BED и src-TSV не поддерживаются программой)')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Имя БД, содержащей коллекцию с (rs)ID-полем (src-db-BED и src-db-TSV не поддерживаются программой)')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Путь к папке для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', min_value=1, value=4, format='%d',
                                                             help='Максимальное количество параллельно обогащаемых таблиц')
                        self.ignore_unrev_lines = st.checkbox(label='ignore-unrev-lines',
                                                              help='Не прописывать строки, не обогащённые rsID')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='revitalize_id_column'):
                        st.header(body='revitalize_id_column')
                        with st.expander(label='description'):
                                st.text(body=RevitalizeIdColumnDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Path to directory with gzipped source tables (src-BED and src-TSV are not supported by the program)')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Name of DB containing collection with (rs)ID field (src-db-BED and src-db-TSV are not supported by the program)')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Path to directory for results')
                        st.subheader(body='Optional widgets')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', min_value=1, value=4, format='%d',
                                                             help='Maximum quantity of tables enriched in parallel')
                        self.ignore_unrev_lines = st.checkbox(label='ignore-unrev-lines',
                                                              help="Don't write lines that not enriched by rsID")
                        self.submit = st.form_submit_button(label='run')
