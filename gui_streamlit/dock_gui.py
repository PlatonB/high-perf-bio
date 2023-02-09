__version__ = 'v3.1'

import streamlit as st
from descriptions.dock_descr import DockDescr
from pymongo import MongoClient

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='dock'):
                        st.header(body='dock')
                        with st.expander(label='description'):
                                st.text(body=DockDescr(version, authors).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Путь к папке со сжатыми аннотируемыми таблицами')
                        self.src_db_name = st.selectbox(label='src-db-name', options=MongoClient().list_database_names(),
                                                        help='Имя БД, по которой аннотировать')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Путь к папке для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Максимальное количество параллельно аннотируемых таблиц')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку)')
                        self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                               help='''Аннотировать, пересекая по геномной локации (src-TSV, src-db-TSV: не применяется).
Аннотировать ID по ID, уточняя по аллелям совпадения ID (src-TSV/BED, src-db-TSV/BED: не применяется)''')
                        self.ann_col_num = st.number_input(label='ann-col-num', min_value=0, format='%d',
                                                           help='Номер аннотируемого столбца (применяется без preset; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
                        self.ann_field_path = st.text_input(label='ann-field-path',
                                                            help='Точечный путь к полю, по которому аннотировать (применяется без preset; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])')
                        self.proj_field_names = st.text_input(label='proj-field-names',
                                                              help='Отбираемые поля верхнего уровня и/или столбцы (через запятую без пробела; имя_столбца_f; поле _id не выведется)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                                      help='Знак препинания для восстановления ячейки из списка')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='dock'):
                        st.header(body='dock')
                        with st.expander(label='description'):
                                st.text(body=DockDescr(version, authors).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Path to directory with gzipped tables, which will be annotated')
                        self.src_db_name = st.selectbox(label='src-db-name', options=MongoClient().list_database_names(),
                                                        help='Name of the DB by which to annotate')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Path to directory for results')
                        st.subheader(body='Optional widgets')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Maximum quantity of parallel annotated tables')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Quantity of metainformation lines of annotated tables (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header)')
                        self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                               help='''Annotate via intersection by genomic location (src-TSV, src-db-TSV: not applicable).
Annotate ID by ID, verifying by alleles ID matches (src-TSV/BED, src-db-TSV/BED: not applicable)''')
                        self.ann_col_num = st.number_input(label='ann-col-num', min_value=0, format='%d',
                                                           help='Number of the annotated column (applicable without preset; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
                        self.ann_field_path = st.text_input(label='ann-field-path',
                                                            help='Dot path to the field by which to annotate (applicable without preset; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])')
                        self.proj_field_names = st.text_input(label='proj-field-names',
                                                              help='Selected top level fields and/or columns (comma separated without spaces; column_name_f; _id field will not be output)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                                      help='Punctuation mark to restore a cell from a list')
                        self.submit = st.form_submit_button(label='run')
