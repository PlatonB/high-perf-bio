__version__ = 'v3.1'

import streamlit as st
from descriptions.ljoin_descr import LjoinDescr
from pymongo import MongoClient

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='ljoin'):
                        st.header(body='ljoin')
                        with st.expander(label='description'):
                                st.text(body=LjoinDescr(version, authors).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_db_name = st.selectbox(label='src-db-name',
                                                        options=MongoClient().list_database_names(),
                                                        help='Имя БД, по которой выполнять работу')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Путь к папке для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.left_coll_names = st.text_input(label='left-coll-names',
                                                             help='Имена левых коллекций (через запятую без пробела; [[все коллекции]])')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Максимальное количество параллельно обрабатываемых левых коллекций')
                        self.right_coll_names = st.text_input(label='right-coll-names',
                                                              help='Имена правых коллекций (через запятую без пробела; [[все коллекции]]; правая, совпадающая с текущей левой, проигнорируется)')
                        self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                               help='''Пересекать или вычитать по геномной локации (экспериментальная фича; src-db-TSV: не применяется).
Пересекать или вычитать ID, уточняя по аллелям совпадения ID (экспериментальная фича; src-db-TSV/BED: не применяется)''')
                        self.lookup_field_path = st.text_input(label='lookup-field-path',
                                                               help='Точечный путь к полю, по которому пересекать или вычитать (применяется без preset; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[первое после _id поле]])')
                        self.action = st.radio(label='action', options=['intersect', 'subtract'],
                                               help='Пересекать или вычитать')
                        self.coverage = st.number_input(label='coverage', value=1, min_value=0, format='%d',
                                                        help='Охват (1 <= c <= количество правых; 0 - приравнять к количеству правых; вычтется 1, если правые и левые совпадают при 1 < c = количество правых)')
                        self.srt_field_group = st.text_input(label='srt-field-group',
                                                             help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
                        self.srt_order = st.radio(label='srt-order', options=['asc', 'desc'],
                                                  help='Порядок сортировки (применяется с srt-field-group)')
                        self.proj_field_names = st.text_input(label='proj-field-names',
                                                              help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-TSV; поле _id не выведется)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                                      help='Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, version, authors):
                with st.form(key='ljoin'):
                        st.header(body='ljoin')
                        with st.expander(label='description'):
                                st.text(body=LjoinDescr(version, authors).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_db_name = st.selectbox(label='src-db-name',
                                                        options=MongoClient().list_database_names(),
                                                        help='Name of the DB to work on')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Path to directory for results')
                        st.subheader(body='Optional widgets')
                        self.left_coll_names = st.text_input(label='left-coll-names',
                                                             help='Left collection names (comma separated without spaces; [[all collections]])')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Maximum quantity of parallel processed left collections')
                        self.right_coll_names = st.text_input(label='right-coll-names',
                                                              help='Right collection names (comma separated without spaces; [[all collections]]; the right one, which matches with current left one, will be ignored)')
                        self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                               help='''Intersect or subtract by genomic location (experimental feature; src-db-TSV: not applicable).
Intersect or subtract ID, verifying by alleles ID matches (experimental feature; src-db-TSV/BED: not applicable)''')
                        self.lookup_field_path = st.text_input(label='lookup-field-path',
                                                               help='Dot path to the field by which to intersect or subtract (applicable without preset; src-db-VCF: [[ID]]; src-db-BED: [[name]], src-db-TSV: [[first field after _id]])')
                        self.action = st.radio(label='action', options=['intersect', 'subtract'],
                                               help='Intersect or subtract')
                        self.coverage = st.number_input(label='coverage', value=1, min_value=0, format='%d',
                                                        help='Coverage (1 <= c <= quantity of rights; 0 - equate to the quantity of rights; it will be deducted 1 if rights and lefts match when 1 < c = quantity of rights)')
                        self.srt_field_group = st.text_input(label='srt-field-group',
                                                             help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF: [[#CHROM+POS]]; src-db-BED: [[chrom+start+end]]; src-db-VCF, src-db-BED: trg-TSV)')
                        self.srt_order = st.radio(label='srt-order', options=['asc', 'desc'],
                                                  help='Order of sorting (applicable with srt-field-group)')
                        self.proj_field_names = st.text_input(label='proj-field-names',
                                                              help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-TSV; _id field will not be output)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                                      help='Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
                        self.submit = st.form_submit_button(label='run')
