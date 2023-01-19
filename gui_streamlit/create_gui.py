__version__ = 'v3.0'

import streamlit as st
from descriptions.create_descr import CreateDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='create'):
                        st.header(body='create')
                        with st.expander(label='description'):
                                st.text(body=CreateDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Путь к папке со сжатыми таблицами, преобразуемыми в коллекции MongoDB-базы')
                        st.subheader(body='Необязательные виджеты')
                        self.trg_db_name = st.text_input(label='trg-db-name',
                                                         help='Имя пополняемой базы данных ([[имя папки со сжатыми таблицами]])')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Максимальное количество параллельно загружаемых таблиц/индексируемых коллекций')
                        self.if_db_exists = st.radio(label='if-db-exists', options=['', 'rewrite', 'replenish'],
                                                     help='Стереть имеющуюся БД или пополнить её данными из новых файлов папки (не допускайте смешивания форматов в одной БД)')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Количество строк метаинформации (src-VCF: не применяется; src-BED: включите шапку (если есть); src-TSV: не включайте шапку (если есть), либо применяйте arbitrary-header)')
                        self.arbitrary_header = st.text_input(label='arbitrary-header',
                                                              help='Произвольная шапка исходных таблиц (через \\t без пробела; src-VCF, src-BED: не применяется)')
                        self.minimal = st.checkbox(label='minimal', value=False,
                                                   help='Загружать только минимально допустимый форматом набор столбцов (src-VCF: 1-ые 8; src-BED: 1-ые 3; src-TSV: не применяется)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['', 'colon', 'comma', 'low_line', 'pipe', 'semicolon'],
                                                      help='Знак препинания для разбиения ячейки на список (src-VCF, src-BED: не применяется)')
                        self.max_fragment_len = st.number_input(label='max-fragment-len', value=100000, min_value=1, format='%d',
                                                                help='Максимальное количество строк фрагмента заливаемой таблицы')
                        self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                              help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]])')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='create'):
                        st.header(body='create')
                        with st.expander(label='description'):
                                st.text(body=CreateDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_dir_path = st.text_input(label='src-dir-path',
                                                          help='Path to directory with gzipped tables which will be converted to collections of MongoDB database')
                        st.subheader(body='Optional widgets')
                        self.trg_db_name = st.text_input(label='trg-db-name',
                                                         help='Name of the replenished DB ([[compressed tables directory name]])')
                        self.append = st.checkbox(label='append', value=False,
                                                  help='Permit addition of data to the existing DB (do not allow mixing of formats in the same DB; only new files of the directory will be uploaded)')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Maximum quantity of parallel uploaded tables/indexed collections')
                        self.if_db_exists = st.radio(label='if-db-exists', options=['', 'rewrite', 'replenish'],
                                                     help='Erase the target DB or replenish it with data from new files of the directory (do not allow mixing of formats in the same DB)')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Quantity of metainformation lines (src-VCF: not applicable; src-BED: include a header (if available); src-TSV: do not include a header (if available), otherwise use arbitrary-header)')
                        self.arbitrary_header = st.text_input(label='arbitrary-header',
                                                              help='Arbitrary header of source tables (\\t-separated without spaces; src-VCF, src-BED: not applicable)')
                        self.minimal = st.checkbox(label='minimal', value=False,
                                                   help='Upload only the minimum set of columns allowed by the format (src-VCF: 1st 8; src-BED: 1st 3; src-TSV: not applicable)')
                        self.sec_delimiter = st.radio(label='sec-delimiter', options=['', 'colon', 'comma', 'low_line', 'pipe', 'semicolon'],
                                                      help='Punctuation mark for dividing a cell to a list (src-VCF, src-BED: not applicable)')
                        self.max_fragment_len = st.number_input(label='max-fragment-len', value=100000, min_value=1, format='%d',
                                                                help='Maximum quantity of rows of uploaded table fragment')
                        self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                              help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]])')
                        self.submit = st.form_submit_button(label='run')
