import streamlit as st
from pymongo import MongoClient
from descriptions.annotate_descr import AnnotateDescr

__version__ = 'v5.1'


class AddWidgetsRu():
    '''
    Создание Streamlit-интерфейса.
    '''

    def __init__(self, version, authors):
        with st.form(key='annotate'):
            st.header(body='annotate')
            with st.expander(label='description'):
                st.text(body=AnnotateDescr(version, authors).ru)
            st.subheader(body='Обязательные виджеты')
            self.src_dir_path = st.text_input(label='src-dir-path',
                                              help='Путь к папке со сжатыми аннотируемыми таблицами')
            self.src_db_name = st.selectbox(label='src-db-name', options=MongoClient().list_database_names(),
                                            help='Имя БД, по которой аннотировать')
            self.trg_place = st.text_input(label='trg-place',
                                           help='Путь к папке или имя БД для результатов')
            st.subheader(body='Необязательные виджеты')
            self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                 help='Максимальное количество параллельно аннотируемых таблиц')
            self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                   help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
            self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                   help='Количество строк метаинформации аннотируемых таблиц (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
            self.extra_query = st.text_input(label='extra-query', value='{}',
                                             help='''Дополнительный запрос ко всем коллекциям БД (синтаксис PyMongo;
{"$and": [ваш_запрос]} при необходимости экранирования имени запрашиваемого поля или оператора $or;
примеры указания типа данных: "any_str", Decimal128("any_str"))''')
            self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                   help='''Аннотировать, пересекая по геномной локации (экспериментальная фича; src-TSV, src-db-TSV: не применяется).
Аннотировать ID по ID, уточняя по аллелям совпадения ID (экспериментальная фича; src-TSV/BED, src-db-TSV/BED: не применяется)''')
            self.ann_col_num = st.number_input(label='ann-col-num', min_value=0, format='%d',
                                               help='Номер аннотируемого столбца (применяется без preset; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
            self.ann_field_path = st.text_input(label='ann-field-path',
                                                help='Точечный путь к полю, по которому аннотировать (применяется без preset; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[первое после _id поле]])')
            self.srt_field_group = st.text_input(label='srt-field-group',
                                                 help='Точечные пути к сортируемым полям (через плюс без пробела; src-db-VCF, src-db-BED: trg-(db-)TSV)')
            self.srt_order = st.radio(label='srt-order', options=['asc', 'desc'],
                                      help='Порядок сортировки (применяется с srt-field-group)')
            self.proj_field_names = st.text_input(label='proj-field-names',
                                                  help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-(db-)TSV; поле _id не выведется)')
            self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                          help='Знак препинания для восстановления ячейки из списка (src-db-VCF, src-db-BED (trg-BED): не применяется)')
            self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                  help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[первое после _id поле]])')
            self.submit = st.form_submit_button(label='run')


class AddWidgetsEn():
    '''
    Создание Streamlit-интерфейса.
    '''

    def __init__(self, version, authors):
        with st.form(key='annotate'):
            st.header(body='annotate')
            with st.expander(label='description'):
                st.text(body=AnnotateDescr(version, authors).en)
            st.subheader(body='Mandatory widgets')
            self.src_dir_path = st.text_input(label='src-dir-path',
                                              help='Path to directory with gzipped tables, which will be annotated')
            self.src_db_name = st.selectbox(label='src-db-name', options=MongoClient().list_database_names(),
                                            help='Name of the DB by which to annotate')
            self.trg_place = st.text_input(label='trg-place',
                                           help='Path to directory or name of the DB for results')
            st.subheader(body='Optional widgets')
            self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                 help='Maximum quantity of parallel annotated tables')
            self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                   help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
            self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                   help='Quantity of metainformation lines of annotated tables (src-VCF: not applicable; src-BED, src-TSV: include a header)')
            self.extra_query = st.text_input(label='extra-query', value='{}',
                                             help='''Additional query to all DB collections (PyMongo syntax;
{"$and": [your_query]} if necessary to shield the queried field name or $or operator;
examples of specifying data type: "any_str", Decimal128("any_str"))''')
            self.preset = st.radio(label='preset', options=['', 'by_location', 'by_alleles'],
                                   help='''Annotate via intersection by genomic location (experimental feature; src-TSV, src-db-TSV: not applicable).
Annotate ID by ID, verifying by alleles ID matches (experimental feature; src-TSV/BED, src-db-TSV/BED: not applicable)''')
            self.ann_col_num = st.number_input(label='ann-col-num', min_value=0, format='%d',
                                               help='Number of the annotated column (applicable without preset; src-VCF: [[3]]; src-BED: [[4]]; src-TSV: [[1]])')
            self.ann_field_path = st.text_input(label='ann-field-path',
                                                help='Dot path to the field by which to annotate (applicable without preset; src-db-VCF: [[ID]]; src-db-BED: [[name]]; src-db-TSV: [[first field after _id]])')
            self.srt_field_group = st.text_input(label='srt-field-group',
                                                 help='Dot paths to sorted fields (plus separated without spaces; src-db-VCF, src-db-BED: trg-(db-)TSV)')
            self.srt_order = st.radio(label='srt-order', options=['asc', 'desc'],
                                      help='Order of sorting (applicable with srt-field-group)')
            self.proj_field_names = st.text_input(label='proj-field-names',
                                                  help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-(db-)TSV; _id field will not be output)')
            self.sec_delimiter = st.radio(label='sec-delimiter', options=['colon', 'comma', 'low_line', 'pipe', 'semicolon'], index=1,
                                          help='Punctuation mark to restore a cell from a list (src-db-VCF, src-db-BED (trg-BED): not applicable)')
            self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                  help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[first field after _id]])')
            self.submit = st.form_submit_button(label='run')
