__version__ = 'v3.0'

import streamlit as st
from descriptions.concatenate_descr import ConcatenateDescr
from pymongo import MongoClient


class AddWidgetsRu():
    '''
    Создание Streamlit-интерфейса.
    '''

    def __init__(self, version, authors):
        with st.form(key='concatenate'):
            st.header(body='concatenate')
            with st.expander(label='description'):
                st.text(body=ConcatenateDescr(version, authors).ru)
            st.subheader(body='Обязательные виджеты')
            self.src_db_name = st.selectbox(label='src-db-name',
                                            options=MongoClient().list_database_names(),
                                            help='Имя БД, которую конкатенировать')
            self.trg_db_name = st.text_input(label='trg-db-name',
                                             help='Имя БД для результатов')
            st.subheader(body='Необязательные аргументы')
            self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                   help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
            self.extra_query = st.text_input(label='extra-query', value='{}',
                                             help='Дополнительный запрос ко всем коллекциям БД (синтаксис PyMongo; примеры указания типа данных: "any_str", Decimal128("any_str"))')
            self.proj_field_names = st.text_input(label='proj-field-names',
                                                  help='Отбираемые поля верхнего уровня (через запятую без пробела; src-db-VCF, src-db-BED: trg-db-TSV)')
            self.del_copies = st.checkbox(label='del-copies',
                                          help='Удалить дубли конечных документов (proj-field-names применяется ранее; вложенные структуры не поддерживаются; _id при сравнении не учитывается)')
            self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                  help='Точечные пути к индексируемых полям (через запятую и/или плюс без пробела; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[первое после _id поле]])')
            self.submit = st.form_submit_button(label='run')


class AddWidgetsEn():
    '''
    Создание Streamlit-интерфейса.
    '''

    def __init__(self, version, authors):
        with st.form(key='concatenate'):
            st.header(body='concatenate')
            with st.expander(label='description'):
                st.text(body=ConcatenateDescr(version, authors).en)
            st.subheader(body='Mandatory widgets')
            self.src_db_name = st.selectbox(label='src-db-name',
                                            options=MongoClient().list_database_names(),
                                            help='Name of DB to concatenate')
            self.trg_db_name = st.text_input(label='trg-db-name',
                                             help='Name of DB for results')
            st.subheader(body='Optional widgets')
            self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                   help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
            self.extra_query = st.text_input(label='extra-query', value='{}',
                                             help='Additional query to all DB collections (PyMongo syntax; examples of specifying data type: "any_str", Decimal128("any_str"))')
            self.proj_field_names = st.text_input(label='proj-field-names',
                                                  help='Selected top level fields (comma separated without spaces; src-db-VCF, src-db-BED: trg-db-TSV)')
            self.del_copies = st.checkbox(label='del-copies',
                                          help='Remove duplicates of target documents (proj-field-names is applied previously; nested structures are not supported; _id is not taken into account when comparing)')
            self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                  help='Dot paths to indexed fields (comma and/or plus separated without spaces; trg-db-VCF: [[#CHROM+POS,ID]]; trg-db-BED: [[chrom+start+end]]; trg-db-TSV: [[first field after _id]])')
            self.submit = st.form_submit_button(label='run')
