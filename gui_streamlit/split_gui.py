__version__ = 'v1.0'

import streamlit as st
from descriptions.split_descr import SplitDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='split'):
                        with st.expander(label='description'):
                                st.text(body=SplitDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Имя БД, коллекции которой разделять')
                        self.trg_place = st.text_input(label='trg-place',
                                                       help='Путь к папке или имя БД для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Максимальное количество параллельно делимых коллекций')
                        self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                               help='Разрешить перезаписать существующую БД в случае конфликта имён (исходную БД перезаписывать нельзя)')
                        self.spl_field_path = st.text_input(label='spl-field-path',
                                                            help='Точечный путь к полю, по которому делить (src-db-VCF: [[#CHROM]]; src-db-BED: [[chrom]]; src-db-TSV: [[первое после _id поле]])')
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
        def __init__(self, ver):
                with st.form(key='split'):
                        with st.expander(label='description'):
                                st.text(body=SplitDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Name of the DB whose collections to split')
                        self.trg_place = st.text_input(label='trg-place',
                                                       help='Path to directory or name of the DB for results')
                        st.subheader(body='Optional widgets')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Maximum quantity of collections splitted in parallel')
                        self.rewrite_existing_db = st.checkbox(label='rewrite-existing-db',
                                                               help='Allow overwriting an existing DB in case of names conflict (the source DB cannot be overwritten)')
                        self.spl_field_path = st.text_input(label='spl-field-path',
                                                            help='Dot path to the field by which to split (src-db-VCF: [[#CHROM]]; src-db-BED: [[chrom]]; src-db-TSV: [[first field after _id]])')
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
