__version__ = 'v1.0'

import streamlit as st
from descriptions.reindex_descr import ReindexDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='reindex'):
                        with st.expander(label='description'):
                                st.text(body=ReindexDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Имя переиндексируемой БД')
                        st.subheader(body='Необязательные виджеты')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Максимальное количество параллельно индексируемых коллекций')
                        self.del_ind_names = st.text_input(label='del-ind-names',
                                                           help='Имена удаляемых индексов (через запятую без пробела)')
                        self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                              help='Точечные пути к индексируемых полям (через запятую без пробела; для составного индекса: через плюс без пробелов)')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='reindex'):
                        with st.expander(label='description'):
                                st.text(body=ReindexDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_db_name = st.text_input(label='src-db-name',
                                                         help='Name of the reindexed DB')
                        st.subheader(body='Optional widgets')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', value=4, min_value=1, format='%d',
                                                             help='Maximum quantity of parallel indexed collections')
                        self.del_ind_names = st.text_input(label='del-ind-names',
                                                           help='Names of deleted indexes (comma separated without spaces)')
                        self.ind_field_groups = st.text_input(label='ind-field-groups',
                                                              help='Dot paths to indexed fields (comma separated without spaces; for a compound index: plus separated without spaces)')
                        self.submit = st.form_submit_button(label='run')
