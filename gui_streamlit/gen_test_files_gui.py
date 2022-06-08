__version__ = 'v1.0'

import streamlit as st
from descriptions.gen_test_files_descr import GenTestFilesDescr

class AddWidgetsRu():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='gen_test_files'):
                        st.header(body='gen_test_files')
                        with st.expander(label='description'):
                                st.text(body=GenTestFilesDescr(ver).ru)
                        st.subheader(body='Обязательные виджеты')
                        self.src_file_path = st.text_input(label='src-file-path',
                                                           help='Путь к сжатому файлу')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Путь к папке для результатов')
                        st.subheader(body='Необязательные виджеты')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Количество строк метаинформации (src-VCF: не применяется; src-BED, src-TSV: включите шапку)')
                        self.thinning_lvl = st.number_input(label='thinning-lvl', min_value=1, value=10, format='%d',
                                                            help='Жёсткость прореживания (чем она больше, тем меньше останется строк)')
                        self.trg_files_quan = st.number_input(label='trg-files-quan', min_value=1, value=4, format='%d',
                                                              help='Количество файлов, по которым разнести результаты')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', min_value=1, value=4, format='%d',
                                                             help='Максимальное количество параллельно генерируемых файлов')
                        self.submit = st.form_submit_button(label='run')
                        
class AddWidgetsEn():
        '''
        Создание Streamlit-интерфейса.
        '''
        def __init__(self, ver):
                with st.form(key='gen_test_files'):
                        st.header(body='gen_test_files')
                        with st.expander(label='description'):
                                st.text(body=GenTestFilesDescr(ver).en)
                        st.subheader(body='Mandatory widgets')
                        self.src_file_path = st.text_input(label='src-file-path',
                                                           help='Path to gzipped file')
                        self.trg_dir_path = st.text_input(label='trg-dir-path',
                                                          help='Path to directory for results')
                        st.subheader(body='Optional widgets')
                        self.meta_lines_quan = st.number_input(label='meta-lines-quan', min_value=0, format='%d',
                                                               help='Quantity of metainformation lines (src-VCF: not applicable; src-BED, src-TSV: include a header)')
                        self.thinning_lvl = st.number_input(label='thinning-lvl', min_value=1, value=10, format='%d',
                                                            help='Thinning level (the higher it is, the fewer lines will be remain)')
                        self.trg_files_quan = st.number_input(label='trg-files-quan', min_value=1, value=4, format='%d',
                                                              help='Quantity of files by which to distribute the results')
                        self.max_proc_quan = st.number_input(label='max-proc-quan', min_value=1, value=4, format='%d',
                                                             help='Maximum quantity of files generated in parallel')
                        self.submit = st.form_submit_button(label='run')
