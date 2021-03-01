__version__ = 'v1.1'

from pymongo import IndexModel, ASCENDING

def create_index_models(coll_name_ext, ind_field_names):
        '''
        Функция не индексирует, а лишь готовит руководящие индексацией
        IndexModel-объекты. В случае db-VCF и db-BED, независимо от того,
        указал исследователь имена индексируемых полей или нет, модели
        индексации формируются для полей с локациями и основополагающими
        идентификаторами. А то, что облигатные индексы полей с хромосами и
        позициями проектируются составными, позволит потом парсерам эффективно
        сортировать свои результаты стандартным для вычислительной генетики
        образом - по геномной локации. Для обозначенных исследователем
        полей планируется построение одиночных индексов. Если они
        придутся на хромосому или позицию db-VCF/db-BED, то будут
        сосуществовать с обязательным компаундным индексом. Обновить
        набор индексов впоследствии можно будет отдельной программой из
        состава проекта, а также с помощью MongoDB Shell или Compass.
        '''
        if coll_name_ext == 'vcf':
                index_models = [IndexModel([('#CHROM', ASCENDING),
                                            ('POS', ASCENDING)]),
                                IndexModel([('ID', ASCENDING)])]
        elif coll_name_ext == 'bed':
                index_models = [IndexModel([('chrom', ASCENDING),
                                            ('start', ASCENDING),
                                            ('end', ASCENDING)]),
                                IndexModel([('name', ASCENDING)])]
        else:
                index_models = []
        if ind_field_names is not None:
                index_models += [IndexModel([(ind_field_name, ASCENDING)]) for ind_field_name in ind_field_names]
        return index_models
