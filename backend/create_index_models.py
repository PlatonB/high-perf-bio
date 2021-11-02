__version__ = 'v2.0'

from pymongo import IndexModel, ASCENDING

def create_index_models(coll_name_ext, ind_field_names):
        '''
        Функция не индексирует, а лишь готовит руководящие индексацией
        IndexModel-объекты. Поле meta принудительно пойдёт на индексацию
        для любого формата. Это чтобы документ с метаинформацией исключать
        из результатов без коллскана. В случае db-VCF и db-BED, независимо
        от того, указал исследователь имена индексируемых полей или
        нет, модели индексации формируются для полей с локациями и
        основополагающими идентификаторами. А то, что облигатные индексы
        полей с хромосомами и позициями проектируются составными, позволит
        потом парсерам эффективно сортировать свои результаты стандартным
        для вычислительной генетики образом - по геномной локации. Для
        обозначенных исследователем полей планируется построение одиночных
        индексов. Если они придутся на хромосому или позицию db-VCF/db-BED,
        то будут сосуществовать с обязательным компаундным индексом. Обновить
        набор индексов впоследствии можно будет отдельной программой из
        состава проекта, а также с помощью MongoDB Shell или Compass.
        '''
        if coll_name_ext == 'vcf':
                index_models = [IndexModel([('meta', ASCENDING)]),
                                IndexModel([('#CHROM', ASCENDING),
                                            ('POS', ASCENDING)]),
                                IndexModel([('ID', ASCENDING)])]
        elif coll_name_ext == 'bed':
                index_models = [IndexModel([('meta', ASCENDING)]),
                                IndexModel([('chrom', ASCENDING),
                                            ('start', ASCENDING),
                                            ('end', ASCENDING)]),
                                IndexModel([('name', ASCENDING)])]
        else:
                index_models = [IndexModel([('meta', ASCENDING)])]
        if ind_field_names is not None:
                index_models += [IndexModel([(ind_field_name, ASCENDING)]) for ind_field_name in ind_field_names]
        return index_models
