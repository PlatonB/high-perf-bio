__version__ = 'v3.1'


def restore_info_cell(info_obj):
    '''
    Функция восстановления ячейки VCF-INFO-столбца
    из MongoDB-объекта. Последний должен представлять
    собой список со словарём бывших парных INFO-элементов
    и с бывшими одиночными INFO-флагами. То, как этот объект
    формируется, подробно описано в строке документации
    соответствующей функции программы создания БД.
    '''
    info_row = []
    for key, val in info_obj[0].items():
        if type(val) is list:
            info_row.append(f'{key}={",".join(map(str, val))}')
        else:
            info_row.append(f'{key}={str(val)}')
    for flag in info_obj[1:]:
        info_row.append(str(flag))
    info_cell = ';'.join(info_row)
    return info_cell


def restore_gt_cell(gt_obj):
    '''
    Функция восстановления ячейки VCF-GT-столбца из
    MongoDB-объекта. Последний должен представлять собой
    словарь, в котором ключи - бывшие FORMAT-элементы,
    а значения - бывшие GT-элементы. Историю
    формирования объекта можно понять по описанию
    соответствующей функции программы создания БД.
    '''
    gt_row = []
    for val in gt_obj.values():
        if type(val) is list:
            if '|' in val or '/' in val:
                join_sym = ''
            else:
                join_sym = ','
            gt_row.append(join_sym.join(map(str,
                                            val)))
        else:
            gt_row.append(str(val))
    gt_cell = ':'.join(gt_row)
    return gt_cell


def restore_line(doc, trg_file_format, sec_delimiter):
    '''
    Функция восстановления полной строки из MongoDB-документа.
    То, как будет выглядеть строка, зависит от формата конечного
    файла. VCF- и BED-строки формируются в соответствии с официальными
    спецификациями этих форматов. Подробно тут это описывать мне лень,
    но могу дать разъяснения в Issues. Восстановление TSV осуществляется
    проще всего: вложенные списки схлопываются по заданному исследователем
    символу, а все элементы верхнего уровня - по табуляции, при этом не
    являющиеся списком элементы любой глубины приобретают строковый тип
    данных. Для TSV есть одно исключение: если попадается список, а имя
    соответствующего поля - INFO, и, к тому же, первым элементом списка
    оказывается словарь, то такой список принимается за VCF-INFO и
    склеивается подходящим алгоритмом. INFO в TSV-объекте может встретиться,
    если TSV происходит от src-db-VCF. Такое возможно, если src-db-VCF
    подвергся отбору полей или нестандартной для формата сортировке.
    '''
    try:
        del doc['_id']
    except KeyError:
        pass
    field_names = list(doc.keys())
    if trg_file_format == 'vcf':
        doc['#CHROM'] = str(doc['#CHROM'])
        doc['POS'] = str(doc['POS'])
        if type(doc['ID']) is list:
            doc['ID'] = ';'.join(doc['ID'])
        if type(doc['ALT']) is list:
            doc['ALT'] = ','.join(doc['ALT'])
        doc['QUAL'] = str(doc['QUAL'])
        doc['INFO'] = restore_info_cell(doc['INFO'])
        if len(field_names) > 8:
            sample_names = field_names[8:]
            format_val = ':'.join(doc[sample_names[0]].keys())
            for sample_name in sample_names:
                doc[sample_name] = restore_gt_cell(doc[sample_name])
        row = list(doc.values())
        if 'format_val' in locals():
            row.insert(8, format_val)
        line = '\t'.join(row) + '\n'
    elif trg_file_format == 'bed':
        for field_name in field_names[:10]:
            doc[field_name] = str(doc[field_name])
        if len(field_names) > 10:
            for field_name in field_names[10:]:
                doc[field_name] = ','.join(map(str,
                                               doc[field_name]))
        line = '\t'.join(doc.values()) + '\n'
    else:
        for field_name in field_names:
            if type(doc[field_name]) is list:
                if field_name == 'INFO' and type(doc[field_name][0]) is dict:
                    doc[field_name] = restore_info_cell(doc[field_name])
                else:
                    doc[field_name] = sec_delimiter.join(map(str,
                                                             doc[field_name]))
            else:
                doc[field_name] = str(doc[field_name])
        line = '\t'.join(doc.values()) + '\n'
    return line
