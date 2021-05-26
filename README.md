# Синопсис.
## Компоненты.
| Программа | Основная функциональность |
| --------- | -------------- |
| annotate | получает характеристики табличного столбца по всей БД; поддерживается пересечение координат |
| concatenate | объединяет коллекции БД с опциональным удалением повторов |
| create_db | создаёт новую БД по VCF, BED или нестандартным таблицам; производит её индексацию |
| download_bio_data | скачивает табличные данные известных биоинформатических проектов |
| left_join | фильтрует одни коллекции БД по наличию/отсутствию значений в других; возможна работа с координатами |
| print_db_info | выводит имена или основные свойства имеющихся БД |
| query | выполняет ваш запрос сразу по всей БД |
| reindex_db | удаляет и строит индексы ранее созданной БД |
| split_by_chr | дробит коллекции БД, основываясь на принадлежности данных к хромосомам |

## Преимущества.
- Высокая скорость работы:
    - вплоть до 8 процессов одновременно;
    - в основе — известная своей отменной производительностью СУБД _MongoDB_.
- Простота запуска:
    - не требуется установка, можно запускать и с флешки;
    - не более трёх обязательных аргументов.
- Наглядность:
    - вывод примерной структуры интересующей БД.
- Автоматизация:
    - одна команда — обработка всех коллекций базы.
    - полностью автоматическое чтение и создание VCF/BED.
- Конвейерность:
    - результаты можно перенаправлять в другую БД, минуя создание конечных файлов.
- Техподдержка:
    - `-h`: вывод тщательно продуманной справки по синтаксису команд.
    - [Тьюториал](#тьюториал) прямо в этом ридми.
    - Всегда бесплатная [консультация](https://github.com/PlatonB/high-perf-bio/issues).

# Перед началом работы.
## Если кратко.
1. Разрешите [зависимости](https://github.com/PlatonB/high-perf-bio#установка-сторонних-компонентов).
2. [Скачайте](https://github.com/PlatonB/high-perf-bio/archive/master.zip) архив с инструментарием.
3. Распакуйте его в любую папку.
4. Освойте [запуск программ из терминала](https://github.com/PlatonB/ngs-pipelines#преодолеваем-страх-командной-строки-linux), если ещё этого не сделали.
5. В терминале перейдите в папку `high-perf-bio-master`.
```
cd /path/to/high-perf-bio-master
```

## Установка сторонних компонентов.
### MongoDB.
Советую вначале ознакомиться с [основами работы в линуксовом терминале](https://github.com/PlatonB/ngs-pipelines#преодолеваем-страх-командной-строки-linux). Впрочем, если совсем лень, можете просто копировать, вставлять и запускать приведённые ниже команды.

#### Ubuntu Linux.
(_[elementary OS](https://elementary.io/ru/)_/_KDE neon_/_Linux Mint_)

Подключение официального репозитория _MongoDB_.
```
wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | sudo apt-key add -
```
```
echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu focal/mongodb-org/4.4 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-4.4.list
```

Обновление индекса пакетов ОС.
```
sudo apt update
```

Собственно, установка _MongoDB_.
```
sudo apt install -y mongodb-org
```

Перманентный запуск _MongoDB_. Лучше так сделать, если планируете использовать _high-perf-bio_ часто.
```
sudo systemctl enable mongod.service
```

Если вам не нужно эксплуатировать _MongoDB_-решения каждый день, то рекомендую команду, активирующую _MongoDB_ до ближайшей перезагрузки.
```
sudo service mongod start
```

После инсталляции настоятельно рекомендую перезагрузиться.
```
sudo systemctl reboot
```

Обязательно проверьте, успешно ли СУБД установилась.
```
mongo --version
```

Если да, то первой строкой выведется номер версии интерактивной консоли _MongoDB_.
```
MongoDB shell version v4.4.1
<...>
```

#### Fedora Linux.
TBD.

### PyMongo.
Установка с помощью _pip_:
```
pip3 install pymongo
```

Установка с помощью [_Conda_](https://github.com/PlatonB/ngs-pipelines#установка-conda):
```
conda install pymongo
```

### Примечание по поводу Windows.
Теоретически, после установки _MongoDB_ и whl-пакета _PyMongo_ программа должна работать. Но у меня сейчас _Windows_ нет, и я пока не проверял. Надеюсь, кто-нибудь поделится опытом в [Issues](https://github.com/PlatonB/high-perf-bio/issues).

# Тьюториал.
Компоненты _high-perf-bio_ управляются исключительно командами. Если вы имели дело только с графическими интерфейсами, можете почитать [мой материал о командной строке](https://github.com/PlatonB/ngs-pipelines#преодолеваем-страх-командной-строки-linux). Ничего сложного там нет, а если что, [обращайтесь](https://github.com/PlatonB/high-perf-bio/issues), я помогу.

Чтобы не прописывать каждый раз путь к тому или иному компоненту, перейдём в основную папку инструментария.
```
cd $HOME/Биоинформатика/high-perf-bio-master
```
Примечание: корректируйте приведённые в примерах пути в зависимости от реального расположения ваших папок или файлов.

Для удобства можете вывести имена всех программ проекта _high-perf-bio_.
```
ls -R
```

Каждый инструмент содержит подробную справку. Пока с ней не ознакомились, реальные задачи не запускайте.
```
python3 create_db.py -h
```
```
python3 annotate.py -h
```
и т.д..

У меня нет цели продемонстрировать в тьюториале все аргументы программ. Наоборот, хочу показать, как обойтись минимумом таковых.

В папке `high-perf-bio-master` уже есть небольшие примеры данных. Но в качестве материала для базы лучше скачать что-то серьёзное, например, [VCF-таблицу всех SNP](https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.38.gz). Переименуйте её, добавив расширение vcf перед gz.

Поскольку формат этого файла — VCF, программа создания БД сама отбросит метастроки, сконвертирует хромосомы из HGVS в обычный вид, преобразует ячейки определённых столбцов в BSON-структуры и проиндексирует поля с хромосомами, позициями и rsIDs. Файл — один, значит, распараллелить заливку не удастся. Таким образом, команда может получиться весьма минималистичной. Достаточно подать путь к папке со свежескачанным VCF.
```
python3 create_db.py -S $HOME/Биоинформатика/dbSNP
```

При работе с _high_perf_bio_ высока вероятность возникновения двух проблем: вы забыли, как называется БД и из чего она состоит. В таких ситуациях перед запуском любого парсера требуется получить представление о парсимой БД. Для начала найдите её имя в списке всех имён.
```
python3 print_db_info.py
```

Затем выведите первостепенные характеристики конкретной базы.
```
python3 print_db_info.py -d dbSNP
```

Теперь мы многое знаем о БД и можем, к примеру, осуществить по ней запрос. Для разминки отберём снипы интервала `chr14:105746998-105803861`, упомянутого в статье "**Локусы влияющие на экспрессию антигенов HLA в участке 14-й хромосомы, ассоциированном с развитием рассеянного склероза, и функции расположенных в них генов**" (П.А. Быкадоров и др.). Помните, что программа _query_ требует весь запрос заключать в одинарные кавычки, операторы и строковые значения - в двойные, а численные значения оставлять без ничего.
```
python3 query.py -D dbSNP -T $HOME/Биоинформатика/ms_locus -q '{"#CHROM": 14, "POS": {"$gte": 105746998, "$lte": 105803861}}'
```

Ещё, чтобы лучше привыкнуть к _high_perf_bio_, найдём характеристики вариантов из папки `high-perf-bio-master/test_data/TSV`. С расположением rsID-столбца повезло — он первый. С единственной коллекцией базы `dbSNP` тоже всё ок — поскольку это ex-VCF, то там есть поле ID. Значит, обойдёмся минимумом аргументов: пути к папке с игрушечной таблицей и конечной папке, имя базы и игнор табличной шапки.
```
python3 annotate.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -D dbSNP -T $HOME/Биоинформатика/TSV_ann -m 1
```

А вот задача явно ближе к реальной биоинформатике. Вы когда нибудь задумывались, есть ли на свете варианты, являющиеся eQTL сразу во всех тканях? Сейчас посмотрим. Скачаем [таблицы значимых пар вариант-ген](https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar). Нам понадобятся только `*.signif_variant_gene_pairs.txt.gz`-файлы. Упростить скачивание _GTEx_-архива и отбор signif-файлов сможет утилита _download_bio_data_:
```
python3 download_bio_data.py -T $HOME/Биоинформатика --cis-eqtls-gtex
```

Таблицы так себе приспособлены к анализу: координаты, аллели и номер сборки запихнуты в один столбец, а rsIDs вовсе отсутствуют. Сконвертировать это непарсибельное хозяйство в VCF предлагаю [скриптом](https://github.com/PlatonB/for-fun/blob/master/eqtls_tsv_to_vcf.py), а обогатить VCFs сниповыми идентификаторами — _high-perf-bio_-плагином _revitalize_id_column_. Дарить rsIDs VCF-файлам готова ранее созданная нами _MongoDB_-версия _dbSNP_. Переходим в папку с плагинами и допредпроцешиваем _GTEx_-данные.
```
cd plugins
```
```
python3 revitalize_id_column.py -S $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF -D dbSNP -T $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF_rsIDs
```

Зальём удобоваримую версию _GTEx_-таблиц в базу. Дадим базе легко воспринимаемое имя. Коллекций — несколько десятков, поэтому лично я предпочёл бы распараллелить размещение и индексацию на 8 процессов.
```
python3 create_db.py -S $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF_rsIDs -d GTEx -p 8
```

Напомню, мы хотели найти eQTLs, дающие о себе знать во всех тканях. Применим алгоритм пересечения. Что такое левые/правые коллекции и охват, подробно рассказано в справке _left_join_ — здесь я на этом останавливаться не буду. Указываем в явном виде одну левую коллекцию. Любую. Правыми коллекциями для решения задачи надо сделать все остальные, но перечислять их не нужно — в программе такой сценарий предусмотрен по умолчанию. То, что нам нужно пересекать, а не вычитать, опять же, явно прописывать не обязательно. С наличием идентификаторов у нас проблем нет, поэтому можно работать по ним, хотя координатные вычисления программой тоже поддерживаются. Подавать аргумент с именем пересекаемого поля не будем, т.к. по дефолту идёт ID. eQTL нам общие для всех тканей надо найти? Значит, задаём охват максимальный. Он равен количеству всех коллекций минус 1 (вычли одну левую). Программа, кстати, позволяет исследователю не тратить время на определение этого значения, а указать `0`.
```
python3 left_join.py -D GTEx -T $HOME/Биоинформатика/GTEx_common -l Adipose_Subcutaneous.v8.signif_variant_gene_pairs_rsIDs.vcf -c 0
```

Вездесущие eQTL обнаружились, и их 63 штуки. А сколько будет вариантов, влияющих на экспрессию генов только в крови и больше нигде? Это решается вычитанием.
```
python3 left_join.py -D GTEx -T $HOME/Биоинформатика/GTEx_onlyWB -l Whole_Blood.v8.signif_variant_gene_pairs_rsIDs.vcf -a subtract -c 0
```

Таких уникальных вариантов аж около половины — 10963 из 20315 всех кровяных.

Найти что ли клинически значимые варианты среди эксклюзивно кровяных? Воспользуемся [таблицей ассоциаций вариант-болезнь](https://www.disgenet.org/static/disgenet_ap1/files/downloads/all_variant_disease_associations.tsv.gz) проекта _DisGeNET_. Эта таблица — небиоинформатического формата, поэтому при создании на её основе базы и дальнейшей работе с этой базой применимы не все удобные умолчания.
```
python3 create_db.py -S $HOME/Биоинформатика/DisGeNET -s semicolon -i snpId,chromosome,position
```

Можно, кстати, из этой БД создать новую, которая разбита на похромосомные коллекции, урезанные до 5 самых необходимых полей.
```
python3 split_by_chr.py -D DisGeNET -T DisGeNET_chrs -f chromosome -k snpId,chromosome,position,diseaseName,score -i snpId,chromosome,position
```

Ой, кажется, забыли проиндексировать поле со скорами ассоциаций. С кем не бывает? Воспользуемся штатным редактором индексов, чтобы потом программы, отсекающие малоподтверждённые пары, не тратили силы на тупой перебор скоров.
```
python3 reindex_db.py -D DisGeNET_chrs -a score
```

Теперь, наконец, аннотируем уникальные Whole Blood eQTLs по _DisGeNET_. Результаты кидаем в новую БД.
```
python annotate.py -S $HOME/Биоинформатика/GTEx_onlyWB -D DisGeNET_chrs -T GTEx_onlyWB_Clin -f snpId -i snpId,chromosome,position,score
```

Отбираем наиболее клинически значимые eQTLs.
```
python3 query.py -D GTEx_onlyWB_Clin -T $HOME/Биоинформатика/GTEx_onlyWB_strongClin -q '{"score": {"$gte": Decimal128("0.9")}}'
```

# Механизм пересечения и вычитания.
Этот раздел — попытка максимально доступно объяснить, как работает программа _left_join_.

## Терминология.
| Термин | Аналог в реляционных БД | Аналог в Python | Пример | Пояснение |
| ------ | ----------------------- | --------------- | ------ | --------- |
| Поле | Столбец | Ключ словаря | `'f1'` | Может присутствовать в одних документах и при этом отсутствовать в других |
| Документ | Строка | Словарь | `{'f1': 1, 'f2': 'one', 'f3': 'c1.txt'}` | Объект из пар поле-значение. В качестве значений могут быть вложенные объекты |
| Коллекция | Таблица | Список словарей | | Набор, как правило, объединённых по смыслу документов |

## Тестовые данные.
В качестве примера будет БД из трёх игрушечных коллекций, которые легко пересекать и вычитать в уме. `_id`-поле здесь и далее опускается.

Коллекция `c1`.
```
{'f1': 1, 'f2': 'one', 'f3': 'c1.txt'}
```
```
{'f1': 2, 'f2': 'two', 'f3': 'c1.txt'}
```
```
{'f1': 3, 'f2': 'three', 'f3': 'c1.txt'}
```
```
{'f1': 4, 'f2': 'four', 'f3': 'c1.txt'}
```
```
{'f1': 5, 'f2': 'five', 'f3': 'c1.txt'}
```

Коллекция `c2`.
```
{'f1': 3, 'f2': 'three', 'f3': 'c2.txt'}
```
```
{'f1': 4, 'f2': 'four', 'f3': 'c2.txt'}
```
```
{'f1': 5, 'f2': 'five', 'f3': 'c2.txt'}
```
```
{'f1': 6, 'f2': 'six', 'f3': 'c2.txt'}
```
```
{'f1': 7, 'f2': 'seven', 'f3': 'c2.txt'}
```

Коллекция `c3`.
```
{'f1': 5, 'f2': 'five', 'f3': 'c3.txt'}
```
```
{'f1': 6, 'f2': 'six', 'f3': 'c3.txt'}
```
```
{'f1': 7, 'f2': 'seven', 'f3': 'c3.txt'}
```
```
{'f1': 8, 'f2': 'eight', 'f3': 'c3.txt'}
```
```
{'f1': 9, 'f2': 'nine', 'f3': 'c3.txt'}
```

## Параметры отбора документов левой коллекции.
По справке к _left_join_ вы уже знаете, что каждая из коллекций, условно обозначенных как "левые", фильтруется по всем "правым". Левой пусть будет `c1`, а в качестве правых — `c2` и `c3`. Непосредственно обрабатываемое поле — допустим, `f1`. Общее представление об условях отбора документов `c1`:
| Действие | Охват | Документ из `c1` сохраняется, если |
| -------- | ----- | ---------------------------- |
| пересечение | 1 | совпадение в `c2` или `c3` |
| пересечение | 2 | совпадение в `c2` и `c3` |
| вычитание | 1 | несовпадение в `c2` или `c3` |
| вычитание | 2 | несовпадение в `c2` и `c3` |

## Решение.
### Стадии.
1. **Левостороннее внешнее объединение**. Независимо от того, хотим мы пересекать или вычитать, выполнение `$lookup`-инструкции лишь объединит коллекции. Каждый документ, получающийся в результате объединения, содержит: 1. все поля документа левой коллекции; 2. поля с соответствиями из правых коллекций (далее — результирующие). Если для элемента выбранного поля данного левого документа не нашлось совпадений в одной из правых коллекций, то в результирующем поле появится пустой список (_Python_-представление `Null`-значения из мира баз данных). Если же совпадение имеется, то в качестве значения результирующего поля возвратится список с содержимым соответствующего правого документа. Если выявилось совпадение с несколькими документами какой-то одной правой коллекции, то они в полном составе поступят в результирующее поле.

Схема объединённого документа.
```
{левый документ, псевдоним правой коллекции 1: [правый документ 1.1, правый документ 1.2, ...],
                 псевдоним правой коллекции 2: [правый документ 2.1, правый документ 2.2, ...],
                 ...}
```

Объединённые документы тестовых коллекций (`c1` vs `c2`, `c3`).
```
{'f1': 1, 'f2': 'one', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```
```
{'f1': 2, 'f2': 'two', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```
```
{'f1': 3, 'f2': 'three', 'f3': 'c1.txt', 'c2': [{'f1': 3, 'f2': 'three', 'f3': 'c2.txt'}], 'c3': []}
```
```
{'f1': 4, 'f2': 'four', 'f3': 'c1.txt', 'c2': [{'f1': 4, 'f2': 'four', 'f3': 'c2.txt'}], 'c3': []}
```
```
{'f1': 5, 'f2': 'five', 'f3': 'c1.txt', 'c2': [{'f1': 5, 'f2': 'five', 'f3': 'c2.txt'}], 'c3': [{'f1': 5, 'f2': 'five', 'f3': 'c3.txt'}]}
```

То же самое в схематичном виде (`l` — документ левой коллекции, `r№a` — alias (псевдоним) правой; `0` — в правой нет совпадений, `1` — есть одно).
```
l, r1a: 0, r2a: 0
```
```
l, r1a: 0, r2a: 0
```
```
l, r1a: 1, r2a: 0
```
```
l, r1a: 1, r2a: 0
```
```
l, r1a: 1, r2a: 1
```

2. **Фильтрация**. Изучая предыдущий пункт, вы уже, наверное, догадались, что пересекать или вычитать — значит, фильтровать результаты объединения по количеству непустых или пустых результирующих списков соответственно.

### Пересечение. Охват 1.
Чтобы значение левого поля `f1` попало в результаты, такое же значение должно найтись хотя бы в одном правом `f1`.

Объединённые документы, отвечающие условию.
```
{'f1': 3, 'f2': 'three', 'f3': 'c1.txt', 'c2': [{'f1': 3, 'f2': 'three', 'f3': 'c2.txt'}], 'c3': []}
```
```
{'f1': 4, 'f2': 'four', 'f3': 'c1.txt', 'c2': [{'f1': 4, 'f2': 'four', 'f3': 'c2.txt'}], 'c3': []}
```
```
{'f1': 5, 'f2': 'five', 'f3': 'c1.txt', 'c2': [{'f1': 5, 'f2': 'five', 'f3': 'c2.txt'}], 'c3': [{'f1': 5, 'f2': 'five', 'f3': 'c3.txt'}]}
```

Схема соответствующих условию объединённых документов.
```
l, r1a: 1, r2a: 0
```
```
l, r1a: 1, r2a: 0
```
```
l, r1a: 1, r2a: 1
```

Окончательный результат в табличном виде.
| f1 | f2 | f3 |
| -- | -- | -- |
| 3 | three | c1.txt |
| 4 | four | c1.txt |
| 5 | five | c1.txt |

### Пересечение. Охват 2.
Чтобы значение левого поля `f1` попало в результаты, такое же значение должно найтись в двух правых `f1`.

Объединённый документ, отвечающий условию.
```
{'f1': 5, 'f2': 'five', 'f3': 'c1.txt', 'c2': [{'f1': 5, 'f2': 'five', 'f3': 'c2.txt'}], 'c3': [{'f1': 5, 'f2': 'five', 'f3': 'c3.txt'}]}
```

Схема соответствующего условию объединённого документа.
```
l, r1a: 1, r2a: 1
```

Окончательный результат в табличном виде.
| f1 | f2 | f3 |
| -- | -- | -- |
| 5 | five | c1.txt |

### Вычитание. Охват 1.
Чтобы значение левого поля `f1` попало в результаты, такого же значения не должно быть хотя бы в одном правом `f1`.

Объединённые документы, отвечающие условию.
```
{'f1': 1, 'f2': 'one', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```
```
{'f1': 2, 'f2': 'two', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```
```
{'f1': 3, 'f2': 'three', 'f3': 'c1.txt', 'c2': [{'f1': 3, 'f2': 'three', 'f3': 'c2.txt'}], 'c3': []}
```
```
{'f1': 4, 'f2': 'four', 'f3': 'c1.txt', 'c2': [{'f1': 4, 'f2': 'four', 'f3': 'c2.txt'}], 'c3': []}
```

Схема соответствующих условию объединённых документов.
```
l, r1a: 0, r2a: 0
```
```
l, r1a: 0, r2a: 0
```
```
l, r1a: 1, r2a: 0
```
```
l, r1a: 1, r2a: 0
```

Окончательный результат в табличном виде.
| f1 | f2 | f3 |
| -- | -- | -- |
| 1 | one | c1.txt |
| 2 | two | c1.txt |
| 3 | three | c1.txt |
| 4 | four | c1.txt |

### Вычитание. Охват 2.
Чтобы значение левого поля `f1` попало в результаты, такого же значения не должно быть в двух правых `f1`.

Объединённые документы, отвечающие условию.
```
{'f1': 1, 'f2': 'one', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```
```
{'f1': 2, 'f2': 'two', 'f3': 'c1.txt', 'c2': [], 'c3': []}
```

Схема соответствующих условию объединённых документов.
```
l, r1a: 0, r2a: 0
```
```
l, r1a: 0, r2a: 0
```

Окончательный результат в табличном виде.
| f1 | f2 | f3 |
| -- | -- | -- |
| 1 | one | c1.txt |
| 2 | two | c1.txt |

## Повторы.
В конечный файл направятся все дубли элемента, находящиеся в пределах поля левой коллекции, но от повторов правых элементов копийность результатов не зависит.

## Задавайте вопросы!
Понятно, что реальные данные гораздо сложнее, чем раз-два-три-четыре-пять. Кидайте свои задачи на пересечение или вычитание в [Issues](https://github.com/PlatonB/high-perf-bio/issues) — подскажу, как решить.

# Полезные советы.
## Метастроки.
У части тулзов _high_perf_bio_ есть опция `--meta-lines-quan`/`-m`. Она даёт программе знать, сколько строк в начале файла не нужно никак трогать. Эти строки содержат т.н. метаинформацию, которая посвящает исследователя в различные особенности файла. Программам же они, как правило, только мешают. VCF — формат довольно строгий, и там метастроки чётко обозначены символами `##`, что даёт возможность скипать их автоматически. Для остальных форматов требуется ручное указание количества игнорируемых строк. Как это количество узнать? Если файл маленький, просто откройте его в обычном блокноте. В хороших блокнотах, как, например, _[elementary OS](https://elementary.io/ru/) Code_, есть нумерация строк, что облегчает задачу. Если же файл большой, то так сделать не получится — в лучшем случае вылезет ошибка, в худшем — осложните себе работу зависанием. Вас раздражает командная строка? Можете считывать первые строки сжатого файла скриптом-предпросмотрщиком из моего репозитория [_bioinformatic-python-scripts_](https://github.com/PlatonB/bioinformatic-python-scripts). Уже привыкли к эмулятору терминала? Тогда юзайте командную утилиту _zless_.
```
zless -N $HOME/Биоинформатика/dbSNP_common/00-common_all.vcf.gz
```

Скроллить можно колесом мыши или двумя пальцами вверх-вниз по тачпаду. Закрыть предпросмотр: `q`.

## Спецсимволы.
В ту или иную команду могут закрасться зарезервированные символы. Командная оболочка, на них наткнувшись, не сможет обработать вашу команду как единое целое. Я, к примеру, при тестировании _high_perf_bio_ получил ошибку из-за решётки в начале имени хромосомного поля.
```
python3 create_db.py -S $HOME/Биоинформатика/dbSNP_common -i #CHROM,POS,ID
```
```
create_db.py: error: argument -i/--ind-col-names: expected one argument
```

Из-за наличия решётки интерпретатор подумал, что `CHROM,POS,ID` — комментарий, а не перечисление индексируемых полей. Получилось, что аргумент `-i`, строго требующий значения, остался без такового. Надо было просто взять набор имён полей в одинарные кавычки (заэкранировать).
```
python3 create_db.py -S $HOME/Биоинформатика/dbSNP_common -i '#CHROM,POS,ID'
```

А знаете, почему программы _high_perf_bio_ заставляют перечислять что-либо через запятую без привычного нам по естественным языкам пробела? Не подумайте, что из вредности:). Пробел считается границей между аргументами. Т.е., при использовании запятых с пробелом каждый следующий элемент воспринимался бы шеллом в качестве нового аргумента.
```
python3 create_db.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -i SYMBOL, AF
```
```
create_db.py: error: unrecognized arguments: AF
```

Требование перечислять без пробела я ввёл, чтобы избавить вас от необходимости экранирования. Правильное оформление последней команды не подразумевает обязательного наличия надоедливых кавычек.
```
python3 create_db.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -i SYMBOL,AF
```
