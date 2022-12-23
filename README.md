# [Readme in English→](https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md)
# Синопсис.
Биоинформатика — это не только _BWA_/_GATK_/_Samtools_/_VEP_/продолжите-список-сами, но и беспросветная **табличная рутина**. Поэтому любой биоинформатик хранит жирную папку со скриптами, а также богатую историю команд, задействующих _coreutils_. Форумы завалены _bash_/_awk_-однострочниками для решения мелких и не очень задач парсинга. Мало того, что парсеров целый зоопарк, так они ещё и медленные все. Есть, правда, _tabix_ и _bedtools_, которые частично закрывают проблему производительности благодаря самописным B-tree-алгоритмам. Но они обеспечивают **быстрый рандомный доступ** только по геномным координатам. А если, предположим, надо найти набор rsIDs и отсечь результаты по p-value?

☛ _HIGH-PERF-BIO_ ПОЗВОЛЯЕТ СОЗДАВАТЬ **ПОЛНОЦЕННЫЕ БАЗЫ ДАННЫХ** НА ОСНОВЕ ТАБЛИЦ И ЗА ДОЛИ СЕКУНДЫ ВЫПОЛНЯТЬ НАИБОЛЕЕ ПОПУЛЯРНЫЕ В БИОИНФОРМАТИКЕ ДЕЙСТВИЯ ☚

Для VCF создаются **многоуровневые документы**, что открывает возможность глубоко парсить `INFO` и генотипные поля.

**Проиндексировать** можно какие угодно поля и группы полей. Индекс редко используется и занимает много места? Можно его удалить, и, если что, потом создать заново отдельным менеджером — _reindex_.

Флагманские компоненты проекта — три **аннотатора** (_annotate_, _dock_, _ljoin_), уникальность которых заключается в свободе выбора характеризуемого столбца и отсутствии привязки к определённому кэшу данных. Грубо говоря, можно аннотировать что угодно по чему угодно.

Что касается **фильтрации**, то в инструментарии имеется удивительная по своей универсальности программа — _query_. Можете написать любые _MongoDB_-запросы на целую научную работу вперёд и хлопнуть их разом. Сочинять запросы - одно удовольствие, т.к. синтаксис _MongoDB_ страшно прост и отлично задокументирован.

По состоянию на начало 2022 года я ещё не видел высокоуровневые программы-**группировщики** данных. Видимо, считается, что суровый дата-сайнтист всякий раз группирует руками в _pandas_ или какой-нибудь СУБД. Но если вдруг хочется одной командой — такое умеют, скорее всего, лишь _count_ и _split_.

Вдобавок, компоненты тулкита способны делать такие характерные для стандартных линуксовых утилит штуки, как **конкатенация** данных, **отбор полей** и **сортировка** значений. Но на огромных скоростях и без заковыристого синтаксиса.

## Компоненты.
### Ядро.
Наиболее универсальные и фичастые программы для взаимодействия с БД.
| Программа | Основная функциональность |
| --------- | ------------------------- |
| annotate | получает характеристики табличного столбца по всем коллекциям БД; поддерживается пересечение координат |
| concatenate | объединяет коллекции; есть опциональная фича удаления дублей документов |
| count | считает количество и частоту элементов каждого набора взаимосвязанных значений разных полей |
| create | создаёт новую или дописывает старую БД по VCF, BED или нестандартным таблицам; производит индексацию коллекций |
| dock | получает характеристики табличного столбца по всем коллекциям и самим таблицам; поддерживается пересечение координат |
| ljoin | фильтрует одни коллекции по наличию/отсутствию значений в других; возможна работа с координатами |
| info | выводит имена или основные свойства имеющихся БД |
| query | выполняет наборы произвольных запросов по всем коллекциям |
| reindex | удаляет и строит индексы коллекций |
| split | дробит коллекции по значениям какого-либо поля |

### Плагины.
Узкоспециализированные парсеры БД с минимумом настроек.
| Программа | Основная функциональность |
| --------- | ------------------------- |
| revitalize_id_column | восполняет столбец ID VCF-файлов идентификаторами вариантов из БД |

### Скрипты.
Полезные инструменты для работы с таблицами (не базами).
| Программа | Основная функциональность |
| --------- | ------------------------- |
| count_lines | выдаёт различные метрики, касающиеся количества табличных строк |
| gen_test_files | создаёт из одной таблицы N меньших таблиц |

## Преимущества.
- Высокая скорость работы:
    - параллельная обработка нескольких файлов или коллекций;
    - в основе — известная своей отменной производительностью СУБД _MongoDB_.
- Простота запуска:
    - не требуется установка, можно запускать и с флешки (при разрешённых зависимостях);
    - обязательные аргументы - только пути к папкам и имя БД.
- Наглядность:
    - есть инструмент вывода примерной структуры интересующей БД.
- Автоматизация:
    - одна команда — обработка всех коллекций базы.
    - полностью автоматическое чтение и создание VCF/BED.
- Гибкость:
    - индексируемые, а значит быстро парсимые, вложенные поля.
- Конвейерность:
    - результаты можно перенаправлять в другую БД, минуя создание конечных файлов (но есть [баг](https://github.com/PlatonB/high-perf-bio/issues/22)).
- Техподдержка:
    - `-h`: вывод тщательно продуманной справки по синтаксису команд.
    - [Тьюториал](#тьюториал) прямо в этом ридми.
    - Всегда бесплатная [консультация](https://github.com/PlatonB/high-perf-bio/issues).

## Недостатки.
- Высокая производительность возможна лишь при вытаскивании небольших частей коллекций. Преимущество в скорости теряется, если запросить, к примеру, половину _dbSNP_.
- Индексы целиком размещаются в RAM. Готовьтесь к тому, что придётся запихивать в оперативку весящий 15.6 ГБ индекс поля ID _dbSNP_.
- Наличие виртуальных ядер процессора не обеспечивает прироста скорости при распараллеливании.
![hpb_create_benchmark](https://user-images.githubusercontent.com/25541767/163469386-122134f4-3a07-46a6-b926-12324093d390.png)
- Инструмент _ljoin_ не использует индексы при пересечении/вычитании по геномным координатам. Создатели _MongoDB_ знают о лежащей в основе проблеме, и, надеюсь, когда-нибудь исправят.
- При скидывании результатов в другую БД сбивается последовательность полей. Голосуйте за исправление этого бага [тут](https://feedback.mongodb.com/forums/924280-database/suggestions/44875462-preserve-field-order-in-merge) (может потребоваться VPN).

# Перед началом работы.
## Чудо-команда установки.
```
wget https://github.com/PlatonB/high-perf-bio/archive/refs/heads/master.zip && unzip -oq master.zip && rm master.zip && cd high-perf-bio-master && bash __install_3rd_party.sh
```

Тулкит скачан и распакован, зависимости разрешены. В принципе, теперь можно приступать к эксплуатации. Но при желании ознакомиться с нюансами установки читайте идущие следом разделы.

## Установка сторонних компонентов.
| Программа | Предназначение |
| --------- | -------------- |
| _MongoDB_ | собственно, размещает данные в базы, индексирует поля и выполняет запросы |
| _PyMongo_ | позволяет сабжевому _Python_-тулкиту бесшовно работать с _MongoDB_-базами |
| _Streamlit_ | основа для веб-интерфейса этого инструментария |

Установка _MongoDB_ сложновата из-за невероятно абсурдного решения признать новую лицензию _MongoDB_ несвободной, повлекшего не менее идиотское удаление продукта из штатных репозиториев линуксовых дистрибутивов. Но в декабре-2022 в составе тулкита появился скрипт, ставящий _MongoDB_ и другие необходимые _high-perf-bio_ сторонние решения одной командой. Выполните 5 элементарных действий:
1. [Скачайте](https://github.com/PlatonB/high-perf-bio/archive/master.zip) архив с инструментарием.
2. Распакуйте его в любую папку.
3. В терминале перейдите в папку `high-perf-bio-master`:
```
cd /path/to/high-perf-bio-master
```
4. Разрешите зависимости:
```
bash __install_3rd_party.sh
```
5. Перезагрузитесь, иначе не заработает GUI тулкита.

## Удаление сторонних компонентов.
Это делается так же просто, как и установка:
1. `cd /path/to/high-perf-bio-master`
2. `bash __uninstall_3rd_party.sh`

## Примечания.
- Если хотите установить MongoDB вручную, то на официальном сайте СУБД есть [подробные инструкции](https://www.mongodb.com/docs/manual/installation/#mongodb-installation-tutorials).
- Скрипт-инсталлятор, найдя папку `miniconda3` в домашнем каталоге, установит _Streamlit_ с помощью _Conda_. В противном случае будет задействован _Pip_.
- Скрипт-инсталлятор ставит _PyMongo_ исключительно через _Pip_, ибо _Conda_-версия этого драйвера неработоспособна.
- Скрипт-деинсталлятор не удаляет _Pip_-зависимости _Streamlit_.
- Скрипт-деинсталлятор беспощадно сносит `/etc/mongod.conf` и другие конфиги. Если вы их правили, забэкапьте.
- _Windows_. Теоретически, после установки _MongoDB_ и whl-пакетов _PyMongo_ + _Streamlit_ программа должна работать. Но у меня сейчас этой ОС нет, и я пока не проверял. Надеюсь, кто-нибудь поделится опытом в [Issues](https://github.com/PlatonB/high-perf-bio/issues).

## Произвольное расположение _MongoDB_-данных.
### (опционально)
К примеру, у вас есть до неприличия маленький SSD и вполне просторный HDD (далее - V1). И так случилось, что _MongoDB_ заталкивает базы и логи в первое из перечисленных мест. Как сделать, чтобы эти данные хостились на V1?

1. Создайте папку для БД и лог-файл.
```
mkdir /mnt/V1/mongodb
```
```
touch /mnt/V1/mongod.log
```

P.S. Можно было и через файл-менеджер;).

2. Предельно аккуратно укажите пути к свежеупомянутым папке и файлу в главном конфиге _MongoDB_:
```
sudo nano /etc/mongod.conf
```
```
<...>
storage:
  dbPath: /mnt/V1/mongodb
<...>
systemLog:
  <...>
  path: /mnt/V1/mongod.log
<...>
```

Сохраните изменения (`CTRL+S`) и выйдите из редактора (`CTRL+X`).

3. Предоставьте необходимые права доступа нашим переселенцам:
```
sudo chown mongodb -R /mnt/V1/mongodb
```
```
sudo chown mongodb -R /mnt/V1/mongod.log
```

## _MongoDB Compass_.
### (опционально)
Рекомендую устанавливать с [Flathub](https://flathub.org/apps/details/com.mongodb.Compass). _Compass_ - родной GUI к _MongoDB_. В нашем случае он позволяет использовать _high-perf-bio_ не вслепую: делает возможным просмотр создаваемых компонентами тулкита коллекций и индексов. _Compass_ также даёт простор для ручного/продвинутого администрирования БД: составлять запросы, оценивать их производительность, экспериментировать с агрегациями и т.д..
![MongoDB_Compass_high-perf-bio](https://user-images.githubusercontent.com/25541767/188226634-539245f2-7aed-4e11-ad6b-f587cb6cd18d.png)

## GUI.
В июне 2022 года графический интерфейс на основе _Streamlit_ достиг функционального паритета с консольным. Но, живя в мире биоинформатики, лучше [привыкать к командной строке](https://github.com/PlatonB/ngs-pipelines#преодолеваем-страх-командной-строки-linux).

### Запуск GUI.
#### Gnome.
Правой кнопкой по `__run_gui_streamlit.sh` --> `Свойства` --> `Права` --> галочку на `Разрешить выполнение файла как программы`. Это надо сделать лишь однократно. В контекстном меню `__run_gui_streamlit.sh` появится пункт `Запустить как программу`. Каждый раз после нажатия на `Запустить как программу` будет открываться вкладка браузера с GUI к _high-perf-bio_, а также, с чисто технической целью, окно _Терминала_. Как закончите работать с тулкитом, просто закройте и то, и другое.

#### KDE.
Правой кнопкой по `__run_gui_streamlit.sh` --> `Свойства` --> `Права` --> галочку на `Является выполняемым` --> `OK`. Правой кнопкой по `__run_gui_streamlit.sh` --> `Открыть с помощью...` --> `Другое приложение...` --> `Система` --> выделите `Konsole` --> `OK`. Вылезет консоль - закройте её. Левой кнопкой по `__run_gui_streamlit.sh` --> галочку на `Больше не задавать этот вопрос` --> `Запустить`. Вышеперечисленное надо сделать лишь один раз. После этих манипуляций GUI будет напрямую запускаться левым кликом по `__run_gui_streamlit.sh`.

#### Командная строка.
```
streamlit run _gui_streamlit.py
```

# Тьюториал.
## Основы и игрушечные примеры.
Управление через командный и графический интерфейсы схоже, и здесь для краткости я буду приводить только [команды](https://github.com/PlatonB/ngs-pipelines#преодолеваем-страх-командной-строки-linux).

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
python3 create.py -h
```
```
python3 annotate.py -h
```
и т.д..

У меня нет цели продемонстрировать в тьюториале все аргументы программ. Наоборот, хочу показать, как обойтись минимумом таковых.

В папке `high-perf-bio-master` уже есть небольшие примеры данных. Но в качестве материала для базы лучше скачать что-то серьёзное, например, [VCF-таблицу всех SNP](https://ftp.ncbi.nih.gov/snp/latest_release/VCF/GCF_000001405.39.gz). Переименуйте её, добавив расширение vcf перед gz.

Поскольку формат этого файла — VCF, программа создания БД сама отбросит метастроки, сконвертирует хромосомы из HGVS в обычный вид, преобразует ячейки определённых столбцов в BSON-структуры и проиндексирует поля с хромосомами, позициями и rsIDs. Файл — один, значит, распараллелить заливку не удастся. Таким образом, команда может получиться весьма минималистичной. Достаточно подать путь к папке со свежескачанным VCF.
```
python3 create.py -S $HOME/Биоинформатика/dbSNP
```

При работе с _high_perf_bio_ высока вероятность возникновения двух проблем: вы забыли, как называется БД и из чего она состоит. В таких ситуациях перед запуском любого парсера требуется получить представление о парсимой БД. Для начала найдите её имя в списке всех имён.
```
python3 info.py
```

Затем выведите первостепенные характеристики конкретной базы.
```
python3 info.py -d dbSNP
```

Теперь мы многое знаем о БД и можем, к примеру, осуществить по ней запрос. Для разминки отберём снипы интервала `chr14:105746998-105803861`, упомянутого в статье "**Локусы влияющие на экспрессию антигенов HLA в участке 14-й хромосомы, ассоциированном с развитием рассеянного склероза, и функции расположенных в них генов**" (П.А. Быкадоров и др.). Создадим текстовый файл `get_ms_locus.txt`, впишем туда запрос и выполним его программой _query_. Помните, что операторы и строковые значения надо заключать в кавычки (неважно, одинарные или двойные), а целочисленные значения оставлять без ничего.
Содержимое `get_ms_locus.txt`:
```
{'#CHROM': 14, 'POS': {'$gte': 105746998, '$lte': 105803861}}
```

Команда, которая прочитает запрос из файла и выполнит его:
```
python3 query.py -S $HOME/Биоинформатика/toy_queries/get_ms_locus.txt -D dbSNP -T $HOME/Биоинформатика/ms_locus
```

Кстати, инструмент _query_ гораздо мощнее, чем могло показаться по примеру выше. Можно задавать не один, а сколько угодно запросов, а также тематически распределять их по разным файлам.

Ещё, чтобы лучше привыкнуть к _high_perf_bio_, найдём характеристики вариантов из папки `high-perf-bio-master/test_data/TSV`. С расположением rsID-столбца повезло — он первый. С единственной коллекцией базы `dbSNP` тоже всё ок — поскольку это ex-VCF, то там есть поле ID. Значит, обойдёмся минимумом аргументов: пути к папке с игрушечной таблицей и конечной папке, имя базы и игнор табличной шапки.
```
python3 annotate.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -D dbSNP -T $HOME/Биоинформатика/TSV_ann -m 1
```

## Пример чуть посерьёзнее.
А вот задача явно ближе к реальной биоинформатике. Вы когда нибудь задумывались, есть ли на свете варианты, являющиеся eQTL сразу во всех тканях? Сейчас посмотрим. Скачаем [таблицы пар вариант-ген](https://storage.googleapis.com/gtex_analysis_v8/single_tissue_qtl_data/GTEx_Analysis_v8_eQTL.tar). Нам понадобятся только `*.signif_variant_gene_pairs.txt.gz`-файлы. Таблицы так себе приспособлены к анализу: координаты, аллели и номер сборки запихнуты в один столбец, а rsIDs вовсе отсутствуют. Сконвертировать это непарсибельное хозяйство в VCF предлагаю [скриптом](https://github.com/PlatonB/for-fun/blob/master/eqtls_tsv_to_vcf.py), а обогатить VCFs сниповыми идентификаторами — _high-perf-bio_-плагином _revitalize_id_column_. Дарить rsIDs VCF-файлам готова ранее созданная нами _MongoDB_-версия _dbSNP_. Насыщение поля `ID` идентификаторами может оказаться неполным. Чтобы не оставалось строк с точками, добавим аргумент `-r`.
```
cd plugins
```
```
python3 revitalize_id_column.py -S $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF -D dbSNP -T $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF_rsIDs -r
```

Зальём удобоваримую версию _GTEx_-таблиц в базу. Коллекций — несколько десятков, поэтому лично я предпочёл бы распараллелить размещение и индексацию на 8 процессов.
```
python3 create.py -S $HOME/Биоинформатика/GTEx_Analysis_v8_eQTL_VCF_rsIDs -d GTEx -p 8
```

Напомню, мы хотели найти варианты, дающие о себе знать во всех тканях. Применим алгоритм пересечения. Что такое левые/правые коллекции и охват, подробно рассказано в справке _ljoin_ — здесь я на этом останавливаться не буду. Указываем в явном виде одну левую коллекцию. Любую. Правыми коллекциями для решения задачи надо сделать все остальные, но перечислять их не нужно — в программе такой сценарий предусмотрен по умолчанию. То, что нам нужно пересекать, а не вычитать, опять же, явно прописывать не обязательно. С наличием идентификаторов у нас проблем нет, поэтому можно работать по ним, хотя координатные вычисления программой тоже поддерживаются. Подавать аргумент с именем пересекаемого поля не будем, т.к. по дефолту идёт ID. eQTL нам общие для всех тканей надо найти? Значит, задаём охват максимальный. Он равен количеству всех коллекций минус 1 (вычли одну левую). Программа, кстати, позволяет исследователю не тратить время на определение этого значения, а указать `0`.
```
python3 ljoin.py -D GTEx -T $HOME/Биоинформатика/GTEx_common -l Adipose_Subcutaneous.v8.signif_variant_gene_pairs_rsIDs.vcf -c 0
```

Вездесущие eQTL обнаружились, и их 224045 штук. А сколько будет вариантов, влияющих на экспрессию генов только в крови и больше нигде? Это решается вычитанием.
```
python3 ljoin.py -D GTEx -T $HOME/Биоинформатика/GTEx_onlyWB -l Whole_Blood.v8.signif_variant_gene_pairs_rsIDs.vcf -a subtract -c 0
```

Таких уникальных вариантов — 86051 из 2399784 кровяных.

Найти что ли клинически значимые варианты среди эксклюзивно кровяных? Воспользуемся [таблицей ассоциаций вариант-патология](https://www.disgenet.org/static/disgenet_ap1/files/downloads/all_variant_disease_associations.tsv.gz) проекта _DisGeNET_. Эта таблица — небиоинформатического формата, поэтому при создании на её основе базы и дальнейшей работе с этой базой применимы не все удобные умолчания.
```
python3 create.py -S $HOME/Биоинформатика/DisGeNET -s semicolon -i snpId,chromosome,position
```

Можно, кстати, из этой БД создать новую, которая разбита на похромосомные коллекции, урезанные до 5 самых необходимых полей.
```
python3 split.py -D DisGeNET -T DisGeNET_chrs -f chromosome -k snpId,chromosome,position,diseaseName,score -i snpId,chromosome,position
```

Ой, кажется, забыли проиндексировать поле со скорами ассоциаций. С кем не бывает? Воспользуемся штатным редактором индексов, чтобы потом программы, отсекающие малоподтверждённые пары, не тратили силы на тупой перебор скоров.
```
python3 reindex.py -D DisGeNET_chrs -a score
```

Теперь, наконец, аннотируем уникальные Whole Blood eQTLs по _DisGeNET_. Результаты кидаем в свежую БД.
```
python3 annotate.py -S $HOME/Биоинформатика/GTEx_onlyWB -D DisGeNET_chrs -T GTEx_onlyWB_Clin -f snpId -i snpId,chromosome,position,score
```

Хоть какое-то упоминание в _DisGeNET_ имеют 1914 кровяных вариантов. Отбирём наиболее клинически значимые. Как в _DisGeNET_ присваиваются скоры? Если ассоциация упоминается в отобранных нейросетевым парсером _BeFree_ статьях, ей прибавляют скор `0.01 * количество_статей`, но не более `0.1`. Если ассоциация взята из одного курируемого источника (_UniProt_, _ClinVar_, _GWAS Catalog_, _GWASdb_) - то за ней сразу бронируется аж `0.7`. Два курируемых источника - `0.8`, три и более - `0.9`. Хочется, конечно, выставить `0.9`, но тогда ничего не останется. Сделаем `0.81`, чтобы связь подтверждалась, как минимум, двумя солидными базами и одной статьёй.
```
{'score': {'$gte': Decimal128('0.81')}}
```
```
python3 query.py -S $HOME/Биоинформатика/queries/parse_GTEx.txt -D GTEx_onlyWB_Clin -T GTEx_onlyWB_strongClin
```

Результаты разбросаны по хромосомам, а названия болезней иногда дублируются. Как тогда собрать список болезней? Очень просто: сконкатенировать коллекции, применив подходящие опции. Поскольку мы используем отбор полей, `-u` уникализирует не целиковые документы, а усечённые с помощью `-k`.
```
python3 concatenate.py -D GTEx_onlyWB_strongClin -T GTEx_onlyWB_081Dis -k diseaseName -u
```

Результат. Варианты, влияющие на экспрессию генов исключительно в крови, обуславливают развитие, вероятнее всего, перечисленных 10 патологий:
**"Psoriasis", "Inflammatory Bowel Diseases", "Vitiligo", "Crohn Disease", "Diabetes Mellitus, Non-Insulin-Dependent", "Age related macular degeneration", "ovarian neoplasm", "Behcet Syndrome", "IGA Glomerulonephritis", "Hirschsprung Disease"**.

[Расскажите](https://github.com/PlatonB/high-perf-bio/issues) о применении _high_perf_bio_ в своих исследованиях!

# Механизм пересечения и вычитания.
Этот раздел — попытка максимально доступно объяснить, как работает программа _ljoin_.

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
По справке к _ljoin_ вы уже знаете, что каждая из коллекций, условно обозначенных как "левые", фильтруется по всем "правым". Левой пусть будет `c1`, а в качестве правых — `c2` и `c3`. Непосредственно обрабатываемое поле — допустим, `f1`. Общее представление об условях отбора документов `c1`:
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
python3 create.py -S $HOME/Биоинформатика/dbSNP_common -i #CHROM,POS,ID
```
```
create.py: error: argument -i/--ind-col-names: expected one argument
```

Из-за наличия решётки интерпретатор подумал, что `CHROM,POS,ID` — комментарий, а не перечисление индексируемых полей. Получилось, что аргумент `-i`, строго требующий значения, остался без такового. Надо было просто взять набор имён полей в одинарные кавычки (заэкранировать).
```
python3 create.py -S $HOME/Биоинформатика/dbSNP_common -i '#CHROM,POS,ID'
```

А знаете, почему программы _high_perf_bio_ заставляют перечислять что-либо через запятую без привычного нам по естественным языкам пробела? Не подумайте, что из вредности:). Пробел считается границей между аргументами. Т.е., при использовании запятых с пробелом каждый следующий элемент воспринимался бы шеллом в качестве нового аргумента.
```
python3 create.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -i SYMBOL, AF
```
```
create.py: error: unrecognized arguments: AF
```

Требование перечислять без пробела я ввёл, чтобы избавить вас от необходимости экранирования. Правильное оформление последней команды не подразумевает обязательного наличия надоедливых кавычек.
```
python3 create.py -S $HOME/Биоинформатика/high-perf-bio-master/test_data/TSV -i SYMBOL,AF
```
