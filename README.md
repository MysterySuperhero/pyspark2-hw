# pyspark2-hw

Включаем Python3:

`source /opt/rh/rh-python36/enable`

Создаем папки для исходных данных:

`hdfs dfs -mkdir -p /user/cloudera/data`

`hdfs dfs -mkdir -p /user/cloudera/data/input`

## Задача 1. Средний рейтинг
Реализуйте подсчет среднего рейтинга продуктов. Результат сохранить в HDFS в файле "avg_rating.csv". Формат каждой записи: ProdId,Rating

Копируем исходные данные:

`~$ hdfs dfs -copyFromLocal -f samples_100.json /user/cloudera/data/input`

Проверяем, что все скопировалось:

`~$ hdfs dfs -ls /user/cloudera/data/input`

Сабмитим код:

`~$ hdfs dfs -rm -r /user/cloudera/data/output_spark_avg_rating`

`~$ spark2-submit --master yarn <file_name> hdfs:///user/cloudera/data/input/sample_full.json  hdfs:///user/cloudera/data/output_spark_avg_rating`

Проверяем, что есть какой-то результат:

`~$ hdfs dfs -ls /user/cloudera/data/output_spark_avg_rating`

`~$ hdfs dfs -cat /user/cloudera/data/output_spark_avg_rating/part-*`

Склеить файлы в .csv

`~$ hdfs dfs -getmerge /user/cloudera/data/output_spark_avg_rating /home/cloudera/Desktop/1_1/avg_rating.csv`

## Задача 2. Добавление наименования продукта
Напишите программу, которая каждому ProdId из "avg_rating.csv" ставит в соответстие названием продукта. Результат сохранить в HDFS в файле "prodname_avg_rating.csv": ProdId,Name,Rating

`~$ hdfs dfs -copyFromLocal -f avg_rating.csv /user/cloudera/data/input`

`~$ hdfs dfs -copyFromLocal -f meta_Electronics.json /user/cloudera/data/input`

Проверяем, что все скопировалось:

`~$ hdfs dfs -ls /user/cloudera/data/input`

Запуск:

`~$ hdfs dfs -rm -r /user/cloudera/data/output_spark_prod_title`
`~$ spark2-submit --master yarn <file_name> hdfs:///user/cloudera/data/input/avg_rating.csv hdfs:///user/cloudera/data/input/meta_Electronics.json  hdfs:///user/cloudera/data/output_spark_prod_title`

Проверяем, что есть какой-то результат:

`~$ hdfs dfs -ls /user/cloudera/data/output_spark_prod_title`

Склеить файлы в .csv:

`~$ hdfs dfs -getmerge /user/cloudera/data/output_spark_prod_title /home/cloudera/Desktop/1_1/prodname_avg_rating.csv`

## Задача 3. Поиск среднего рейтинга по названию продукта
Напишите программу, которая выводит средний рейтинги всех продуктов из "prodname_avg_rating.csv", в названии которых встречается введенное при запуске слово: ProdId,Name,Rating

Проверяем, что все есть файл prodname_avg_rating.csv:

`~$ hdfs dfs -ls /user/cloudera/data/input`

Если нет, то копируем:
`~$ hdfs dfs -copyFromLocal -f prodname_avg_rating.csv /user/cloudera/data/input`
