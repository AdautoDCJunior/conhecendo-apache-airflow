from pyspark.sql import SparkSession, functions as f
from os import path
from pathlib import Path


if __name__ == '__main__':
  spark = SparkSession\
    .builder\
    .appName('twitter_insight_tweet')\
    .getOrCreate()

  path_src = path.join(
    str(Path('~/cursos/alura/conhecendo-apache-airflow').expanduser()),
    'datalake/silver/twitter_aluraonline/tweet'
  )

  tweet = spark.read.json(path_src)

  alura = tweet\
    .where(tweet['author_id'] == '1566580880')\
    .select('author_id', 'conversation_id')

  tweet = tweet.alias('tweet')\
    .join(
      alura.alias('alura'),
      [
        alura.author_id != tweet.author_id,
        alura.conversation_id == tweet.conversation_id
      ],
      'left'
    ).withColumn(
      'alura_conversation',
      f.when(
        f.col('alura.conversation_id').isNotNull(), 1
      ).otherwise(0)
    ).withColumn(
      'reply_alura',
      f.when(
        f.col('tweet.in_reply_to_user_id') == '1566580880', 1
      ).otherwise(0)
    ).groupBy(
      f.to_date('created_at').alias('created_date')
    ).agg(
      f.count_distinct('id').alias('n_tweets'),
      f.count_distinct('tweet.conversation_id').alias('n_conversation'),
      f.sum('alura_conversation').alias('alura_conversation'),
      f.sum('reply_alura').alias('reply_alura')
    ).withColumn(
      'week_day',
      f.date_format('created_date', 'E'))

  path_dest = path.join(
    str(Path('~/cursos/alura/conhecendo-apache-airflow').expanduser()),
    'datalake/gold/twitter_insight_tweet'
  )

  tweet.coalesce(1)\
    .write\
    .json(path_dest)