#Sec3->create spark df using python collection and pandas df
# Create Single Column Spark Dataframe using List

help(spark.createDataFrame)
'''
createDataFrame(data, schema=None, samplingRatio=None, verifySchema=True) method of pyspark.sql.session.SparkSession instance
Creates a :class:`DataFrame` from an :class:`RDD`, a list or a :class:`pandas.DataFrame`.

Examples
    --------
    >>> l = [('Alice', 1)]
    >>> spark.createDataFrame(l).collect()
    [Row(_1='Alice', _2=1)]
    >>> spark.createDataFrame(l, ['name', 'age']).collect()
    [Row(name='Alice', age=1)]
    
    >>> d = [{'name': 'Alice', 'age': 1}]
    >>> spark.createDataFrame(d).collect()
    [Row(age=1, name='Alice')]
    
    >>> rdd = sc.parallelize(l)
    >>> spark.createDataFrame(rdd).collect()
    [Row(_1='Alice', _2=1)]
    >>> df = spark.createDataFrame(rdd, ['name', 'age'])
    >>> df.collect()
    [Row(name='Alice', age=1)]
    
    >>> from pyspark.sql import Row
    >>> Person = Row('name', 'age')
    >>> person = rdd.map(lambda r: Person(*r))
    >>> df2 = spark.createDataFrame(person)
    >>> df2.collect()
    [Row(name='Alice', age=1)]
    
    >>> from pyspark.sql.types import *
    >>> schema = StructType([
    ...    StructField("name", StringType(), True),
    ...    StructField("age", IntegerType(), True)])
    >>> df3 = spark.createDataFrame(rdd, schema)
    >>> df3.collect()
    [Row(name='Alice', age=1)]
    
    >>> spark.createDataFrame(df.toPandas()).collect()  # doctest: +SKIP
    [Row(name='Alice', age=1)]
    >>> spark.createDataFrame(pandas.DataFrame([[1, 2]])).collect()  # doctest: +SKIP
    [Row(0=1, 1=2)]
    
    >>> spark.createDataFrame(rdd, "a: string, b: int").collect()
    [Row(a='Alice', b=1)]
    >>> rdd = rdd.map(lambda row: row[1])
    >>> spark.createDataFrame(rdd, "int").collect()
    [Row(value=1)]
    >>> spark.createDataFrame(rdd, "boolean").collect() # doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    Py4JJavaError: ...
'''

#02 Create Single Column Spark Dataframe using List(for Single Column  we need to to specify schema)

ages_list = [21, 23, 18, 41, 32]
names_list = ['Scott', 'Donald', 'Mickey']
spark.createDataFrame(ages_list, 'int') #Out[6]: DataFrame[value: int]
spark.createDataFrame(names_list, 'string') #Out[10]: DataFrame[value: string]
#or
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StringType
spark.createDataFrame(ages_list, IntegerType()) #Out[8]: DataFrame[value: int]
spark.createDataFrame(names_list, StringType()) #Out[12]: DataFrame[value: string]


#03 Create Multi Column Spark Dataframe using Python list, for list of tuples schema is not required
ages_list = [(21, ), (23, ), (41, ), (32, )]
type(ages_list) #list
type(ages_list[2])#tuple
spark.createDataFrame(ages_list) #DataFrame[_1: bigint]
spark.createDataFrame(ages_list, 'age int') #DataFrame[age: int]

users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
spark.createDataFrame(users_list) #DataFrame[_1: bigint, _2: string]
spark.createDataFrame(users_list, 'user_id int, user_first_name string') #DataFrame[user_id: int, user_first_name: string]


#04 Overview of Spark Row
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
df = spark.createDataFrame(users_list, 'user_id int, user_first_name string')
df.collect()#converts dataframe in to list of row objects
'''
Out[3]: [Row(user_id=1, user_first_name='Scott'),
 Row(user_id=2, user_first_name='Donald'),
 Row(user_id=3, user_first_name='Mickey'),
 Row(user_id=4, user_first_name='Elvis')]
'''

type(df.collect()) #list

from pyspark.sql import Row
help(Row)
''' 
class Row(builtins.tuple)
 |  Row(*args, **kwargs)
'''

r = Row("Alice", 11) #<Row('Alice', 11)>
row2 = Row(name="Alice", age=11) #Row(name='Alice', age=11)
row2.name #'Alice'
row2['name'] #'Alice'

#05 Convert List of Lists into Spark Dataframe using Row
users_list = [[1, 'Scott'], [2, 'Donald'], [3, 'Mickey'], [4, 'Elvis']]
spark.createDataFrame(users_list, 'user_id int, user_first_name string')

#better convert list of lists to Row object and create df using Row(*args, **kwargs)
from pyspark.sql import Row
users_rows = [Row(*user) for user in users_list] #[<Row(1, 'Scott')>, <Row(2, 'Donald')>, <Row(3, 'Mickey')>, <Row(4, 'Elvis')>]
spark.createDataFrame(users_rows, 'user_id int, user_first_name string') #DataFrame[user_id: int, user_first_name: string]

#06 Convert List of Tuples into Spark Dataframe using Row
users_list = [(1, 'Scott'), (2, 'Donald'), (3, 'Mickey'), (4, 'Elvis')]
spark.createDataFrame(users_list, 'user_id int, user_first_name string')

from pyspark.sql import Row
users_rows = [Row(*user) for user in users_list]
spark.createDataFrame(users_rows, 'user_id int, user_first_name string')

#07 Convert List of Dicts into Spark Dataframe using Row
users_list = [
    {'user_id': 1, 'user_first_name': 'Scott'},
    {'user_id': 2, 'user_first_name': 'Donald'},
    {'user_id': 3, 'user_first_name': 'Mickey'},
    {'user_id': 4, 'user_first_name': 'Elvis'}
]
spark.createDataFrame(users_list) # DataFrame[user_first_name: string, user_id: bigint]
# creating df from list of dicts will be deprecated so better convert to Row and create df
from pyspark.sql import Row
users_rows = [Row(*user.values()) for user in users_list] #using *args notation
#Out[20]: [<Row(1, 'Scott')>, <Row(2, 'Donald')>, <Row(3, 'Mickey')>, <Row(4, 'Elvis')>]
spark.createDataFrame(users_rows, 'user_id bigint, user_first_name string') #DataFrame[user_id: bigint, user_first_name: string]

users_rows = [Row(**user) for user in users_list] #using **kwargs notation
'''
Out[10]: [Row(user_id=1, user_first_name='Scott'),
 Row(user_id=2, user_first_name='Donald'),
 Row(user_id=3, user_first_name='Mickey'),
 Row(user_id=4, user_first_name='Elvis')]
'''
spark.createDataFrame(users_rows) # DataFrame[user_id: bigint, user_first_name: string]


def dummy(**kwargs):
    print(kwargs)
    print(len(kwargs))
user_details = {'user_id': 1, 'user_first_name': 'Scott'}

dummy(**user_details) #unpacks dic
'''
{'user_id': 1, 'user_first_name': 'Scott'}
2
'''

dummy(user_id=1, user_first_name='Scott')
'''
{'user_id': 1, 'user_first_name': 'Scott'}
2
'''

dummy(user_details_ky=user_details)
'''
{'user_details_ky': {'user_id': 1, 'user_first_name': 'Scott'}}
1
'''
#*args unpacks list or tuple
def dummy(*args):
    print(args)
    print(len(args))

user_details = (1, 'Scott')
dummy(*user_details)
'''
(1, 'Scott')
2
'''

user_details = [1, 'Scott']
dummy(*user_details)
'''
(1, 'Scott')
2
'''

dummy(user_details)

([1, 'Scott'],)
1


#08 Overview of Basic Data Types in Spark

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]
from pyspark.sql import Row

users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()
'''
root
 |-- id: long (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- is_customer: boolean (nullable = true)
 |-- amount_paid: double (nullable = true)
 |-- customer_from: date (nullable = true)
 |-- last_updated_ts: timestamp (nullable = true)
'''
users_df.columns
'''
['id','first_name','last_name','email','is_customer','amount_paid','customer_from','last_updated_ts']
'''
users_df.dtypes
'''
[('id', 'bigint'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('email', 'string'),
 ('is_customer', 'boolean'),
 ('amount_paid', 'double'),
 ('customer_from', 'date'),
 ('last_updated_ts', 'timestamp')]
'''

#09 Specifying Schema for Spark Dataframe using String
users_schema = '''
    id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    is_customer BOOLEAN,
    amount_paid FLOAT,
    customer_from DATE,
    last_updated_ts TIMESTAMP
'''
spark.createDataFrame(users, schema=users_schema)

#10 Specifying Schema for Spark Dataframe using List
users_schema = [
    'id INT',
    'first_name STRING',
    'last_name STRING',
    'email STRING',
    'is_customer BOOLEAN',
    'amount_paid FLOAT',
    'customer_from DATE',
    'last_updated_ts TIMESTAMP'
]
spark.createDataFrame(users, schema=users_schema)

#11 Specifying Schema using Spark Types
from pyspark.sql.types import *
users_schema = StructType([
    StructField('id', IntegerType()),
    StructField('first_name', StringType()),
    StructField('last_name', StringType()),
    StructField('email', StringType()),
    StructField('is_customer', BooleanType()),
    StructField('amount_paid', FloatType()),
    StructField('customer_from', DateType()),
    StructField('last_updated_ts', TimestampType())
])
spark.createDataFrame(users, schema=users_schema)

spark.createDataFrame(users, schema=users_schema).rdd.collect()#convert df to rdd and then to list of Row objects

#12 Create Spark Dataframe using Pandas Dataframe
#if the elements in list of dictionary are not same throughout,convert to pd df and then create spark df
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "is_customer": False,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]
import pandas as pd
spark.createDataFrame(pd.DataFrame(users)).show()


#14 Array Type Columns in Spark Dataframes

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": ["+1 234 567 8901", "+1 234 567 8911"],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": ["+1 234 567 8923", "+1 234 567 8934"],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": ["+1 714 512 9752", "+1 714 512 6601"],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": ["+1 817 934 7142"],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]


from pyspark.sql import Row
users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()
'''
root
 |-- id: long (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- phone_numbers: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- is_customer: boolean (nullable = true)
 |-- amount_paid: double (nullable = true)
 |-- customer_from: date (nullable = true)
 |-- last_updated_ts: timestamp (nullable = true)
'''
users_df.select('id', 'phone_numbers').show(truncate=False)
'''
+---+----------------------------------+
|id |phone_numbers                     |
+---+----------------------------------+
|1  |[+1 234 567 8901, +1 234 567 8911]|
|2  |[+1 234 567 8923, +1 234 567 8934]|
|3  |[+1 714 512 9752, +1 714 512 6601]|
|4  |null                              |
|5  |[+1 817 934 7142]                 |
+---+----------------------------------+

'''


from pyspark.sql.functions import explode

users_df. \
    withColumn('phone_number', explode('phone_numbers')). \
    drop('phone_numbers'). \
    show()

'''
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+
| id|first_name|   last_name|               email|is_customer|amount_paid|customer_from|    last_updated_ts|   phone_number|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|+1 234 567 8901|
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|+1 234 567 8911|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|+1 234 567 8923|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|+1 234 567 8934|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|+1 714 512 9752|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|+1 714 512 6601|
|  5|      Kurt|        Rome|krome4@shutterfly...|      false|       null|         null|2021-04-02 00:55:18|+1 817 934 7142|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+

'''

from pyspark.sql.functions import explode_outer
users_df. \
    withColumn('phone_number', explode_outer('phone_numbers')). \
    drop('phone_numbers'). \
    show()
'''
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+
| id|first_name|   last_name|               email|is_customer|amount_paid|customer_from|    last_updated_ts|   phone_number|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|+1 234 567 8901|
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|+1 234 567 8911|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|+1 234 567 8923|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|+1 234 567 8934|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|+1 714 512 9752|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|+1 714 512 6601|
|  4|     Ashby|    Maddocks|  amaddocks3@home.pl|      false|       null|         null|2021-04-10 17:45:30|           null|
|  5|      Kurt|        Rome|krome4@shutterfly...|      false|       null|         null|2021-04-02 00:55:18|+1 817 934 7142|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+---------------+
'''


from pyspark.sql.functions import col

users_df. \
    select('id', col('phone_numbers')[0].alias('mobile'), col('phone_numbers')[1].alias('home')). \
    show()
+---+---------------+---------------+
| id|         mobile|           home|
+---+---------------+---------------+
|  1|+1 234 567 8901|+1 234 567 8911|
|  2|+1 234 567 8923|+1 234 567 8934|
|  3|+1 714 512 9752|+1 714 512 6601|
|  4|           null|           null|
|  5|+1 817 934 7142|           null|
+---+---------------+---------------+

#15 Map Type Columns in Spark Dataframes

import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": {"mobile": "+1 234 567 8901", "home": "+1 234 567 8911"},
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers": {"mobile": "+1 234 567 8923", "home": "+1 234 567 8934"},
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": {"mobile": "+1 714 512 9752", "home": "+1 714 512 6601"},
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": None,
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": {"mobile": "+1 817 934 7142"},
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

from pyspark.sql import Row
users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.printSchema()
'''
root
 |-- id: long (nullable = true)
 |-- first_name: string (nullable = true)
 |-- last_name: string (nullable = true)
 |-- email: string (nullable = true)
 |-- phone_numbers: map (nullable = true)
 |    |-- key: string
 |    |-- value: string (valueContainsNull = true)
 |-- is_customer: boolean (nullable = true)
 |-- amount_paid: double (nullable = true)
 |-- customer_from: date (nullable = true)
 |-- last_updated_ts: timestamp (nullable = true)
'''

users_df.select('id', 'phone_numbers').show(truncate=False)
'''
+---+----------------------------------------------------+
|id |phone_numbers                                       |
+---+----------------------------------------------------+
|1  |{mobile -> +1 234 567 8901, home -> +1 234 567 8911}|
|2  |{mobile -> +1 234 567 8923, home -> +1 234 567 8934}|
|3  |{mobile -> +1 714 512 9752, home -> +1 714 512 6601}|
|4  |null                                                |
|5  |{mobile -> +1 817 934 7142}                         |
+---+----------------------------------------------------+
'''
from pyspark.sql.functions import col
users_df. \
    select('id', col('phone_numbers')['mobile'].alias('mobile'), col('phone_numbers')['home'].alias('home')). \
    show()
'''
+---+---------------+---------------+
| id|         mobile|           home|
+---+---------------+---------------+
|  1|+1 234 567 8901|+1 234 567 8911|
|  2|+1 234 567 8923|+1 234 567 8934|
|  3|+1 714 512 9752|+1 714 512 6601|
|  4|           null|           null|
|  5|+1 817 934 7142|           null|
+---+---------------+---------------+
'''
from pyspark.sql.functions import explode
users_df.select('id', explode('phone_numbers')).show()
'''
+---+------+---------------+
| id|   key|          value|
+---+------+---------------+
|  1|mobile|+1 234 567 8901|
|  1|  home|+1 234 567 8911|
|  2|mobile|+1 234 567 8923|
|  2|  home|+1 234 567 8934|
|  3|mobile|+1 714 512 9752|
|  3|  home|+1 714 512 6601|
|  5|mobile|+1 817 934 7142|
+---+------+---------------+
'''

from pyspark.sql.functions import explode_outer
users_df.select('id', explode_outer('phone_numbers')).show()
'''
+---+------+---------------+
| id|   key|          value|
+---+------+---------------+
|  1|mobile|+1 234 567 8901|
|  1|  home|+1 234 567 8911|
|  2|mobile|+1 234 567 8923|
|  2|  home|+1 234 567 8934|
|  3|mobile|+1 714 512 9752|
|  3|  home|+1 714 512 6601|
|  4|  null|           null|
|  5|mobile|+1 817 934 7142|
+---+------+---------------+
'''

users_df.select('*', explode('phone_numbers')). \
    withColumnRenamed('key', 'phone_type'). \
    withColumnRenamed('value', 'phone_number'). \
    drop('phone_numbers'). \
    show()
'''
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+----------+---------------+
| id|first_name|   last_name|               email|is_customer|amount_paid|customer_from|    last_updated_ts|phone_type|   phone_number|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+----------+---------------+
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|    mobile|+1 234 567 8901|
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|      home|+1 234 567 8911|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|    mobile|+1 234 567 8923|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|      home|+1 234 567 8934|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|    mobile|+1 714 512 9752|
|  3|    Orelie|      Penney|openney2@vistapri...|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|      home|+1 714 512 6601|
|  5|      Kurt|        Rome|krome4@shutterfly...|      false|       null|         null|2021-04-02 00:55:18|    mobile|+1 817 934 7142|
+---+----------+------------+--------------------+-----------+-----------+-------------+-------------------+----------+---------------+
'''

#16 Struct Type Columns in Spark Dataframes

from pyspark.sql import Row
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

users_df = spark.createDataFrame([Row(**user) for user in users])
users_df.select('id', 'phone_numbers').show(truncate=False)
'''
+---+----------------------------------+
|id |phone_numbers                     |
+---+----------------------------------+
|1  |{+1 234 567 8901, +1 234 567 8911}|
|2  |{+1 234 567 8923, 1 234 567 8934} |
|3  |{+1 714 512 9752, +1 714 512 6601}|
|4  |{null, null}                      |
|5  |{+1 817 934 7142, null}           |
+---+----------------------------------+
'''
users_df.dtypes
'''
[('id', 'bigint'),
 ('first_name', 'string'),
 ('last_name', 'string'),
 ('email', 'string'),
 ('phone_numbers', 'struct<mobile:string,home:string>'),
 ('is_customer', 'boolean'),
 ('amount_paid', 'double'),
 ('customer_from', 'date'),
 ('last_updated_ts', 'timestamp')]
'''

users_df. \
    select('id', 'phone_numbers.mobile', 'phone_numbers.home'). \
    show()

#or
users_df. \
    select('id', 'phone_numbers.*'). \
    show()
'''
+---+---------------+---------------+
| id|         mobile|           home|
+---+---------------+---------------+
|  1|+1 234 567 8901|+1 234 567 8911|
|  2|+1 234 567 8923| 1 234 567 8934|
|  3|+1 714 512 9752|+1 714 512 6601|
|  4|           null|           null|
|  5|+1 817 934 7142|           null|
+---+---------------+---------------+
'''
from pyspark.sql.functions import col
users_df. \
    select('id', col('phone_numbers')['mobile'], col('phone_numbers')['home']). \
    show()
'''
+---+--------------------+------------------+
| id|phone_numbers.mobile|phone_numbers.home|
+---+--------------------+------------------+
|  1|     +1 234 567 8901|   +1 234 567 8911|
|  2|     +1 234 567 8923|    1 234 567 8934|
|  3|     +1 714 512 9752|   +1 714 512 6601|
|  4|                null|              null|
|  5|     +1 817 934 7142|              null|
+---+--------------------+------------------+
'''


Question 1:
You need to create a Data Frame by specifying the schema for the following data. Which one of the following is the right answer. The schema should contain 2 fields with names id and first_name using data types INT and STRING respectively.


schema= ['id INT', 'fn STRING']
incorrect answer. Please try again.
Even though Schema can be specified as list of strings, the data type will be inferred based on the data. When the schema is specified in this manner, the column names will be incorrect. The names will have data types as well and also there will be additional information related to data types which are inferred from the data.

schema= ' id INT, fn STRING'
Correct Answer, we should be able to define the schema as string. The string should contain column names and data types separated by comma (,).


#Sec4 ->select and rename cols
from pyspark.sql import Row
import datetime
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]

'''
Enabling for Conversion to/from Pandas
Apache Arrow is an in-memory columnar data format that is used in Spark to efficiently transfer data between JVM and Python processes. This currently is most beneficial to Python users that work with Pandas/NumPy data
Arrow is available as an optimization when converting a Spark DataFrame to a Pandas DataFrame using the call toPandas() and when creating a Spark DataFrame from a Pandas DataFrame with createDataFrame(pandas_df). To use Arrow when executing these calls, users need to first set the Spark configuration spark.sql.execution.arrow.pyspark.enabled to true. This is disabled by default.
'''
import pandas as pd
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)
users_df = spark.createDataFrame(pd.DataFrame(users))
users_df.show()
'''
+---+----------+------------+--------------------+--------------------+-------+-----------+-----------+-------------+-------------------+
| id|first_name|   last_name|               email|       phone_numbers|courses|is_customer|amount_paid|customer_from|    last_updated_ts|
+---+----------+------------+--------------------+--------------------+-------+-----------+-----------+-------------+-------------------+
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|{+1 234 567 8901,...| [1, 2]|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|{+1 234 567 8923,...|    [3]|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|
|  3|    Orelie|      Penney|openney2@vistapri...|{+1 714 512 9752,...| [2, 4]|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|
|  4|     Ashby|    Maddocks|  amaddocks3@home.pl|        {null, null}|     []|      false|        NaN|         null|2021-04-10 17:45:30|
|  5|      Kurt|        Rome|krome4@shutterfly...|{+1 817 934 7142,...|     []|      false|        NaN|         null|2021-04-02 00:55:18|
+---+----------+------------+--------------------+--------------------+-------+-----------+-----------+-------------+-------------------+
'''



#03 Overview of Narrow and Wide Transformations
'''

    Here are the functions related to narrow transformations. Narrow transformations doesn't result in shuffling. These are also known as row level transformations.
        df.select
        df.filter
        df.withColumn
        df.withColumnRenamed
        df.drop
    Here are the functions related to wide transformations.
        df.distinct
        df.union or any set operation
        df.join or any join operation
        df.groupBy
        df.sort or df.orderBy
    Any function that result in shuffling is wide transformation. For all the wide transformations, we have to deal with group of records based on a key.

'''

#04 Overview of Select on Spark Data Frame

%run "./02 Creating Spark Data Frame to Select and Rename Columns"
help(users_df.select)
'''
select(*cols) method of pyspark.sql.dataframe.DataFrame instance
    Projects a set of expressions and returns a new :class:`DataFrame`.
    
    .. versionadded:: 1.3.0
    
    Parameters
    ----------
    cols : str, :class:`Column`, or list
        column names (string) or expressions (:class:`Column`).
        If one of the column names is '*', that column is expanded to include all columns
        in the current :class:`DataFrame`.
    
    Examples
    --------
    >>> df.select('*').collect()
    [Row(age=2, name='Alice'), Row(age=5, name='Bob')]
    >>> df.select('name', 'age').collect()
    [Row(name='Alice', age=2), Row(name='Bob', age=5)]
    >>> df.select(df.name, (df.age + 10).alias('age')).collect()
    [Row(name='Alice', age=12), Row(name='Bob', age=15)]

'''

users_df.select('*').show()
users_df.select('id', 'first_name', 'last_name').show()
users_df.select(['id', 'first_name', 'last_name']).show()

# Defining alias to the dataframe
users_df.alias('u').select('u.*').show()
users_df.alias('u').select('u.id', 'u.first_name', 'u.last_name').show()

from pyspark.sql.functions import col
users_df.select(col('id'), 'first_name', 'last_name').show()

from pyspark.sql.functions import col, concat, lit
users_df.select(
    col('id'), 
    'first_name', 
    'last_name',
    concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')
).show()


#05 Overview of selectExpr on Spark Data Frame

help(users_df.selectExpr)
'''
selectExpr(*expr) method of pyspark.sql.dataframe.DataFrame instance
    Projects a set of SQL expressions and returns a new :class:`DataFrame`.
    This is a variant of :func:`select` that accepts SQL expressions.
 Examples
    --------
    >>> df.selectExpr("age * 2", "abs(age)").collect()
    [Row((age * 2)=4, abs(age)=2), Row((age * 2)=10, abs(age)=5)]
'''
#pass arthimetic expressions or sql functions not pyspark sql funcs

#mostly same as select

users_df.selectExpr('*').show()
# Defining alias to the dataframe
users_df.alias('u').selectExpr('u.*').show()

users_df.selectExpr('id', 'first_name', 'last_name').show()
users_df.selectExpr(['id', 'first_name', 'last_name']).show()


# Using selectExpr to use Spark SQL Functions, no need of any import for selectExpr
users_df.selectExpr('id', 'first_name', 'last_name', "concat(first_name, ', ', last_name) AS full_name").show()

#equivalent of above using select
users_df. \
    select(
        'id', 'first_name', 'last_name', 
        concat(col('first_name'), lit(', '), col('last_name')).alias('full_name')
    ). \
    show()

users_df.createOrReplaceTempView('users')
spark.sql("""SELECT id, first_name, last_name,concat(first_name, ', ', last_name) AS full_name FROM users"""). show()


#06 Referring Columns using Spark Data Frame Names

users_df['id']  #Out[7]: Column<'id'>
type(users_df['id'])   #pyspark.sql.column.Column

from pyspark.sql.functions import col
col('id')    #Out[7]: Column<'id'>

users_df.select('id', col('first_name'), 'last_name').show()

#with select can refer column using users_df['id'] notation
users_df.select(users_df['id'], col('first_name'), 'last_name').show()

users_df. \
    select(
        'id', 'first_name', 'last_name', 
        concat(users_df['first_name'], lit(', '), col('last_name')).alias('full_name')
    ). \
    show()


#below will work .notation for alias
users_df.alias('u').select('u.id', col('first_name'), 'last_name').show()

# below does not work as there is no object by name u in this session.
users_df.alias('u').select(u['id'], col('first_name'), 'last_name').show() #NameError: name 'u' is not defined


# Using selectExpr to use Spark SQL Functions
users_df.alias('u').selectExpr('id', 'first_name', 'last_name', "concat(u.first_name, ', ', u.last_name) AS full_name").show()

# This does not work as selectExpr can only take column names or SQL style expressions on column names
users_df.selectExpr(col('id'), 'first_name', 'last_name').show() #TypeError: Column is not iterable


users_df.createOrReplaceTempView('users')
spark.sql("""
    SELECT id, first_name, last_name,
        concat(u.first_name, ', ', u.last_name) AS full_name
    FROM users AS u
"""). \
    show()

#07 Understanding col function in Spark
from pyspark.sql.functions import col
cols = ['id', 'first_name', 'last_name']
users_df.select(*cols).show()

help(col)
'''
col(col)
    Returns a :class:`~pyspark.sql.Column` based on the given column name.'
    Examples
    --------
    >>> col('x')
    Column<'x'>
    >>> column('x')
    Column<'x'>
'''

user_id = col('id')
users_df.select(user_id).show()

'''
There are quite a few functions available on top of column type

    cast (can be used on all important data frame functions such as select, filter, groupBy, orderBy, etc)
    asc, desc (typically used as part of sort or orderBy)
    contains (typically used as part of filter or where)


'''

#ex using cast func on customer_from col
users_df.select(
    col('id'), 
    date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
).show()


cols = [col('id'), date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')]
users_df.select(*cols).show()
'''
+---+-------------+
| id|customer_from|
+---+-------------+
|  1|     20210115|
|  2|     20210214|
|  3|     20210121|
|  4|         null|
|  5|         null|
+---+-------------+
'''

#08 Invoking Functions using Spark Column Objects
from pyspark.sql.functions import col, lit, concat
full_name_col = concat(col('first_name'), lit(', '), col('last_name'))
full_name_alias = full_name_col.alias('full_name')
users_df.select('id', full_name_alias).show()
'''
+---+--------------------+
| id|           full_name|
+---+--------------------+
|  1|Corrie, Van den Oord|
|  2|   Nikolaus, Brewitt|
|  3|      Orelie, Penney|
|  4|     Ashby, Maddocks|
|  5|          Kurt, Rome|
+---+--------------------+
'''

#Convert data type of customer_from date to numeric type
from pyspark.sql.functions import date_format
customer_from_alias = date_format('customer_from', 'yyyyMMdd').cast('int').alias('customer_from')
users_df.select('id', customer_from_alias).show()

#09 Understanding lit function in Spark

from pyspark.sql.functions import lit,col

# lit returns column type
lit(25.0) #Out[24]: Column<'25.0'>

users_df.createOrReplaceTempView('users')
spark.sql("""
    SELECT id, (amount_paid + 25) AS amount_paid
    FROM users
"""). \
    show()

users_df. \
    selectExpr('id', '(amount_paid + 25) AS amount_paid'). \
    show()

#use col() and lit toagther to avoid errors when we add to numerics to coulumns
users_df.select('id', col('amount_paid') + lit(25.0)).show()

# This will fail
users_df.select('id', 'amount_paid' + 25).show()
# This will also fail
users_df.select('id', 'amount_paid' + '25').show()
    
# 10 Overview of Renaming Spark Data Frame Columns or Expressions 
'''
There are multiple ways to rename Spark Data Frame Columns or Expressions.

    We can rename column or expression using alias as part of select
    We can add or rename column or expression using withColumn on top of Data Frame.
    We can rename one column at a time using withColumnRenamed on top of Data Frame.
    We typically use withColumn to perform row level transformations and then to provide a name to the result. If we provide the same name as existing column, then the column will be replaced with new one.
    If we want to just rename the column then it is better to use withColumnRenamed.
    If we want to apply any transformation, we need to either use select or withColumn
    We can rename bunch of columns using toDF.
'''

#11 Naming derived columns using withColumn
help(users_df.withColumn)
'''
withColumn(colName, col) method of pyspark.sql.dataframe.DataFrame instance
    Returns a new :class:`DataFrame` by adding a column or replacing the
    existing column that has the same name.
    
    The column expression must be an expression over this :class:`DataFrame`; attempting to add
    a column from some other :class:`DataFrame` will raise an error.
    
    .. versionadded:: 1.3.0
    
    Parameters
    ----------
    colName : str
        string, name of the new column.
    col : :class:`Column`
        a :class:`Column` expression for the new column.
    
    Notes
    -----
    This method introduces a projection internally. Therefore, calling it multiple
    times, for instance, via loops in order to add multiple columns can generate big
    plans which can cause performance issues and even `StackOverflowException`.
    To avoid this, use :func:`select` with the multiple columns at once.
    
    Examples
    --------
    >>> df.withColumn('age2', df.age + 2).collect()
    [Row(age=2, name='Alice', age2=4), Row(age=5, name='Bob', age2=7)]

'''
from pyspark.sql.functions import col
users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('firnm', users_df['first_name']). \
    show()

from pyspark.sql.functions import concat, lit
users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumn('full_name', concat('first_name', lit(', '), 'last_name')). \
    show()

# Equivalent logic using select
users_df. \
    select(
        'id', 'first_name', 'last_name',
        concat('first_name', lit(', '), 'last_name').alias('full_name')
    ). \
    show()

#12 Renaming Columns using withColumnRenamed

help(users_df.withColumnRenamed)
'''
Help on method withColumnRenamed in module pyspark.sql.dataframe:

withColumnRenamed(existing, new) method of pyspark.sql.dataframe.DataFrame instance
    Returns a new :class:`DataFrame` by renaming an existing column.
    This is a no-op if schema doesn't contain the given column name.
    
    .. versionadded:: 1.3.0
    
    Parameters
    ----------
    existing : str
        string, name of the existing column to rename.
    new : str
        string, new name of the column.
    
    Examples
    --------
    >>> df.withColumnRenamed('age', 'age2').collect()
    [Row(age2=2, name='Alice'), Row(age2=5, name='Bob')]
'''

'''

    Rename id to user_id
    Rename first_name to user_first_name
    Rename last_name to user_last_name

'''

users_df. \
    select('id', 'first_name', 'last_name'). \
    withColumnRenamed('id', 'user_id'). \
    withColumnRenamed('first_name', 'user_first_name'). \
    withColumnRenamed('last_name', 'user_last_name'). \
    show()

#withColumn column is added at the end, withColumnRenamed same order is preserved after renaming

#13 Renaming Spark Data Frame columns or expressions using alias
# Using select
users_df. \
    select(
        col('id').alias('user_id'),
        col('first_name').alias('user_first_name'),
        col('last_name').alias('user_last_name'),
        concat(col('first_name'), lit(', '), col('last_name')).alias('user_full_name')
    ). \
    show()

users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        concat(users_df['first_name'], lit(', '), users_df['last_name']).alias('user_full_name')
    ). \
    show()

# Using withColumn and alias (first select and then withColumn)
users_df. \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name')
    ). \
    withColumn('user_full_name', concat(col('user_first_name'), lit(', '), col('user_last_name'))). \
    show()

# Using withColumn and alias (first withColumn and then select)
users_df. \
    withColumn('user_full_name', concat(col('first_name'), lit(', '), col('last_name'))). \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ). \
    show()

users_df. \
    withColumn('user_full_name', concat(users_df['first_name'], lit(', '), users_df['last_name'])). \
    select(
        users_df['id'].alias('user_id'),
        users_df['first_name'].alias('user_first_name'),
        users_df['last_name'].alias('user_last_name'),
        'user_full_name'
    ). \
    show()

#14 Renaming and Reordering multiple Spark Data Frame Columns

# required columns from original list
required_columns = ['id', 'first_name', 'last_name', 'email', 'phone_numbers', 'courses']

# new column name list
target_column_names = ['user_id', 'user_first_name', 'user_last_name', 'user_email', 'user_phone_numbers', 'enrolled_courses']

users_df. \
    select(required_columns). \
    toDF(*target_column_names). \
    show()

#Sect5 Manipulate cols

#02 Predefined Functions
#We typically process data in the columns using functions in pyspark.sql.functions
#in terminal
pip3 install databricks-cli
databricks configure -h

databricks configure --token

'''
Let us copy the required data sets used for this course by using databricks fs.

    Make sure Databricks CLI is installed and configured with token.
    Run this command to validate you have access to the environment you are working with - databricks fs ls
    Create a folder by name dbfs:/public using databricks fs mkdirs dbfs:/public.

Here are the instructions to setup retail_db data set under dbfs:/public.

    Clone the GitHub repository by using git clone https//github.com/dgadiraju/retail_db.git.
    Make sure to get into the folder and remove .git folder by using rm -rf .git. If you are using Windows you can use File Explorer to delete the folder.
    Run the below command to copy the retail_db folder into dbfs:/public. Make sure to provide fully qualified path.

databricks fs cp /Users/itversity/retail_db dbfs:/public/retail_db --recursive

    Use the below command to validate. It should return details related to 6 folders and files.

databricks fs ls dbfs:/public/retail_db

Follow the similar instructions to setup retail_db_json data set under dbfs:/public using this git repo - https//github.com/itversity/retail_db_json.git

'''

# Reading data
orders = spark.read.csv(
    '/FileStore/tables/orders',
    schema='order_id INT, order_date STRING, order_customer_id INT, order_status STRING'
)
orders.show(5,False)
'''
+--------+---------------------+-----------------+---------------+
|order_id|order_date           |order_customer_id|order_status   |
+--------+---------------------+-----------------+---------------+
|1       |2013-07-25 00:00:00.0|11599            |CLOSED         |
|2       |2013-07-25 00:00:00.0|256              |PENDING_PAYMENT|
|3       |2013-07-25 00:00:00.0|12111            |COMPLETE       |
|4       |2013-07-25 00:00:00.0|8827             |CLOSED         |
|5       |2013-07-25 00:00:00.0|11318            |COMPLETE       |
+--------+---------------------+-----------------+---------------+
'''

from pyspark.sql.functions import date_format

# Function as part of projections
orders.select('*', date_format('order_date', 'yyyyMM').alias('order_month')).show()

orders.withColumn('order_month', date_format('order_date', 'yyyyMM')).show()

# Function as part of where or filter

orders. \
    filter(date_format('order_date', 'yyyyMM') == 201401). \
    show()

# Function as part of groupBy

orders. \
    groupBy(date_format('order_date', 'yyyyMM').alias('order_month')). \
    count(). \
    show()
'''
+-----------+-----+
|order_month|count|
+-----------+-----+
|     201401| 5908|
|     201405| 5467|
|     201312| 5892|
|     201310| 5335|
|     201311| 6381|
|     201307| 1533|
|     201407| 4468|
|     201403| 5778|
|     201404| 5657|
|     201402| 5635|
|     201309| 5841|
|     201406| 5308|
|     201308| 5680|
+-----------+-----+
'''
df=orders.withColumn('order_month', date_format('order_date', 'yyyyMM'))
df=orders.withColumn('order_month', date_format('order_date', 'yyyyMM'))
from pyspark.sql.functions import col
#df.filter(col('order_month')==201401).show()
df.groupBy(col('order_month')).count().show()


#03 Create Dummy Dataframes
l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")
df.show()

from pyspark.sql.functions import current_date
df.select(current_date()). \
    show()
'''
+-----+
|dummy|
+-----+
|    X|
+-----+

+--------------+
|current_date()|
+--------------+
|    2022-03-18|
+--------------+
'''
df.select(current_date().alias("current_date")). \
    show()
'''
+------------+
|current_date|
+------------+
|  2022-03-18|
+------------+
'''
employees = [
    (1, "Scott", "Tiger", 1000.0, 
      "united states", "+1 123 456 7890", "123 45 6789"
    ),
     (2, "Henry", "Ford", 1250.0, 
      "India", "+91 234 567 8901", "456 78 9123"
     ),
     (3, "Nick", "Junior", 750.0, 
      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
     ),
     (4, "Bill", "Gomes", 1500.0, 
      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
     )
]

employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )
employeesDF.show(truncate=False)
'''
+-----------+----------+---------+------+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|nationality   |phone_number    |ssn        |
+-----------+----------+---------+------+--------------+----------------+-----------+
|1          |Scott     |Tiger    |1000.0|united states |+1 123 456 7890 |123 45 6789|
|2          |Henry     |Ford     |1250.0|India         |+91 234 567 8901|456 78 9123|
|3          |Nick      |Junior   |750.0 |united KINGDOM|+44 111 111 1111|222 33 4444|
|4          |Bill      |Gomes    |1500.0|AUSTRALIA     |+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+--------------+----------------+-----------+
'''
from pyspark.sql.functions import col, upper
employeesDF. \
    select(upper("first_name"), upper("last_name")). \
    show()
employeesDF. \
    select(upper(col("first_name")), upper(col("last_name"))). \
    show()
employeesDF. \
    groupBy(upper(col("nationality"))). \
    count(). \
    show()

#Also, if we want to use functions such as alias, desc etc on columns then we have to pass the column names as column type (not as strings).

# We can invoke desc on columns which are of type column
employeesDF. \
    orderBy(col("employee_id").desc()). \
    show()

# This will fail as the function desc is available only on column type.
employeesDF. \
    orderBy("employee_id".desc()). \
    show()
employeesDF. \
    orderBy(col("first_name").desc()). \
    show()
# Alternative - we can also refer column names using Data Frame like this
employeesDF. \
    orderBy(upper(employeesDF['first_name']).alias('first_name')). \
    show()
# Alternative - we can also refer column names using Data Frame like this
employeesDF. \
    orderBy(upper(employeesDF.first_name).alias('first_name')). \
    show()
#Sometimes, we want to add a literal to the column values. For example, we might want to concatenate first_name and last_name separated by comma and space in between.
from pyspark.sql.functions import concat

#below will throw error AnalysisException: cannot resolve '`, `' given input columns: [employee_id, first_name, last_name, nationality, phone_number, salary, ssn];
employeesDF. \
    select(concat(col("first_name"), ", ", col("last_name"))). \
    show()

# Referring columns using Data Frame also throw same err as above
employeesDF. \
    select(concat(employeesDF["first_name"], ", ", employeesDF["last_name"])). \
    show()

from pyspark.sql.functions import concat, col, lit

employeesDF. \
    select(concat(col("first_name"), 
                  lit(", "), 
                  col("last_name")
                 ).alias("full_name")
          ). \
    show(truncate=False)

#04 Categories Of Functions


'''

    String Manipulation Functions
        Case Conversion - lower, upper
        Getting Length - length
        Extracting substrings - substring, split
        Trimming - trim, ltrim, rtrim
        Padding - lpad, rpad
        Concatenating string - concat, concat_ws
    Date Manipulation Functions
        Getting current date and time - current_date, current_timestamp
        Date Arithmetic - date_add, date_sub, datediff, months_between, add_months, next_day
        Beginning and Ending Date or Time - last_day, trunc, date_trunc
        Formatting Date - date_format
        Extracting Information - dayofyear, dayofmonth, dayofweek, year, month
    Aggregate Functions
        count, countDistinct
        sum, avg
        min, max
    Other Functions - We will explore depending on the use cases.
        CASE and WHEN
        CAST for type casting
        Functions to manage special types such as ARRAY, MAP, STRUCT type columns
        Many others


'''
#05 Getting Help on Spark Functions
from pyspark.sql.functions import date_format, col, lit, concat, concat_ws
help(date_format)
'''

date_format(date, format)
    Converts a date/timestamp/string to a value of string in the format specified by the date
    format given by the second argument.
    
    A pattern could be for instance `dd.MM.yyyy` and could return a string like '18.03.1993'. All
    pattern letters of `datetime pattern`_. can be used.
    
    .. _datetime pattern: https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html
    
    .. versionadded:: 1.5.0
    
    Notes
    -----
    Whenever possible, use specialized functions like `year`.
    
    Examples
    --------
    >>> df = spark.createDataFrame([('2015-04-08',)], ['dt'])
    >>> df.select(date_format('dt', 'MM/dd/yyy').alias('date')).collect()
    [Row(date='04/08/2015')]

'''

help(concat)
'''
concat(*cols)
    Concatenates multiple input columns together into a single column.
    The function works with strings, binary and compatible array columns.
    
    .. versionadded:: 1.5.0
    
    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat(df.s, df.d).alias('s')).collect()
    [Row(s='abcd123')]
    
    >>> df = spark.createDataFrame([([1, 2], [3, 4], [5]), ([1, 2], None, [3])], ['a', 'b', 'c'])
    >>> df.select(concat(df.a, df.b, df.c).alias("arr")).collect()
    [Row(arr=[1, 2, 3, 4, 5]), Row(arr=None)]
'''

help(concat_ws)
'''
concat_ws(sep, *cols)
    Concatenates multiple input string columns together into a single string column,
    using the given separator.
    
    .. versionadded:: 1.5.0
    
    Examples
    --------
    >>> df = spark.createDataFrame([('abcd','123')], ['s', 'd'])
    >>> df.select(concat_ws('-', df.s, df.d).alias('s')).collect()
    [Row(s='abcd-123')]
'''


#07 Common String Manipulation Functions

from pyspark.sql.functions import concat
employeesDF. \
    withColumn("full_name", concat("first_name", "last_name")). \
    show()
from pyspark.sql.functions import concat, lit
employeesDF. \
    withColumn("full_name", concat("first_name", lit(", "), "last_name")). \
    show()


employeesDF = spark.createDataFrame(employees). \
    toDF("employee_id", "first_name",
         "last_name", "salary",
         "nationality", "phone_number",
         "ssn"
        )
from pyspark.sql.functions import col, lower, upper, initcap, length
employeesDF. \
  select("employee_id", "nationality"). \
  withColumn("nationality_upper", upper(col("nationality"))). \
  withColumn("nationality_lower", lower(col("nationality"))). \
  withColumn("nationality_initcap", initcap(col("nationality"))). \
  withColumn("nationality_length", length(col("nationality"))). \
  show()

#08 Extracting Strings using substring
l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")

#We can use substring function to extract substring from main string using Pyspark.
from pyspark.sql.functions import substring, lit

# Function takes 3 arguments
# First argument is a column from which we want to extract substring.
# Second argument is the character from which string is supposed to be extracted.
# Third argument is number of characters from the first argument.
df.select(substring(lit("Hello World"), 7, 5)). \
  show()
'''
+----------------------------+
|substring(Hello World, 7, 5)|
+----------------------------+
|                       World|
+----------------------------+
'''
df.select(substring(lit("Hello World"), -5, 5)). \
  show()
'''
+----------------------------+
|substring(Hello World, 7, 5)|
+----------------------------+
|                       World|
+----------------------------+
'''

employees = [(1, "Scott", "Tiger", 1000.0, 
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, 
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, 
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )
'''
+-----------+----------+---------+------+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|nationality   |phone_number    |ssn        |
+-----------+----------+---------+------+--------------+----------------+-----------+
|1          |Scott     |Tiger    |1000.0|united states |+1 123 456 7890 |123 45 6789|
|2          |Henry     |Ford     |1250.0|India         |+91 234 567 8901|456 78 9123|
|3          |Nick      |Junior   |750.0 |united KINGDOM|+44 111 111 1111|222 33 4444|
|4          |Bill      |Gomes    |1500.0|AUSTRALIA     |+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+--------------+----------------+-----------+
'''

from pyspark.sql.functions import substring, col
employeesDF. \
    select("employee_id", "phone_number", "ssn"). \
    withColumn("phone_last4", substring(col("phone_number"), -4, 4).cast("int")). \
    withColumn("ssn_last4", substring(col("ssn"), 8, 4).cast("int")). \
    show()
'''
+-----------+----------------+-----------+-----------+---------+
|employee_id|    phone_number|        ssn|phone_last4|ssn_last4|
+-----------+----------------+-----------+-----------+---------+
|          1| +1 123 456 7890|123 45 6789|       7890|     6789|
|          2|+91 234 567 8901|456 78 9123|       8901|     9123|
|          3|+44 111 111 1111|222 33 4444|       1111|     4444|
|          4|+61 987 654 3210|789 12 6118|       3210|     6118|
+-----------+----------------+-----------+-----------+---------+

'''
#09 Extracting Strings using split,If we are processing variable length columns with delimiter then we use split to extract the information.
'''
split takes 2 arguments, column and delimiter.
split convert each string into array and we can access the elements using index.
We can also use explode in conjunction with split to explode the list or array into records in Data Frame. It can be used in cases such as word count, phone count etc.
'''
l = [('X', )]
df = spark.createDataFrame(l, "dummy STRING")
from pyspark.sql.functions import split, explode, lit
df.select(split(lit("Hello World, how are you"), " ")). \
    show(truncate=False)
'''
+--------------------------------------+
|split(Hello World, how are you,  , -1)|
+--------------------------------------+
|[Hello, World,, how, are, you]        |
+-
'''
df.select(split(lit("Hello World, how are you"), " ")[2]). \
    show(truncate=False)
'''
+-----------------------------------------+
|split(Hello World, how are you,  , -1)[2]|
+-----------------------------------------+
|how                                      |
+-----------------------------------------+

'''
df.select(explode(split(lit("Hello World, how are you"), " ")).alias('word')). \
    show(truncate=False)

'''
+------+
|word  |
+------+
|Hello |
|World,|
|how   |
|are   |
|you   |
+------+

'''

from pyspark.sql.functions import split, explode
employeesDF = employeesDF. \
    select('employee_id', 'phone_numbers', 'ssn'). \
    withColumn('phone_number', explode(split('phone_numbers', ',')))

'''
+-----------+---------------------------------+-----------+----------------+
|employee_id|phone_numbers                    |ssn        |phone_number    |
+-----------+---------------------------------+-----------+----------------+
|1          |+1 123 456 7890,+1 234 567 8901  |123 45 6789|+1 123 456 7890 |
|1          |+1 123 456 7890,+1 234 567 8901  |123 45 6789|+1 234 567 8901 |
|2          |+91 234 567 8901                 |456 78 9123|+91 234 567 8901|
|3          |+44 111 111 1111,+44 222 222 2222|222 33 4444|+44 111 111 1111|
|3          |+44 111 111 1111,+44 222 222 2222|222 33 4444|+44 222 222 2222|
|4          |+61 987 654 3210,+61 876 543 2109|789 12 6118|+61 987 654 3210|
|4          |+61 987 654 3210,+61 876 543 2109|789 12 6118|+61 876 543 2109|
+-----------+---------------------------------+-----------+----------------+

'''
employeesDF. \
    select("employee_id", "phone_number", "ssn"). \
    withColumn("area_code", split("phone_number", " ")[1].cast("int")). \
    withColumn("phone_last4", split("phone_number", " ")[3].cast("int")). \
    withColumn("ssn_last4", split("ssn", " ")[2].cast("int")). \
    show()

'''
+-----------+----------------+-----------+---------+-----------+---------+
|employee_id|    phone_number|        ssn|area_code|phone_last4|ssn_last4|
+-----------+----------------+-----------+---------+-----------+---------+
|          1| +1 123 456 7890|123 45 6789|      123|       7890|     6789|
|          1| +1 234 567 8901|123 45 6789|      234|       8901|     6789|
|          2|+91 234 567 8901|456 78 9123|      234|       8901|     9123|
|          3|+44 111 111 1111|222 33 4444|      111|       1111|     4444|
|          3|+44 222 222 2222|222 33 4444|      222|       2222|     4444|
|          4|+61 987 654 3210|789 12 6118|      987|       3210|     6118|
|          4|+61 876 543 2109|789 12 6118|      876|       2109|     6118|
+-----------+----------------+-----------+---------+-----------+---------+

'''

employeesDF. \
    groupBy('employee_id'). \
    count(). \
    show()
'''
+-----------+-----+
|employee_id|count|
+-----------+-----+
|          1|    2|
|          2|    1|
|          3|    2|
|          4|    2|
+-----------+-----+
'''

#10 Padding Characters around strings

l = [('X',)]
df = spark.createDataFrame(l).toDF("dummy")
from pyspark.sql.functions import lit, lpad
df.select(lpad(lit("Hello"), 10, "-").alias("dummy")).show()
'''
+----------+
|     dummy|
+----------+
|-----Hello|
+----------+

'''
employeesDF = spark.createDataFrame(employees). \
    toDF("employee_id", "first_name",
         "last_name", "salary",
         "nationality", "phone_number",
         "ssn"
        ).show()
'''
+-----------+----------+---------+------+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+--------------+----------------+-----------+

'''
from pyspark.sql.functions import lpad, rpad, concat
empFixedDF = employeesDF.select(
    concat(
        lpad("employee_id", 5, "0"), 
        rpad("first_name", 10, "-"), 
        rpad("last_name", 10, "-"),
        lpad("salary", 10, "0"), 
        rpad("nationality", 15, "-"), 
        rpad("phone_number", 17, "-"), 
        "ssn"
    ).alias("employee")
)
'''
+------------------------------------------------------------------------------+
|employee                                                                      |
+------------------------------------------------------------------------------+
|00001Scott-----Tiger-----00001000.0united states--+1 123 456 7890--123 45 6789|
|00002Henry-----Ford------00001250.0India----------+91 234 567 8901-456 78 9123|
|00003Nick------Junior----00000750.0united KINGDOM-+44 111 111 1111-222 33 4444|
|00004Bill------Gomes-----00001500.0AUSTRALIA------+61 987 654 3210-789 12 6118|
+------------------------------------------------------------------------------+

'''

l = [("   Hello.    ",) ]
df = spark.createDataFrame(l).toDF("dummy")
from pyspark.sql.functions import col, ltrim, rtrim, trim
df.withColumn("ltrim", ltrim(col("dummy"))). \
  withColumn("rtrim", rtrim(col("dummy"))). \
  withColumn("trim", trim(col("dummy"))). \
  show()
'''
+-------------+----------+---------+------+
|        dummy|     ltrim|    rtrim|  trim|
+-------------+----------+---------+------+
|   Hello.    |Hello.    |   Hello.|Hello.|
+-------------+----------+---------+------+
'''

from pyspark.sql.functions import expr
spark.sql('DESCRIBE FUNCTION rtrim').show(truncate=False)
'''
+-----------------------------------------------------------------------------+
|function_desc                                                                |
+-----------------------------------------------------------------------------+
|Function: rtrim                                                              |
|Class: org.apache.spark.sql.catalyst.expressions.StringTrimRight             |
|Usage: 
    rtrim(str) - Removes the trailing space characters from `str`.
  |
+-----------------------------------------------------------------------------+

'''
# if we do not specify trimStr, it will be defaulted to space, trim removes bot leading and trailing spaces
df.withColumn("ltrim", expr("ltrim(dummy)")). \
  withColumn("rtrim", expr("rtrim('.', rtrim(dummy))")). \
  withColumn("trim", trim(col("dummy"))). \
  show()
'''
+-------------+----------+--------+------+
|        dummy|     ltrim|   rtrim|  trim|
+-------------+----------+--------+------+
|   Hello.    |Hello.    |   Hello|Hello.|
+-------------+----------+--------+------+

'''

df.withColumn("ltrim", expr("trim(LEADING ' ' FROM dummy)")). \
  withColumn("rtrim", expr("trim(TRAILING '.' FROM rtrim(dummy))")). \
  withColumn("trim", expr("trim(BOTH ' ' FROM dummy)")). \
  show()

'''
+-------------+----------+--------+------+
|        dummy|     ltrim|   rtrim|  trim|
+-------------+----------+--------+------+
|   Hello.    |Hello.    |   Hello|Hello.|
+-------------+----------+--------+------+

'''




#12 Date and Time Manipulation Functions

'''
We can use current_date to get today’s server date.Date will be returned using yyyy-MM-dd format.
We can use current_timestamp to get current server time.Timestamp will be returned using yyyy-MM-dd HH:mm:ss:SSS format.
Hours will be by default in 24 hour format.
'''
l = [("X", )]
df = spark.createDataFrame(l).toDF("dummy")
from pyspark.sql.functions import current_date, current_timestamp
help(current_timestamp)
df.select(current_date()).show() #yyyy-MM-dd
df.select(current_timestamp()).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS
'''
+--------------+
|current_date()|
+--------------+
|    2022-01-02|
+--------------+
+----------------------+
|current_timestamp()   |
+----------------------+
|2022-01-02 21:47:23.32|
+----------------------+
We can convert a string which contain date or timestamp in non-standard format to standard date or time using to_date or to_timestamp function respectively.
'''
from pyspark.sql.functions import lit, to_date, to_timestamp
help(to_date) # to_date(col,format) takes col and string as params to convert col to date

df.select(to_date(lit('20210228'), 'yyyyMMdd').alias('to_date')).show()
+----------+
|   to_date|
+----------+
|2021-02-28|
+----------+

df.select(to_timestamp(lit('20210228 1725'), 'yyyyMMdd HHmm').alias('to_timestamp')).show()
+-------------------+
|       to_timestamp|
+-------------------+
|2021-02-28 17:25:00|
+-------------------+

#13 Date and Time Arithmetic

'''
Adding days to a date or timestamp - date_add
Subtracting days from a date or timestamp - date_sub
Getting difference between 2 dates or timestamps - datediff
Getting the number of months between 2 dates or timestamps - months_between
Adding months to a date or timestamp - add_months
Getting next day from a given date - next_day
We can apply these on standard date or timestamp. All the functions return date even when applied on timestamp field.

'''

datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")
datetimesDF.show(truncate=False)

'''
+----------+-----------------------+
|date      |time                   |
+----------+-----------------------+
|2014-02-28|2014-02-28 10:00:00.123|
|2016-02-29|2016-02-29 08:08:08.999|
|2017-10-31|2017-12-31 11:59:59.123|
|2019-11-30|2019-08-31 00:00:00.000|
+----------+-----------------------+

date_add(start, days) ->Returns the date that is `days` days after `start`
    
help(date_add)
Add 10 days to both date and time values.
Subtract 10 days from both date and time values.


'''
datetimesDF. \
    withColumn("date_add_date", date_add("date", 10)). \
    withColumn("date_add_time", date_add("time", 10)). \
    withColumn("date_sub_date", date_sub("date", 10)). \
    withColumn("date_sub_time", date_sub("time", 10)). \
    show()
'''
+----------+--------------------+-------------+-------------+-------------+-------------+
|      date|                time|date_add_date|date_add_time|date_sub_date|date_sub_time|
+----------+--------------------+-------------+-------------+-------------+-------------+
|2014-02-28|2014-02-28 10:00:...|   2014-03-10|   2014-03-10|   2014-02-18|   2014-02-18|
|2016-02-29|2016-02-29 08:08:...|   2016-03-10|   2016-03-10|   2016-02-19|   2016-02-19|
|2017-10-31|2017-12-31 11:59:...|   2017-11-10|   2018-01-10|   2017-10-21|   2017-12-21|
|2019-11-30|2019-08-31 00:00:...|   2019-12-10|   2019-09-10|   2019-11-20|   2019-08-21|
+----------+--------------------+-------------+-------------+-------------+-------------+

'''
#Get the difference between current_date and date values as well as current_timestamp and time values.
#datediff(end,start) -> returns no of days from start to end
from pyspark.sql.functions import current_date, current_timestamp, datediff

datetimesDF. \
    withColumn("datediff_date", datediff(current_date(), "date")). \
    withColumn("datediff_time", datediff(current_timestamp(), "time")). \
    show()
'''
+----------+--------------------+-------------+-------------+
|      date|                time|datediff_date|datediff_time|
+----------+--------------------+-------------+-------------+
|2014-02-28|2014-02-28 10:00:...|         2865|         2865|
|2016-02-29|2016-02-29 08:08:...|         2134|         2134|
|2017-10-31|2017-12-31 11:59:...|         1524|         1463|
|2019-11-30|2019-08-31 00:00:...|          764|          855|
+----------+--------------------+-------------+-------------+


Get the number of months between current_date and date values as well as current_timestamp and time values.
Add 3 months to both date values as well as time values.
help(months_between)-> months_between(date1,date2,roundOff=True) roundOff default 8 decimal places

'''
from pyspark.sql.functions import months_between, add_months, round
datetimesDF. \
    withColumn("months_between_date", round(months_between(current_date(), "date"), 2)). \
    withColumn("months_between_time", round(months_between(current_timestamp(), "time"), 2)). \
    withColumn("add_months_date", add_months("date", 3)). \
    withColumn("add_months_time", add_months("time", 3)). \
    show(truncate=False)

#14 Using date and time trunc functions
'''
In Data Warehousing we quite often run to date reports such as 
week to date, month to date, year to date etc. Let us understand how we can take care of such requirements using appropriate functions over Spark Data Frames.

We can use trunc or date_trunc for the same to get the beginning date of the week, month, current year etc by passing date or timestamp to it.
We can use trunc to get beginning date of the month or year by passing date or timestamp to it 
- for example trunc(current_date(), "MM") will give the first of the current month.
We can use date_trunc to get beginning date of the month or year as well as beginning time of the day or hour by passing timestamp to it.
    Get beginning date based on month - date_trunc("MM", current_timestamp())
    Get beginning time based on day - date_trunc("DAY", current_timestamp())
'''

from pyspark.sql.functions import trunc, date_trunc

#Get beginning month date using date field and beginning year date using time field.
#trunc returns date
from pyspark.sql.functions import trunc
datetimesDF. \
    withColumn("date_trunc", trunc("date", "MM")). \
    withColumn("time_trunc", trunc("time", "yy")). \
    show(truncate=False)
'''
+----------+-----------------------+----------+----------+
|date      |time                   |date_trunc|time_trunc|
+----------+-----------------------+----------+----------+
|2014-02-28|2014-02-28 10:00:00.123|2014-02-01|2014-01-01|
|2016-02-29|2016-02-29 08:08:08.999|2016-02-01|2016-01-01|
|2017-10-31|2017-12-31 11:59:59.123|2017-10-01|2017-01-01|
|2019-11-30|2019-08-31 00:00:00.000|2019-11-01|2019-01-01|
+----------+-----------------------+----------+----------+
'''
#Get beginning hour time using date and time field.
from pyspark.sql.functions import date_trunc
#date_trunc returns timestamp
datetimesDF. \
    withColumn("date_trunc", date_trunc('MM', "date")). \
    withColumn("time_trunc", date_trunc('yy', "time")). \
    show(truncate=False)
'''
+----------+-----------------------+-------------------+-------------------+
|date      |time                   |date_trunc         |time_trunc         |
+----------+-----------------------+-------------------+-------------------+
|2014-02-28|2014-02-28 10:00:00.123|2014-02-01 00:00:00|2014-01-01 00:00:00|
|2016-02-29|2016-02-29 08:08:08.999|2016-02-01 00:00:00|2016-01-01 00:00:00|
|2017-10-31|2017-12-31 11:59:59.123|2017-10-01 00:00:00|2017-01-01 00:00:00|
|2019-11-30|2019-08-31 00:00:00.000|2019-11-01 00:00:00|2019-01-01 00:00:00|
+----------+-----------------------+-------------------+-------------------+

'''
datetimesDF. \
    withColumn("date_dt", date_trunc("HOUR", "date")). \
    withColumn("time_dt", date_trunc("HOUR", "time")). \
    withColumn("time_dt1", date_trunc("dd", "time")). \
    show(truncate=False)
'''
+----------+-----------------------+-------------------+-------------------+-------------------+
|date      |time                   |date_dt            |time_dt            |time_dt1           |
+----------+-----------------------+-------------------+-------------------+-------------------+
|2014-02-28|2014-02-28 10:00:00.123|2014-02-28 00:00:00|2014-02-28 10:00:00|2014-02-28 00:00:00|
|2016-02-29|2016-02-29 08:08:08.999|2016-02-29 00:00:00|2016-02-29 08:00:00|2016-02-29 00:00:00|
|2017-10-31|2017-12-31 11:59:59.123|2017-10-31 00:00:00|2017-12-31 11:00:00|2017-12-31 00:00:00|
|2019-11-30|2019-08-31 00:00:00.000|2019-11-30 00:00:00|2019-08-31 00:00:00|2019-08-31 00:00:00|
+----------+-----------------------+-------------------+-------------------+-------------------+

'''

#15 Date and Time Extract Functions
'''

year
month
weekofyear
dayofyear
dayofmonth
dayofweek
hour
minute
second

help(dayofweek)
'''
l = [("X", )]
df = spark.createDataFrame(l).toDF("dummy")
from pyspark.sql.functions import year, month, weekofyear, dayofmonth, \
    dayofyear, dayofweek, current_date
df.select(
    current_date().alias('current_date'), 
    year(current_date()).alias('year'),
    month(current_date()).alias('month'),
    weekofyear(current_date()).alias('weekofyear'),
    dayofyear(current_date()).alias('dayofyear'),
    dayofmonth(current_date()).alias('dayofmonth'),
    dayofweek(current_date()).alias('dayofweek')
).show() #yyyy-MM-dd
'''
+------------+----+-----+----------+---------+----------+---------+
|current_date|year|month|weekofyear|dayofyear|dayofmonth|dayofweek|
+------------+----+-----+----------+---------+----------+---------+
|  2022-01-02|2022|    1|        52|        2|         2|        1|
+------------+----+-----+----------+---------+----------+---------+
'''
from pyspark.sql.functions import current_timestamp, hour, minute, second
df.select(
    current_timestamp().alias('current_timestamp'), 
    year(current_timestamp()).alias('year'),
    month(current_timestamp()).alias('month'),
    dayofmonth(current_timestamp()).alias('dayofmonth'),
    hour(current_timestamp()).alias('hour'),
    minute(current_timestamp()).alias('minute'),
    second(current_timestamp()).alias('second')
).show(truncate=False) #yyyy-MM-dd HH:mm:ss.SSS

'''
+-----------------------+----+-----+----------+----+------+------+
|current_timestamp      |year|month|dayofmonth|hour|minute|second|
+-----------------------+----+-----+----------+----+------+------+
|2022-01-02 21:49:07.472|2022|1    |2         |21  |49    |7     |
+-----------------------+----+-----+----------+----+------+------+
'''

#16 Using to_date and to_timestamp
#to convert non standard dates and timestamps to standard dates and timestamps.
'''

yyyy-MM-dd is the standard date format
yyyy-MM-dd HH:mm:ss.SSS is the standard timestamp format
Most of the date manipulation functions expect date and time using standard format. However, we might not have data in the expected standard format.
In those scenarios we can use to_date and to_timestamp to convert non standard dates and timestamps to standard ones respectively.

'''

from pyspark.sql.functions import lit, to_date
l = [("X", )]
df = spark.createDataFrame(l).toDF("dummy")
df.select(to_date(lit('20210302'), 'yyyyMMdd').alias('to_date')).show()
'''
+----------+
|   to_date|
+----------+
|2021-03-02|
+----------+
'''
# year and day of year to standard date
df.select(to_date(lit('2021061'), 'yyyyDDD').alias('to_date')).show()
'''
+----------+
|   to_date|
+----------+
|2021-03-02|
+----------+
'''


df.select(to_date(lit('02/03/2021'),   'dd/MM/yyyy').alias('to_date')).show()
df.select(to_date(lit('02-03-2021'),   'dd-MM-yyyy').alias('to_date')).show()
df.select(to_date(lit('02-Mar-2021'),  'dd-MMM-yyyy').alias('to_date')).show()
df.select(to_date(lit('02-March-2021'),'dd-MMMM-yyyy').alias('to_date')).show()
df.select(to_date(lit('March 2, 2021'),'MMMM d, yyyy').alias('to_date')).show()
'''
+----------+
|   to_date|
+----------+
|2021-03-02|
+----------+
'''

from pyspark.sql.functions import to_timestamp
df.select(to_timestamp(lit('02-Mar-2021'), 'dd-MMM-yyyy').alias('to_date')).show()
df.select(to_timestamp(lit('02-Mar-2021 17:30:15'), 'dd-MMM-yyyy HH:mm:ss').alias('to_date')).show()
'''
+-------------------+
|            to_date|
+-------------------+
|2021-03-02 00:00:00|
+-------------------+
'''

datetimes = [(20140228, "28-Feb-2014 10:00:00.123"),
                     (20160229, "20-Feb-2016 08:08:08.999"),
                     (20171031, "31-Dec-2017 11:59:59.123"),
                     (20191130, "31-Aug-2019 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date BIGINT, time STRING")
datetimesDF.printSchema()
'''
+--------+------------------------+
|date    |time                    |
+--------+------------------------+
|20140228|28-Feb-2014 10:00:00.123|
|20160229|20-Feb-2016 08:08:08.999|
|20171031|31-Dec-2017 11:59:59.123|
|20191130|31-Aug-2019 00:00:00.000|
+--------+------------------------+
root
 |-- date: long (nullable = true)
 |-- time: string (nullable = true)

Let us convert data in datetimesDF to standard dates or timestamps

'''
from pyspark.sql.functions import col, to_date, to_timestamp
datetimesDF. \
    withColumn('to_date', to_date(col('date').cast('string'), 'yyyyMMdd')). \
    withColumn('to_timestamp', to_timestamp(col('time'), 'dd-MMM-yyyy HH:mm:ss.SSS')). \
    show(truncate=False)
'''
+--------+------------------------+----------+-----------------------+
|date    |time                    |to_date   |to_timestamp           |
+--------+------------------------+----------+-----------------------+
|20140228|28-Feb-2014 10:00:00.123|2014-02-28|2014-02-28 10:00:00.123|
|20160229|20-Feb-2016 08:08:08.999|2016-02-29|2016-02-20 08:08:08.999|
|20171031|31-Dec-2017 11:59:59.123|2017-10-31|2017-12-31 11:59:59.123|
|20191130|31-Aug-2019 00:00:00.000|2019-11-30|2019-08-31 00:00:00    |
+--------+------------------------+----------+-----------------------+
'''

#17 Using date_format Function
'''
how to extract information from dates or times using date_format function.

We can use date_format to extract the required information in a desired format from standard date or timestamp. Earlier we have explored to_date and to_timestamp to convert non standard date or timestamp to standard ones respectively.
There are also specific functions to extract year, month, day with in a week, a day with in a month, day with in a year etc. These are covered as part of earlier topics in this section or module.
'''
datetimes = [("2014-02-28", "2014-02-28 10:00:00.123"),
                     ("2016-02-29", "2016-02-29 08:08:08.999"),
                     ("2017-10-31", "2017-12-31 11:59:59.123"),
                     ("2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes, schema="date STRING, time STRING")

from pyspark.sql.functions import date_format
#Get the year and month from both date and time columns using yyyyMM format. Also make sure that the data type is converted to integer.
datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    show(truncate=False)

# yyyy
# MM
# dd
# DD julian day  within year 65th day as 065 or 240th day as 240
# HH 24HR
# hh 12hr
# mm min
# ss sec
# SSS millisecs
'''
+----------+-----------------------+-------+-------+
|date      |time                   |date_ym|time_ym|
+----------+-----------------------+-------+-------+
|2014-02-28|2014-02-28 10:00:00.123|201402 |201402 |
|2016-02-29|2016-02-29 08:08:08.999|201602 |201602 |
|2017-10-31|2017-12-31 11:59:59.123|201710 |201712 |
|2019-11-30|2019-08-31 00:00:00.000|201911 |201908 |
+----------+-----------------------+-------+-------+
'''
datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM")). \
    withColumn("time_ym", date_format("time", "yyyyMM")). \
    printSchema()
datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    printSchema()
'''
root
 |-- date: string (nullable = true)
 |-- time: string (nullable = true)
 |-- date_ym: string (nullable = true)
 |-- time_ym: string (nullable = true)
 root
 |-- date: string (nullable = true)
 |-- time: string (nullable = true)
 |-- date_ym: integer (nullable = true)
 |-- time_ym: integer (nullable = true)


'''
datetimesDF. \
    withColumn("date_ym", date_format("date", "yyyyMM").cast('int')). \
    withColumn("time_ym", date_format("time", "yyyyMM").cast('int')). \
    show(truncate=False)

'''
+----------+-----------------------+-------+-------+
|date      |time                   |date_ym|time_ym|
+----------+-----------------------+-------+-------+
|2014-02-28|2014-02-28 10:00:00.123|201402 |201402 |
|2016-02-29|2016-02-29 08:08:08.999|201602 |201602 |
|2017-10-31|2017-12-31 11:59:59.123|201710 |201712 |
|2019-11-30|2019-08-31 00:00:00.000|201911 |201908 |
+----------+-----------------------+-------+-------+

Get the information from time in yyyyMMddHHmmss format.
'''
datetimesDF. \
    withColumn("date_dt", date_format("date", "yyyyMMddHHmmss").cast('long')). \
    withColumn("date_ts", date_format("time", "yyyyMMddHHmmss").cast('long')). \
    show(truncate=False)
'''
+----------+-----------------------+--------------+--------------+
|date      |time                   |date_dt       |date_ts       |
+----------+-----------------------+--------------+--------------+
|2014-02-28|2014-02-28 10:00:00.123|20140228000000|20140228100000|
|2016-02-29|2016-02-29 08:08:08.999|20160229000000|20160229080808|
|2017-10-31|2017-12-31 11:59:59.123|20171031000000|20171231115959|
|2019-11-30|2019-08-31 00:00:00.000|20191130000000|20190831000000|
+----------+-----------------------+--------------+--------------+

Get year and day of year using yyyyDDD format.
'''
datetimesDF. \
    withColumn("date_yd", date_format("date", "yyyyDDD").cast('int')). \
    withColumn("time_yd", date_format("time", "yyyyDDD").cast('int')). \
    show(truncate=False)
'''
+----------+-----------------------+-------+-------+
|date      |time                   |date_yd|time_yd|
+----------+-----------------------+-------+-------+
|2014-02-28|2014-02-28 10:00:00.123|2014059|2014059|
|2016-02-29|2016-02-29 08:08:08.999|2016060|2016060|
|2017-10-31|2017-12-31 11:59:59.123|2017304|2017365|
|2019-11-30|2019-08-31 00:00:00.000|2019334|2019243|
+----------+-----------------------+-------+-------+

Get complete description of the date.
'''
datetimesDF. \
    withColumn("date_desc", date_format("date", "MMMM d, yyyy")). \
    show(truncate=False)
'''
+----------+-----------------------+-----------------+
|date      |time                   |date_desc        |
+----------+-----------------------+-----------------+
|2014-02-28|2014-02-28 10:00:00.123|February 28, 2014|
|2016-02-29|2016-02-29 08:08:08.999|February 29, 2016|
|2017-10-31|2017-12-31 11:59:59.123|October 31, 2017 |
|2019-11-30|2019-08-31 00:00:00.000|November 30, 2019|
+----------+-----------------------+-----------------+
Get name of the week day using date.
'''
datetimesDF. \
    withColumn("day_name_abbr", date_format("date", "EE")). \
    show(truncate=False)
datetimesDF. \
    withColumn("day_name_full", date_format("date", "EEEE")). \
    show(truncate=False)
'''
+----------+-----------------------+-------------+
|date      |time                   |day_name_abbr|
+----------+-----------------------+-------------+
|2014-02-28|2014-02-28 10:00:00.123|Fri          |
|2016-02-29|2016-02-29 08:08:08.999|Mon          |
|2017-10-31|2017-12-31 11:59:59.123|Tue          |
|2019-11-30|2019-08-31 00:00:00.000|Sat          |
+----------+-----------------------+-------------+

+----------+-----------------------+-------------+
|date      |time                   |day_name_full|
+----------+-----------------------+-------------+
|2014-02-28|2014-02-28 10:00:00.123|Friday       |
|2016-02-29|2016-02-29 08:08:08.999|Monday       |
|2017-10-31|2017-12-31 11:59:59.123|Tuesday      |
|2019-11-30|2019-08-31 00:00:00.000|Saturday     |
+----------+-----------------------+-------------+
'''

#18 Dealing with unix_timestamp
'''
It is an integer and started from January 1st 1970 Midnight UTC.
Beginning time is also known as epoch and is incremented by 1 every second.
We can convert Unix Timestamp to regular date or timestamp and vice versa.
We can use unix_timestamp to convert regular date or timestamp to a unix timestamp value. For example unix_timestamp(lit("2019-11-19 00:00:00"))
We can use from_unixtime to convert unix timestamp to regular date or timestamp. For example from_unixtime(lit(1574101800))
We can also pass format to both the functions.

'''
datetimes = [(20140228, "2014-02-28", "2014-02-28 10:00:00.123"),
                     (20160229, "2016-02-29", "2016-02-29 08:08:08.999"),
                     (20171031, "2017-10-31", "2017-12-31 11:59:59.123"),
                     (20191130, "2019-11-30", "2019-08-31 00:00:00.000")
                ]
datetimesDF = spark.createDataFrame(datetimes).toDF("dateid", "date", "time")
'''
+--------+----------+-----------------------+
|dateid  |date      |time                   |
+--------+----------+-----------------------+
|20140228|2014-02-28|2014-02-28 10:00:00.123|
|20160229|2016-02-29|2016-02-29 08:08:08.999|
|20171031|2017-10-31|2017-12-31 11:59:59.123|
|20191130|2019-11-30|2019-08-31 00:00:00.000|
+--------+----------+-----------------------+
'''
#Get unix timestamp for dateid, date and time.
from pyspark.sql.functions import unix_timestamp, col

datetimesDF. \
    withColumn("unix_date_id", unix_timestamp(col("dateid").cast("string"), "yyyyMMdd")). \
    withColumn("unix_date", unix_timestamp("date", "yyyy-MM-dd")). \
    withColumn("unix_time", unix_timestamp("time")). \
    show()

'''
help(unix_timestamp)
unix_timestamp(timestamp=None,format='yyyy-MM-dd HH:mm:ss)
convert time string witha given pattern ('yyyy-MM-dd HH:mm:ss default value)to unix time stamp (in seconds),using default timezone and locale,return null if fail
if timestamp is None, it returns current timestamp
'''
#Create a Dataframe by name unixtimesDF with one column unixtime using 4 values. You can use the unix timestamp generated for time column in previous task.
unixtimes = [(1393561800, ),
             (1456713488, ),
             (1514701799, ),
             (1567189800, )
            ]
unixtimesDF = spark.createDataFrame(unixtimes).toDF("unixtime")
unixtimesDF.printSchema()
unixtimesDF.show()
'''
root
 |-- unixtime: long (nullable = true)

+----------+
|  unixtime|
+----------+
|1393561800|
|1456713488|
|1514701799|
|1567189800|
+----------+

'''


#Get date in yyyyMMdd format and also complete timestamp.

from pyspark.sql.functions import from_unixtime
unixtimesDF. \
    withColumn("date", from_unixtime("unixtime", "yyyyMMdd")). \
    withColumn("time", from_unixtime("unixtime")). \
    show()
#yyyyMMdd

'''
+----------+--------+-------------------+
|  unixtime|    date|               time|
+----------+--------+-------------------+
|1393561800|20140228|2014-02-28 04:30:00|
|1456713488|20160229|2016-02-29 02:38:08|
|1514701799|20171231|2017-12-31 06:29:59|
|1567189800|20190830|2019-08-30 18:30:00|
+----------+--------+-------------------+
'''

#19 Dealing with nulls
'''
Let us understand how to deal with nulls using functions that are available in Spark.
We can use coalesce to return first non null value.
We also have traditional SQL style functions such as nvl. However, they can be used either with expr or selectExpr.
'''

employees = [(1, "Scott", "Tiger", 1000.0, 10,
                      "united states", "+1 123 456 7890", "123 45 6789"
                     ),
                     (2, "Henry", "Ford", 1250.0, None,
                      "India", "+91 234 567 8901", "456 78 9123"
                     ),
                     (3, "Nick", "Junior", 750.0, '',
                      "united KINGDOM", "+44 111 111 1111", "222 33 4444"
                     ),
                     (4, "Bill", "Gomes", 1500.0, 10,
                      "AUSTRALIA", "+61 987 654 3210", "789 12 6118"
                     )
                ]
employeesDF = spark. \
    createDataFrame(employees,
                    schema="""employee_id INT, first_name STRING, 
                    last_name STRING, salary FLOAT, bonus STRING, nationality STRING,
                    phone_number STRING, ssn STRING"""
                   )
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+

'''

#convert empty strings to null and then use coalesce, bonus col in the above df
from pyspark.sql.functions import lit,col

employeesDF. \
    withColumn('bonus1', col('bonus').cast('int')). \
    show()
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|bonus1|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|    10|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123|  null|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|  null|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|    10|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
'''

employeesDF. \
    withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
    show()
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|bonus1|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|    10|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123|     0|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|     0|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|    10|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
'''

from pyspark.sql.functions import expr

employeesDF. \
    withColumn('bonus', expr("nvl(bonus, 0)")). \
    show()
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|    0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
'''
employeesDF. \
    withColumn('bonus', expr("nvl(nullif(bonus, ''), 0)")). \
    show()

'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|    0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|    0|united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+

'''

employeesDF. \
    withColumn('payment', col('salary') + (col('salary') * coalesce(col('bonus').cast('int'), lit(0)) / 100)). \
    show()
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+-------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|payment|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+-------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789| 1100.0|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123| 1250.0|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|  750.0|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118| 1650.0|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+-------+

'''
help(employeesDF.na) #->drop,fill,replace functions

#replaces null values, na.fill fillna both are alias
employeesDF.fillna(0.0,'salary').fillna('na','last_name)
             
                                        
#20 Using case and when                                       
'''
CASE and WHEN is typically used to apply transformations based up on conditions. We can use CASE and WHEN similar to SQL using expr or selectExpr.
If we want to use APIs, Spark provides functions such as when and otherwise. when is available as part of pyspark.sql.functions.
On top of column type that is generated using when we should be able to invoke otherwise.
'''
#transform bonus to 0 in case of null or empty, otherwise return the bonus amount.
from pyspark.sql.functions import coalesce, lit, col
employeesDF. \
    withColumn('bonus1', coalesce(col('bonus').cast('int'), lit(0))). \
    show()
                                        
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|bonus1|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|    10|
|          2|     Henry|     Ford|1250.0| null|         India|+91 234 567 8901|456 78 9123|     0|
|          3|      Nick|   Junior| 750.0|     |united KINGDOM|+44 111 111 1111|222 33 4444|     0|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|    10|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+------+

'''
from pyspark.sql.functions import expr
employeesDF. \
    withColumn(
        'bonus', 
        expr("""
            CASE WHEN bonus IS NULL OR bonus = '' THEN 0
            ELSE bonus
            END
            """)
    ). \
    show()
'''
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|bonus|   nationality|    phone_number|        ssn|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
|          1|     Scott|    Tiger|1000.0|   10| united states| +1 123 456 7890|123 45 6789|
|          2|     Henry|     Ford|1250.0|    0|         India|+91 234 567 8901|456 78 9123|
|          3|      Nick|   Junior| 750.0|    0|united KINGDOM|+44 111 111 1111|222 33 4444|
|          4|      Bill|    Gomes|1500.0|   10|     AUSTRALIA|+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+-----+--------------+----------------+-----------+
'''
from pyspark.sql.functions import when
employeesDF. \
    withColumn(
        'bonus',
        when((col('bonus').isNull()) | (col('bonus') == lit('')), 0).otherwise(col('bonus'))
    ). \
    show()

                                        
persons = [
    (1, 1),
    (2, 13),
    (3, 18),
    (4, 60),
    (5, 120),
    (6, 0),
    (7, 12),
    (8, 160)
]
personsDF = spark.createDataFrame(persons, schema='id INT, age INT')
personsDF. \
    withColumn(
        'category',
        expr("""
            CASE
            WHEN age BETWEEN 0 AND 2 THEN 'New Born'
            WHEN age > 2 AND age <= 12 THEN 'Infant'
            WHEN age > 12 AND age <= 48 THEN 'Toddler'
            WHEN age > 48 AND age <= 144 THEN 'Kid'
            ELSE 'Teenager or Adult'
            END
        """)
    ). \
    show()
                                        
personsDF. \
    withColumn(
        'category',
        when(col('age').between(0, 2), 'New Born').
        when((col('age') > 2) & (col('age') <= 12), 'Infant').
        when((col('age') > 12) & (col('age') <= 48), 'Toddler').
        when((col('age') > 48) & (col('age') <= 144), 'Kid').
        otherwise('Teenager or Adult')
    ). \
    show()                                        
 '''
 +---+---+-----------------+
| id|age|         category|
+---+---+-----------------+
|  1|  1|         New Born|
|  2| 13|          Toddler|
|  3| 18|          Toddler|
|  4| 60|              Kid|
|  5|120|              Kid|
|  6|  0|         New Born|
|  7| 12|           Infant|
|  8|160|Teenager or Adult|
+---+---+-----------------+
'''
                                        
                                        
#Sect06: Filtering from df

from pyspark.sql import Row                                        
import datetime
                                        
users = [
    {
        "id": 1,
        "first_name": "Corrie",
        "last_name": "Van den Oord",
        "email": "cvandenoord0@etsy.com",
        "gender": "male",
        "current_city": "Dallas",
        "phone_numbers": Row(mobile="+1 234 567 8901", home="+1 234 567 8911"),
        "courses": [1, 2],
        "is_customer": True,
        "amount_paid": 1000.55,
        "customer_from": datetime.date(2021, 1, 15),
        "last_updated_ts": datetime.datetime(2021, 2, 10, 1, 15, 0)
    },
    {
        "id": 2,
        "first_name": "Nikolaus",
        "last_name": "Brewitt",
        "email": "nbrewitt1@dailymail.co.uk",
        "gender": "male",
        "current_city": "Houston",
        "phone_numbers":  Row(mobile="+1 234 567 8923", home="1 234 567 8934"),
        "courses": [3],
        "is_customer": True,
        "amount_paid": 900.0,
        "customer_from": datetime.date(2021, 2, 14),
        "last_updated_ts": datetime.datetime(2021, 2, 18, 3, 33, 0)
    },
    {
        "id": 3,
        "first_name": "Orelie",
        "last_name": "Penney",
        "email": "openney2@vistaprint.com",
        "gender": "female",
        "current_city": "",
        "phone_numbers": Row(mobile="+1 714 512 9752", home="+1 714 512 6601"),
        "courses": [2, 4],
        "is_customer": True,
        "amount_paid": 850.55,
        "customer_from": datetime.date(2021, 1, 21),
        "last_updated_ts": datetime.datetime(2021, 3, 15, 15, 16, 55)
    },
    {
        "id": 4,
        "first_name": "Ashby",
        "last_name": "Maddocks",
        "email": "amaddocks3@home.pl",
        "gender": "male",
        "current_city": "San Fransisco",
        "phone_numbers": Row(mobile=None, home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 10, 17, 45, 30)
    },
    {
        "id": 5,
        "first_name": "Kurt",
        "last_name": "Rome",
        "email": "krome4@shutterfly.com",
        "gender": "female",
        "current_city": None,
        "phone_numbers": Row(mobile="+1 817 934 7142", home=None),
        "courses": [],
        "is_customer": False,
        "amount_paid": None,
        "customer_from": None,
        "last_updated_ts": datetime.datetime(2021, 4, 2, 0, 55, 18)
    }
]                                       
                                        
import pandas as pd
spark.conf.set('spark.sql.execution.arrow.pyspark.enabled', False)
users_df = spark.createDataFrame(pd.DataFrame(users))
                                        '''
+---+----------+------------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+
| id|first_name|   last_name|               email|gender| current_city|       phone_numbers|courses|is_customer|amount_paid|customer_from|    last_updated_ts|
+---+----------+------------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+
|  1|    Corrie|Van den Oord|cvandenoord0@etsy...|  male|       Dallas|{+1 234 567 8901,...| [1, 2]|       true|    1000.55|   2021-01-15|2021-02-10 01:15:00|
|  2|  Nikolaus|     Brewitt|nbrewitt1@dailyma...|  male|      Houston|{+1 234 567 8923,...|    [3]|       true|      900.0|   2021-02-14|2021-02-18 03:33:00|
|  3|    Orelie|      Penney|openney2@vistapri...|female|             |{+1 714 512 9752,...| [2, 4]|       true|     850.55|   2021-01-21|2021-03-15 15:16:55|
|  4|     Ashby|    Maddocks|  amaddocks3@home.pl|  male|San Fransisco|        {null, null}|     []|      false|        NaN|         null|2021-04-10 17:45:30|
|  5|      Kurt|        Rome|krome4@shutterfly...|female|         null|{+1 817 934 7142,...|     []|      false|        NaN|         null|2021-04-02 00:55:18|
+---+----------+------------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+
                                        '''
                                        
help(users_df.filter)
'''
filter(condition) method of pyspark.sql.dataframe.DataFrame instance
    Filters rows using the given condition.
    :func:`where` is an alias for :func:`filter`.
 condition : :class:`Column` or str
        a :class:`Column` of :class:`types.BooleanType`
        or a string of SQL expression. 
>>> df.filter(df.age > 3).collect()
    [Row(age=5, name='Bob')]
    >>> df.where(df.age == 2).collect()
    [Row(age=2, name='Alice')]
    
>>> df.filter("age > 3").collect()
    [Row(age=5, name='Bob')]
    >>> df.where("age = 2").collect()
    [Row(age=2, name='Alice')]    
'''
#We can pass conditions either by using SQL Style or Non SQL Style.
from pyspark.sql.functions import col
users_df.where(col('id') == 1).show()
users_df.filter(users_df['id'] == 1).show()
users_df.where(users_df['id'] == 1).show()
# 'id == 1' also works
users_df.filter('id = 1').show()
'''
                                        
    Equal -> = or ==
    Not Equal -> !=
    Greater Than -> >
    Less Than -> <
    Greater Than or Equal To -> >=
    Less Than or Equal To -> <=
    IN Operator -> isin function or IN or contains function
    Between Operator -> between function or BETWEEN with AND

'''
#Get list of customers (is_customer flag is set to true)                                        
users_df.filter(col('is_customer') == True).show()
users_df.filter(col('is_customer') == 'true').show()                                        
users_df.filter('is_customer = "true"').show()                                        
users_df.createOrReplaceTempView('users')                                        
spark.sql('''SELECT * FROM usersWHERE is_customer = "true"''').show()

#Get users from Dallas
users_df.filter("current_city == 'Dallas'").show()
# Get the customers who paid 900.0
users_df.filter(col('amount_paid') == '900.0').show()
users_df.filter('amount_paid == "900.0"').show()
                                        
#Get the customers where paid amount is not a number
from pyspark.sql.functions import isnan
users_df.select('amount_paid', isnan('amount_paid')).show()                                       
'''
+-----------+------------------+
|amount_paid|isnan(amount_paid)|
+-----------+------------------+
|    1000.55|             false|
|      900.0|             false|
|     850.55|             false|
|        NaN|              true|
|        NaN|              true|
+-----------+------------------+
'''
users_df.filter(isnan('amount_paid') == True).show()                                        
'''
+---+----------+---------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+
| id|first_name|last_name|               email|gender| current_city|       phone_numbers|courses|is_customer|amount_paid|customer_from|    last_updated_ts|
+---+----------+---------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+
|  4|     Ashby| Maddocks|  amaddocks3@home.pl|  male|San Fransisco|        {null, null}|     []|      false|        NaN|         null|2021-04-10 17:45:30|
|  5|      Kurt|     Rome|krome4@shutterfly...|female|         null|{+1 817 934 7142,...|     []|      false|        NaN|         null|2021-04-02 00:55:18|
+---+----------+---------+--------------------+------+-------------+--------------------+-------+-----------+-----------+-------------+-------------------+

'''
#Get all the users who are not living in Dallas.
users_df. \
    select('id', 'current_city'). \
    show()   
                                        '''
+---+-------------+
| id| current_city|
+---+-------------+
|  1|       Dallas|
|  2|      Houston|
|  3|             |
|  4|San Fransisco|
|  5|         null|
+---+-------------+
'''
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') != 'Dallas'). \
    show()
                                        '''
+---+-------------+
| id| current_city|
+---+-------------+
|  2|      Houston|
|  3|             |
|  4|San Fransisco|
+---+-------------+
'''
users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') != 'Dallas') | (col('current_city').isNull())). \
    show()
'''
+---+-------------+
| id| current_city|
+---+-------------+
|  2|      Houston|
|  3|             |
|  4|San Fransisco|
|  5|         null|
+---+-------------+
'''
#sql style same as above result
users_df. \
    select('id', 'current_city'). \
    filter("current_city != 'Dallas' OR current_city IS NULL"). \
    show()
                                        
#Get user id and email whose last updated timestamp is between 2021 Feb 15th and 2021 March 15th.
users_df. \
    select('id', 'email', 'last_updated_ts'). \
    show()
'''
+---+--------------------+-------------------+
| id|               email|    last_updated_ts|
+---+--------------------+-------------------+
|  1|cvandenoord0@etsy...|2021-02-10 01:15:00|
|  2|nbrewitt1@dailyma...|2021-02-18 03:33:00|
|  3|openney2@vistapri...|2021-03-15 15:16:55|
|  4|  amaddocks3@home.pl|2021-04-10 17:45:30|
|  5|krome4@shutterfly...|2021-04-02 00:55:18|
+---+--------------------+-------------------+
'''
c = col('last_updated_ts')
help(c.between)
                                        '''
between(lowerBound, upperBound) method of pyspark.sql.column.Column instance
    True if the current column is between the lower bound and upper bound, inclusive.
                                        '''
users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter(col('last_updated_ts').between('2021-02-15 00:00:00', '2021-03-15 23:59:59')). \
    show()                                        
users_df. \
    select('id', 'email', 'last_updated_ts'). \
    filter("last_updated_ts BETWEEN '2021-02-15 00:00:00' AND '2021-03-15 23:59:59'"). \
    show()                                        
#Get all the users whose payment is in the range of 850 and 900.
users_df. \
    select('id', 'amount_paid'). \
    filter(col('amount_paid').between(850, 900)). \
    show()
                                        
users_df. \
    select('id', 'amount_paid'). \
    filter('amount_paid BETWEEN "850" AND "900"'). \
    show()                                        

#08 Dealing with Null Values while Filtering Data in Spark Data Frames
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isNotNull()). \
    show()     
                                       
users_df. \
    select('id', 'current_city'). \
    filter('current_city IS NOT NULL'). \
    show()
'''
+---+-------------+
| id| current_city|
+---+-------------+
|  1|       Dallas|
|  2|      Houston|
|  3|             |
|  4|San Fransisco|
+---+-------------+

'''
#Get all the users whose city is null                                        
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isNull()). \
    show()                                       
users_df. \
    select('id', 'current_city'). \
    filter('current_city IS NULL'). \
    show()
                                        
'''
Boolean Operations
    Boolean OR
    Boolean AND
    Negation NOT
Get list of users whose city is null or empty string (users with no cities associated)    
'''
users_df. \
    select('id', 'current_city'). \
    filter((col('current_city') == '') | (col('current_city').isNull())). \
    show()          
                                        
# this will fail because conditions are not enclosed in circular brackets
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city') == '' | (col('current_city').isNull())). \
    show()                                        
                                        
users_df. \
    select('id', 'current_city'). \
    filter("current_city = '' OR current_city IS NULL"). \
    show()                                      
                                        

    #Get list of users whose city is either Houston or Dallas.
users_df. \
    select('id', 'current_city'). \
    filter(col('current_city').isin('Houston', 'Dallas')). \
    show()
                                        
users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas')"). \
    show()                                        
                                        
# Boolean OR including null check
users_df. \
    select('id', 'current_city'). \
    filter((col('current_city').isin('Houston', 'Dallas', '')) | (col('current_city').isNull())). \
    show()
                                        
users_df. \
    select('id', 'current_city'). \
    filter("current_city IN ('Houston', 'Dallas', '') OR current_city IS NULL"). \
    show()
                                        
'''
+---+------------+
| id|current_city|
+---+------------+
|  1|      Dallas|
|  2|     Houston|
|  3|            |
|  5|        null|
+---+------------+

'''
#Get Customers who paid greater than 900                                        
from pyspark.sql.functions import col, isnan
                                        
users_df. \
    select('id', 'amount_paid'). \
    show()
'''
+---+-----------+
| id|amount_paid|
+---+-----------+
|  1|    1000.55|
|  2|      900.0|
|  3|     850.55|
|  4|        NaN|
|  5|        NaN|
+---+-----------+
'''
                                        
users_df. \
    filter((col('amount_paid') > 900) & (isnan(col('amount_paid')) == False)). \
    select('id', 'amount_paid'). \
    show()

users_df. \
    filter('amount_paid > 900 AND isnan(amount_paid) = false'). \
    select('id', 'amount_paid'). \
    show()                                        
'''
+---+-----------+
| id|amount_paid|
+---+-----------+
|  1|    1000.55|
+---+-----------+
'''
#Get the users who became customers after 2021-01-21
users_df. \
    select('id', 'customer_from'). \
    filter(col('customer_from') > '2021-01-21'). \
    show()
                                        
users_df. \
    select('id', 'customer_from'). \
    filter('customer_from > "2021-01-21"').show()
                                        
                                        
#Get Male Customers (gender is male and is_customer is equals to true)                                        
users_df. \
    filter((col('gender') == 'male') & (col('is_customer') == True)). \
    select('id', 'gender', 'is_customer'). \
    show()                                        
users_df. \
    filter("gender = 'male' AND is_customer = true"). \
    select('id', 'gender', 'is_customer'). \
    show()                                        
                                        
#Get the users who become customers between 2021 Jan 20th and 2021 Feb 15th.                                        
users_df. \
    filter((col('customer_from') >= '2021-01-20') & (col('customer_from') <= '2021-02-15')). \
    select('id', 'customer_from'). \
    show()                                       
users_df. \
    filter("customer_from >= '2021-01-20' AND customer_from  <= '2021-02-15'"). \
    select('id', 'customer_from'). \
    show()                                        
                                        
#Get id and email of users who are not customers or city contain empty string.
                                        
users_df. \
    filter((col('current_city') == '') | (col('is_customer') == False)). \
    select('id', 'email', 'current_city', 'is_customer'). \
    show()
                                        
users_df. \
    filter("current_city = '' OR is_customer = false"). \
    select('id', 'email', 'current_city', 'is_customer'). \
    show()
                                        
                                        
#Get id and email of users who are not customers or customers whose last updated time is before 2021-03-01
                                        
users_df. \
    filter((col('is_customer') == False) | (col('last_updated_ts') < '2021-03-01')). \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    show()
                                        
users_df. \
    filter("is_customer = false OR last_updated_ts < '2021-03-01'"). \
    select('id', 'email', 'is_customer', 'last_updated_ts'). \
    show()
                                        
'''
+---+--------------------+-----------+-------------------+
| id|               email|is_customer|    last_updated_ts|
+---+--------------------+-----------+-------------------+
|  1|cvandenoord0@etsy...|       true|2021-02-10 01:15:00|
|  2|nbrewitt1@dailyma...|       true|2021-02-18 03:33:00|
|  4|  amaddocks3@home.pl|      false|2021-04-10 17:45:30|
|  5|krome4@shutterfly...|      false|2021-04-02 00:55:18|
+---+--------------------+-----------+-------------------+

'''
                                        
                                        
                                        
