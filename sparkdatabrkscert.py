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






























