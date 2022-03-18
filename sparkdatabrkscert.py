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
+-----------+----------+---------+------+--------------+----------------+-----------+
|employee_id|first_name|last_name|salary|nationality   |phone_number    |ssn        |
+-----------+----------+---------+------+--------------+----------------+-----------+
|1          |Scott     |Tiger    |1000.0|united states |+1 123 456 7890 |123 45 6789|
|2          |Henry     |Ford     |1250.0|India         |+91 234 567 8901|456 78 9123|
|3          |Nick      |Junior   |750.0 |united KINGDOM|+44 111 111 1111|222 33 4444|
|4          |Bill      |Gomes    |1500.0|AUSTRALIA     |+61 987 654 3210|789 12 6118|
+-----------+----------+---------+------+--------------+----------------+-----------+

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
