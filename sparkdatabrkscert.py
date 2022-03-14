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
