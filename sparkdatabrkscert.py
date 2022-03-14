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

