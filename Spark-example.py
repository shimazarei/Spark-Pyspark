
# coding: utf-8

# In[1]:


import findspark


# In[2]:


findspark.init()


# In[3]:


from pyspark import SparkContext, SparkConf


# In[4]:


conf = SparkConf()
sc =SparkContext()


# In[5]:


conf.setMaster('yarn-client')


# In[6]:


conf.setAppName('anaconda-pyspark')


# In[7]:


data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)


# In[8]:


distData


# In[9]:


def mod(x):
    import numpy as np
    return (x, np.mod(x, 2))

rdd = sc.parallelize(range(1000)).map(mod).take(10)
print(rdd)


# In[10]:


# 1. Read external data in Spark as Rdd
rdd = sc.textFile("path")


# In[11]:


# 2. Create rdd from a list
rdd = sc.parallelize(["id","name","3","5"])


# In[12]:


import numpy as np


# In[13]:


from pyspark.sql import SparkSession
spark = SparkSession.builder         .master("local")         .appName("Example")         .config("spark.some.config.option", "some-value")         .getOrCreate()


# In[14]:


# 3. Dataframe to rdd
import pandas as pd
import pyspark.sql 
from pyspark.sql import SQLContext
df = pd.DataFrame(data=[1,2,3], columns=["a"])
print(df)
spDF =spark.createDataFrame(df,['Number'])
spDF.show()
spDF.rdd.first()


# In[15]:


s = [(['Shima'], 35)]
df2 = spark.createDataFrame(s)
df2.collect()


# In[16]:


spark.createDataFrame(s, ['name', 'age']).collect()


# In[17]:


d = [{'name': 'Shali', 'age': 5}]
spark.createDataFrame(d).collect()


# In[18]:


rdd = sc.parallelize(s)
spark.createDataFrame(rdd).collect()


# In[19]:


df = spark.createDataFrame(rdd, ['name', 'age'])
df.collect()


# In[20]:


from pyspark.sql import Row


# In[21]:


Person = Row('name', 'age')


# In[22]:


person = rdd.map(lambda r: Person(*r))


# In[23]:


df2 = spark.createDataFrame(person)


# In[24]:


df2.collect()


# In[25]:


from pyspark.sql.types import *
schema = StructType([
         StructField("name", StringType(), True),
         StructField("age", IntegerType(), True)])


# In[26]:


df3 = spark.createDataFrame(rdd, schema)
df3.collect()


# In[27]:


spark.createDataFrame(df.toPandas()).collect()


# In[28]:


spark.createDataFrame(pd.DataFrame([[1, 2]])).collect()


# In[29]:


spark.createDataFrame(rdd, "a: string, b: int").collect()


# In[30]:


rdd = rdd.map(lambda row: row[1])


# In[31]:


spark.createDataFrame(rdd, "int").collect()


# In[32]:


spark.range(1, 7, 2).collect()


# In[33]:


df.createOrReplaceTempView("table1")
df2 = spark.sql("SELECT * from table1")
df2.show()


# In[34]:


df.createOrReplaceTempView("table1")
df2 = spark.table("table1")
sorted(df.collect()) == sorted(df2.collect())


# In[35]:


l = [('Alice', 1)]


# In[36]:


from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
sqlContext.createDataFrame(l).collect()


# In[37]:


sqlContext.createDataFrame(l, ['name', 'age']).collect()


# In[38]:


d = [{'name': 'Alice', 'age': 1}]
sqlContext.createDataFrame(d).collect()


# In[39]:


rdd = sc.parallelize(l)


# In[40]:


sqlContext.createDataFrame(rdd).collect()


# In[41]:


df = sqlContext.createDataFrame(rdd, ['name', 'age'])


# In[42]:


df.collect()


# In[43]:


sqlContext.registerDataFrameAsTable(df, "table1")
sqlContext.dropTempTable("table1")


# In[44]:


sqlContext.getConf("spark.sql.shuffle.partitions")


# In[45]:


sqlContext.getConf("spark.sql.shuffle.partitions", u"10")


# In[46]:


sqlContext.setConf("spark.sql.shuffle.partitions", u"50")


# In[47]:


sqlContext.getConf("spark.sql.shuffle.partitions", u"10")


# In[48]:


sqlContext.range(1, 7, 2).collect()


# In[49]:


sqlContext.range(3).collect()


# In[50]:


import tempfile
text_sdf = sqlContext.readStream.text(tempfile.mkdtemp())
text_sdf.isStreaming


# In[51]:


sqlContext.registerDataFrameAsTable(df, "table1")


# In[52]:


sqlContext.registerFunction("stringLengthString", lambda x: len(x))


# In[53]:


sqlContext.sql("SELECT stringLengthString('test')").collect()


# In[54]:


from pyspark.sql.types import IntegerType
sqlContext.registerFunction("stringLengthInt", lambda x: len(x), IntegerType())
sqlContext.sql("SELECT stringLengthInt('test')").collect()


# In[55]:


sqlContext.udf.register("stringLengthInt", lambda x: len(x), IntegerType())
sqlContext.sql("SELECT stringLengthInt('test')").collect()


# In[56]:


sqlContext.registerFunction("stringLengthString", lambda x: len(x))


# In[57]:


sqlContext.sql("SELECT stringLengthString('test')").collect()


# In[58]:


sqlContext.registerDataFrameAsTable(df, "table1")


# In[59]:


df2 = sqlContext.table("table1")


# In[60]:


sorted(df.collect()) == sorted(df2.collect())


# In[61]:


sqlContext.registerDataFrameAsTable(df, "table1")


# In[62]:


"table1" in sqlContext.tableNames()


# In[63]:


"table1" in sqlContext.tableNames("default")


# In[64]:


df2 = sqlContext.tables()


# In[65]:


df2.filter("tableName = 'table1'").first()


# In[66]:


sqlContext.registerFunction("stringLengthString", lambda x: len(x))


# In[67]:


sqlContext.sql("SELECT stringLengthString('test')").collect()


# In[68]:


df.agg({"age": "max"}).collect()


# In[69]:


from pyspark.sql import functions as F


# In[70]:


df.agg(F.min(df.age)).collect()


# In[71]:


from pyspark.sql.functions import *
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
joined_df.select("df_as1.name", "df_as2.name", "df_as2.age").collect()


# In[72]:


df.coalesce(1).rdd.getNumPartitions()


# In[73]:


df.collect()


# In[74]:


df.columns
df.count()


# In[75]:


df.createGlobalTempView("people")


# In[76]:


df2 = spark.sql("select * from global_temp.people")
sorted(df.collect()) == sorted(df2.collect())


# In[77]:


df.createOrReplaceTempView("people")


# In[78]:


df2 = df.filter(df.age > 3)
df2.createOrReplaceTempView("people")
df3 = spark.sql("select * from people")
sorted(df3.collect()) == sorted(df2.collect())


# In[79]:


spark.catalog.dropTempView("people")


# In[80]:


df.select("age", "name").collect()


# In[81]:


df.cube("name", df.age).count().orderBy("name", "age").show()


# In[82]:


df.describe(['age']).show()


# In[83]:


#df.drop('age').collect()
#df.drop(df.age).collect()
#df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()
#df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()


# In[84]:


df.distinct().count()


# In[85]:


from pyspark.sql import Row
df = sc.parallelize([         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=10, height=80)]).toDF()
df.dropDuplicates().show()


# In[86]:


df.dtypes


# In[87]:


df.explain()


# In[88]:


df.explain(True)


# In[89]:


df.na.fill(50).show()


# In[90]:


df.na.fill({'age': 50, 'name': 'unknown'}).show()


# In[91]:


df.filter(df.age > 3).collect()


# In[92]:


# To create DataFrame using SQLContext
#people = sqlContext.read.parquet("...")
#department = sqlContext.read.parquet("...")

#people.filter(people.age > 30).join(department, people.deptId == department.id) \
#  .groupBy(department.name, "gender").agg({"salary": "avg", "age": "max"})


# In[93]:


df.agg({"age": "max"}).collect()


# In[94]:


from pyspark.sql import functions as F


# In[95]:


df.agg(F.min(df.age)).collect()


# In[96]:


from pyspark.sql.functions import *
df_as1 = df.alias("df_as1")
df_as2 = df.alias("df_as2")
joined_df = df_as1.join(df_as2, col("df_as1.name") == col("df_as2.name"), 'inner')
joined_df.select("df_as1.name", "df_as2.name", "df_as2.age").collect()


# In[97]:


#Returns a new DataFrame that has exactly numPartitions partitions
df.coalesce(1).rdd.getNumPartitions()


# In[98]:


df.collect()


# In[99]:


df.columns


# In[100]:


#return number of rows
df.count()


# In[101]:


df.createGlobalTempView("human")
df2 = spark.sql("select * from global_temp.human")
sorted(df.collect()) == sorted(df2.collect())


# In[102]:


spark.catalog.dropGlobalTempView("human")


# In[103]:


df2 = df.filter(df.age > 3)


# In[104]:


df2.createOrReplaceTempView("people")


# In[105]:


df3 = spark.sql("select * from people")


# In[106]:


sorted(df3.collect()) == sorted(df2.collect())


# In[107]:


spark.catalog.dropTempView("people")


# In[108]:


df.createTempView("people")


# In[109]:


df2 = spark.sql("select * from people")


# In[110]:


sorted(df.collect()) == sorted(df2.collect())


# In[111]:


df.select("age", "name", "height").collect()


# In[112]:


df2.select("name", "height").collect()


# In[113]:


df.crossJoin(df2.select("height")).select("age", "name").collect()


# In[114]:


df.select("height").collect()


# In[115]:


#Create a multi-dimensional cube for the current DataFrame using the specified columns
df.cube("name", df.age).count().orderBy("name", "age").show()


# In[116]:


df.describe(['age']).show()


# In[117]:


df.describe().show()


# In[118]:


df.distinct().count()


# In[119]:


df.drop('age').collect()


# In[120]:


df.drop(df.age).collect()


# In[121]:


df.join(df2, df.name == df2.name, 'inner').drop(df.name).collect()


# In[122]:


df.join(df2, df.name == df2.name, 'inner').drop(df2.name).collect()


# In[123]:


df.join(df2, 'name', 'inner').drop('age', 'height').collect()


# In[124]:


from pyspark.sql import Row
df = sc.parallelize([         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=10, height=80)]).toDF()
df.dropDuplicates().show()


# In[125]:


df.dropDuplicates(['name', 'height']).show()


# In[126]:


df.join(df, ['name', 'age']).select(df.name, df.age).collect()


# In[127]:


#Calculates the hash code of given columns
spark.createDataFrame([('ABC',)], ['a']).select(hash('a').alias('hash')).collect()


# In[128]:


df.cube("name").agg(grouping_id(), sum("age")).orderBy("name").show()


# In[129]:


#Computes hex value of the given column
spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()


# In[130]:


#Extract the hours of a given date as integer
df = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])
df.select(hour('a').alias('hour')).collect()


# In[131]:


#Translate the first letter of each word to upper case in the sentence
spark.createDataFrame([('ab cd',)], ['a']).select(initcap("a").alias('v')).collect()


# In[132]:


#Locate the position of the first occurrence of substr column in the given string
df = spark.createDataFrame([('abcd',)], ['s',])
df.select(instr(df.s, 'b').alias('s')).collect()


# In[133]:


#An expression that returns true iff the column is NaN
df = spark.createDataFrame([(1.0, float('nan')), (float('nan'), 2.0)], ("a", "b"))
df.select(isnan("a").alias("r1"), isnan(df.a).alias("r2")).collect()


# In[134]:


#An expression that returns true iff the column is null
df = spark.createDataFrame([(1, None), (None, 2)], ("a", "b"))
df.select(isnull("a").alias("r1"), isnull(df.a).alias("r2")).collect()


# In[135]:


#Creates a new row for a json column according to the given field names
data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]
df = spark.createDataFrame(data, ("key", "jstring"))
df.select(df.key, json_tuple(df.jstring, 'f1', 'f2')).collect()


# In[136]:


#Returns the last day of the month which the given date belongs to
df = spark.createDataFrame([('1997-02-10',)], ['d'])
df.select(last_day(df.d).alias('date')).collect()


# In[137]:


#Returns the least value of the list of column names, skipping null values
df = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])
df.select(least(df.a, df.b, df.c).alias("least")).collect()


# In[138]:


#Calculates the length of a string or binary expression
spark.createDataFrame([('ABC',)], ['a']).select(length('a').alias('length')).collect()


# In[139]:


#Computes the Levenshtein distance of the two given strings
df0 = spark.createDataFrame([('kitten', 'sitting',)], ['l', 'r'])
df0.select(levenshtein('l', 'r').alias('d')).collect()


# In[140]:


#Locate the position of the first occurrence of substr in a string column, after position pos
dfs = spark.createDataFrame([('abcd',)], ['s',])
dfs.select(locate('b', dfs.s, 1).alias('s')).collect()


# In[141]:


d = [{'name': ['Shima','Dana','Shali','Arshan'], 'Age':['35','2','5','3'], 'height':['163','50','60','30']}]
df1 = spark.createDataFrame(d)
df1.show()


# In[142]:


from pyspark.sql import Row
df = sc.parallelize([         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=5, height=80),         Row(name='Alice', age=10, height=80),
        Row(name='Bob',   age=20, height=100),
        Row(name='Shima', age=35, height=163),
        Row(name='Shali', age=37, height=170)]).toDF()
df.dropDuplicates().show()


# In[143]:


#If there is only one argument, then this takes the natural logarithm of the argument
df.select(log(10.0, df.age).alias('ten')).rdd.map(lambda l: str(l.ten)[:7]).collect()


# In[144]:


df.select('*').collect()


# In[145]:


df.select('name', 'age').collect()


# In[146]:


df.select(df.name, (df.age + 10).alias('age')).collect()


# In[147]:


df.selectExpr("age * 2", "abs(age)").collect()


# In[148]:


df


# In[149]:


df.show(truncate=3)


# In[150]:


df.sort(df.age.desc()).collect()


# In[151]:


df.sort("age", ascending=False).collect()


# In[152]:


df.orderBy(df.age.desc()).collect()


# In[153]:


from pyspark.sql.functions import *
df.sort(asc("age")).collect()


# In[154]:


df.orderBy(desc("age"), "name").collect()


# In[155]:


df.orderBy(["age", "name"], ascending=[0, 1]).collect()


# In[156]:


df.sortWithinPartitions("age", ascending=False).show()


# In[157]:


#Get the DataFrame‘s current storage level.
df.storageLevel


# In[158]:


df.cache().storageLevel


# In[159]:


#df2.persist(storageLevel.DISK_ONLY_2).storageLevel


# In[160]:


df.take(2)


# In[161]:


df.toDF('f1', 'f2', 'f3').collect()


# In[162]:


df.toJSON().first()


# In[163]:


list(df.toLocalIterator())


# In[164]:


df.toPandas()


# In[165]:


df.withColumn('age2', df.age + 2).collect()


# In[166]:


df.withColumnRenamed('age', 'age2').collect()


# In[167]:


df.groupBy().avg('age').collect()


# In[168]:


df3.groupBy().avg('age', 'height').collect()
df.show()


# In[169]:


sorted(df.groupBy(df.age).count().collect())


# In[170]:


df.groupBy().max('age').collect()


# In[171]:


df3.groupBy().max('age', 'height').collect()


# In[172]:


df.groupBy().mean('age').collect()


# In[173]:


df3.groupBy().mean('age', 'height').collect()


# In[174]:


df.groupBy().min('age').collect()


# In[175]:


df3.groupBy().min('age', 'height').collect()


# In[176]:


df.groupBy("age").pivot("name").sum("height").collect()
df.show()


# In[177]:


df.groupBy().sum('age').collect()


# In[178]:


df3.groupBy().sum('age', 'height').collect()


# In[179]:


df.select(df.age.alias("age")).collect()


# In[180]:


df.select(df.name, df.age.between(2, 4)).show()


# In[181]:


df.select(df.age.cast("string").alias('ages')).collect()


# In[182]:


df.select(df.age.cast(StringType()).alias('ages')).collect()
df.show()


# In[183]:


dfr = sc.parallelize([Row(r=Row(a=1, b="b"))]).toDF()


# In[184]:


dfr.select(dfr.r.getField("b")).show()
df.show()


# In[185]:


dfr.select(dfr.r.a).show()


# In[186]:


#An expression that gets an item at position ordinal out of a list, or gets an item by key out of a dict.
dfl = sc.parallelize([([1, 2], {"key": "value"})]).toDF(["l", "d"])


# In[187]:


dfl.select(dfl.l.getItem(0), dfl.d.getItem("key")).show()


# In[188]:


dfl.select(dfl.l[0], dfl.d["key"]).show()


# In[189]:


from pyspark.sql.functions import *
from pyspark.sql import functions as F
df.select(df.name, F.when(df.age > 3, 1).otherwise(0)).show()


# In[190]:


from pyspark.sql import Window
window = Window.partitionBy("name").orderBy("age").rowsBetween(-1, 1)
from pyspark.sql.functions import rank, min


# In[191]:


df.select(df.name.substr(1, 3).alias("col")).collect()


# In[192]:


df.show()


# In[193]:


df.select(df.name, F.when(df.age > 4, 1).when(df.age < 3, -1).otherwise(0)).show()


# In[194]:


row = Row(name="Alice", age=11)


# In[195]:


row['name'], row['age']


# In[196]:


row.name, row.age


# In[197]:


df.show()


# In[198]:


'name' in row


# In[199]:


'wrong_key' in row


# In[200]:


Person = Row("name", "age")
Person


# In[201]:


'name' in Person


# In[202]:


'wrong_key' in Person


# In[203]:


Person("Alice", 11)


# In[204]:


Row(name="Alice", age=11).asDict() == {'name': 'Alice', 'age': 11}


# In[205]:


row = Row(key=1, value=Row(name='a', age=2))


# In[206]:


row.asDict() == {'key': 1, 'value': Row(age=2, name='a')}


# In[207]:


row.asDict(True) == {'key': 1, 'value': {'name': 'a', 'age': 2}}


# In[208]:


df.na.drop().show()


# In[223]:


df.na.fill({'age': 50, 'name': 'unknown'}).show()


# In[224]:


df.na.replace(10, 20).show()


# In[225]:


df.na.replace(['Alice', 'Bob'], ['A', 'B'], 'name').show()


# In[226]:


df.show()


# In[227]:


# ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
window = Window.orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)


# In[228]:


df.show()


# In[229]:


# PARTITION BY country ORDER BY date RANGE BETWEEN 3 PRECEDING AND 3 FOLLOWING
window = Window.orderBy("date").partitionBy("country").rangeBetween(-3, 3)


# In[230]:


df.show()


# In[231]:


df.dtypes


# In[232]:


df.show()


# In[233]:


struct1 = StructType([StructField("f1", StringType(), True)])
struct1["f1"]


# In[234]:


df.show()


# In[235]:


struct1[0]


# In[236]:


struct1 = StructType().add("f1", StringType(), True).add("f2", StringType(), True, None)


# In[219]:


struct2 = StructType([StructField("f1", StringType(), True),     StructField("f2", StringType(), True, None)])
struct1 == struct2


# In[237]:


df.show()


# In[238]:


struct1 = StructType().add(StructField("f1", StringType(), True))


# In[239]:


struct2 = StructType([StructField("f1", StringType(), True)])
struct1 == struct2


# In[222]:


struct1 = StructType().add("f1", "string", True)
struct2 = StructType([StructField("f1", StringType(), True)])
struct1 == struct2


# In[240]:


df.show()


# In[241]:


dfres = spark.createDataFrame([('2015-04-08',)], ['d'])
dfres.select(add_months(dfres.d, 1).alias('d')).collect()


# In[231]:


dfres.show()


# In[242]:


df.show()


# In[243]:


dfre = spark.createDataFrame([(["a", "b", "c"],), ([],)], ['data'])


# In[244]:


dfre.select(array_contains(dfre.data, "a")).collect()


# In[245]:


df.show()


# In[246]:


df.select(bin(df.age).alias('c')).collect()


# In[247]:


spark.createDataFrame([(2.5,)], ['a']).select(bround('a', 0).alias('r')).collect()


# In[248]:


df.show()


# In[251]:


cDf = spark.createDataFrame([(None, None), (1, None), (None, 2)], ("a", "b"))
cDf.show()


# In[252]:


cDf.select(coalesce(cDf["a"], cDf["b"])).show()


# In[253]:


cDf.select('*', coalesce(cDf["a"], lit(0.0))).show()


# In[254]:


dfz = spark.createDataFrame([('abcd','123')], ['s', 'd'])


# In[255]:


dfz.select(concat(dfz.s, dfz.d).alias('s')).collect()


# In[256]:


dfx = spark.createDataFrame([('abcd','123')], ['s', 'd'])


# In[259]:


dfz.select(concat_ws('-', dfz.s, dfz.d).alias('s')).collect()


# In[261]:


dfc = spark.createDataFrame([("010101",)], ['n'])
dfc.select(conv(dfc.n, 2, 16).alias('hex')).collect()


# In[262]:


dfn = spark.createDataFrame([(5,)], ['n'])


# In[263]:


dfn.select(factorial(dfn.n).alias('f')).collect()


# In[264]:


spark.createDataFrame([(5,)], ['a']).select(format_number('a', 4).alias('v')).collect()


# In[265]:


dfi = spark.createDataFrame([(5, "hello")], ['a', 'b'])


# In[268]:


dfi.select(format_string('%d %s', dfi.a, dfi.b).alias('v')).collect()


# In[269]:


from pyspark.sql.types import *
data = [(1, '''{"a": 1}''')]


# In[270]:


schema = StructType([StructField("a", IntegerType())])


# In[271]:


dfh = spark.createDataFrame(data, ("key", "value"))


# In[273]:


dfh.select(from_json(dfh.value, schema).alias("json")).collect()


# In[274]:


dfj = spark.createDataFrame([('1997-02-28 10:30:00',)], ['t'])


# In[276]:


dfj.select(from_utc_timestamp(dfj.t, "PST").alias('t')).collect()


# In[277]:


df.show()


# In[278]:


data = [("1", '''{"f1": "value1", "f2": "value2"}'''), ("2", '''{"f1": "value12"}''')]


# In[279]:


dfy = spark.createDataFrame(data, ("key", "jstring"))


# In[281]:


dfy.select(dfy.key, get_json_object(dfy.jstring, '$.f1').alias("c0"),                   get_json_object(dfy.jstring, '$.f2').alias("c1") ).collect()


# In[282]:


dfq = spark.createDataFrame([(1, 4, 3)], ['a', 'b', 'c'])


# In[283]:


dfq.select(greatest(dfq.a, dfq.b, dfq.c).alias("greatest")).collect()


# In[284]:


df.cube("name").agg(grouping("name"), sum("age")).orderBy("name").show()


# In[285]:


df.cube("name").agg(grouping_id(), sum("age")).orderBy("name").show()


# In[286]:


spark.createDataFrame([('ABC',)], ['a']).select(hash('a').alias('hash')).collect()


# In[288]:


spark.createDataFrame([('ABC', 3)], ['a', 'b']).select(hex('a'), hex('b')).collect()


# In[289]:


dfll = spark.createDataFrame([('2015-04-08 13:08:15',)], ['a'])


# In[291]:


dfll.select(hour('a').alias('hour')).collect()

