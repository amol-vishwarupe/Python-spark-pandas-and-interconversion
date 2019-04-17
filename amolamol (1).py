#!/usr/bin/env python
# coding: utf-8

# In[4]:


import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from datetime import datetime
import numpy as np
import pandas as pd


# In[5]:


spark = SparkSession.builder.appName("LondonCrime").getOrCreate()


# In[6]:


data = spark.read.format("CSV").option("header","true").load("london_crime_by_lsoa.csv")


# In[7]:


data.printSchema()


# In[8]:


data.count()


# In[9]:


data.limit(5).show()


# In[10]:


tot_borough = data.select("borough").distinct()


# In[11]:


tot_borough.show()


# In[12]:


hackney_data = data.filter(data["borough"] == "Hackney")
hackney_data.show(5)


# In[13]:


data_2015_2016 = data.filter(data["year"].isin(["2015"]))
data_2015_2016.count()


# In[15]:


data_2015_2016.select("lsoa_code" ,"borough","major_category","minor_category","year","month").coalesce(5).write.option("header","true").csv("amol_export_part.csv")


# In[16]:


data_2015_2016.createOrReplaceTempView('records')


# In[18]:


all_records = spark.sql('SELECT * FROM records')
all_records.show(5)


# In[19]:


spark.sql('SELECT * FROM records WHERE year = 2015 and month = 5').show()


# In[20]:


spark.sql('SELECT year , month , count(*) FROM records GROUP BY year , month').show()


# In[21]:


spark.sql('SELECT * FROM records WHERE year = 2015 and month = 5').coalesce(1).write.option("header","true").csv("amol_export_sql.csv")


# In[22]:


data_import = spark.read.format("CSV").option("header","true").load("amol_export_sql.csv")
data_import.count()


# In[23]:


data_import.show(30)


# In[24]:


result_pdf = data_import.select("*").toPandas()


# In[28]:


result_pdf


# In[42]:


result_pdf.iloc[0:5 , 2:4]


# In[55]:


result_pdf


# In[74]:


abc  = result_pdf.groupby('major_category').count()
plot_data = abc.iloc[:5 ,1:2]


# In[76]:


plot_data


# In[77]:


plot_data.plot.bar()


# In[ ]:




