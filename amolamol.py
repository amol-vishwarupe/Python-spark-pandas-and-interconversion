#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
from pyspark.sql import SparkSession


# In[ ]:


spark = SparkSession.builder.appName("LondonCrime").getOrCreate()


# In[3]:


data = spark.read.format("CSV").option("header","true").load("london_crime_by_lsoa.csv")


# In[4]:


data.printSchema()


# In[5]:


data.count()


# In[6]:


data.limit(5).show()


# In[7]:


tot_borough = data.select("borough").distinct()


# In[8]:


tot_borough.show()


# In[9]:


hackney_data = data.filter(data["borough"] == "Hackney")
hackney_data.show(5)


# In[18]:


data_2015_2016 = data.filter(data["year"].isin(["2015"]))
data_2015_2016.count()


# In[20]:


data_2015_2016.select("lsoa_code" ,"borough","major_category","minor_category","year","month").coalesce(5).write.option("header","true").csv("amol_export_part.csv")


# In[ ]:




