# -*- coding: utf-8 -*-
"""
Created on Mon Jan 08 2018

@author: Dr. J. Andrew Howe

blah
"""


import psycopg2 as postgres
import pandas as pd
import pickle
import os

# impute all?
impute = True

''' Connect to the database '''
db_source = 'dataport'
#db_source = 'kapsarc'

if db_source == 'dataport':
	# Dataport connect data:
	dp_base = "postgres"
	dp_host = "dataport.cloud"
	dp_port = 5434
	dp_user = "mHYl5sJ5Xavb"
	dp_pwrd = ""
	db_schema = "commercial"
elif db_source == 'kapsarc':
	# kapsarc copy connect data
	dp_base = "pecan_raw"
	dp_host = "helium-63.test.kapsarc"
	dp_port = 5432
	dp_user = "howej"
	dp_pwrd = ""
	db_schema = "public"

''' Acquire the Data '''
# connect & open a cursor
conn = postgres.connect(database = dp_base, user = dp_user,
	password = dp_pwrd, host = dp_host, port = dp_port)

# get the survey data
survSQL = open(os.getcwd()+os.sep+"2014survey_prep.sql",'rt').read()
survSQL = survSQL.replace("SCHEMA",db_schema)
surveys = pd.read_sql_query(survSQL, conn, coerce_float=False, index_col = ["dataid"])
conn.close()

''' data preparation '''
# impute missing values for thermostat settings: fill with the median values
temps = ['temp_summer_weekday_workday_p','temp_summer_weekday_morning_p',
       'temp_summer_weekday_evening_p', 'temp_summer_sleeping_hours_hours_p',
       'temp_summer_weekend_hours_p', 'temp_winter_weekday_workday_p',
       'temp_winter_weekday_morning_p', 'temp_winter_weekday_evening_p',
       'temp_winter_sleeping_hours_hours_p', 'temp_winter_weekend_hours_p']
for t in temps:
    surveys[t].fillna(surveys[t].median(),inplace=True)

# impute for categoricals, maybe - must either impute or drop,
# because continued support for NaNs in categorical is not guaranteed
if impute:
	surveys.foundation_type_p.fillna(surveys.foundation_type_p.mode().iloc[0], inplace=True) # fill with the mode
	surveys.income_level_p.fillna(surveys.income_level_p.mode().iloc[0], inplace=True) # fill with the mode
	surveys.programmed_p.fillna(surveys.programmed_p.mode().iloc[0], inplace=True) # fill with the mode
	surveys.pv_satisfied_p.fillna(3, inplace=True) # fill with 3 = Neutral
else:
	surveys.dropna(inplace = True)

# make some of the variables categorical
categors = ['foundation_type_p','education_level_p','income_level_p','pv_satisfied_p','hvac_p']
surveys.foundation_type_p = pd.Categorical(surveys.foundation_type_p,surveys.foundation_type_p.unique())
surveys.education_level_p = pd.Categorical(surveys.education_level_p,surveys.education_level_p.unique())
surveys.income_level_p = pd.Categorical(surveys.income_level_p,surveys.income_level_p.unique())
surveys.pv_satisfied_p = pd.Categorical(surveys.pv_satisfied_p,surveys.pv_satisfied_p.unique())
surveys.hvac_p = pd.Categorical(surveys.hvac_p,surveys.hvac_p.unique())
# now use one-hot-encoding on the categorical columns
surveysOHE = pd.get_dummies(surveys[categors])

''' finally, put it all together '''
# join
surveydata = surveys.join(surveysOHE,how='inner')
# save
pickle.dump(surveydata,open(os.getcwd()+os.sep+'survey_data.p','wb'))
