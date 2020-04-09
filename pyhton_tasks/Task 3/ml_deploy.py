import pandas as pd
import sqlalchemy as db
import psycopg2
import h5py
from keras.models import load_model
from keras.models import model_from_json
import os;

# --------------------------- change working directory ---------------------------------------
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# --------------------------- create connection with the database ----------------------------
con = db.create_engine('postgresql://abdo:abdo@localhost:5432/task3_db')

query = """
select pregnancies , glucose , bloodpressure , skinthickness , insulin , bmi , diabetespedigreefunction , age
from public."diabetes_unscored"
Except
select pregnancies , glucose , bloodpressure , skinthickness , insulin , bmi , diabetespedigreefunction , age
from public."diabetes_scored" ;
"""
# --------------------------- load the unscored table -----------------------
transform_diabetes_df = pd.read_sql(query, con)

# --------------------------- load model architucture -----------------------

json_file = open('model.json', 'r')
model_json = json_file.read()
json_file.close()
loaded_model = model_from_json(model_json)

# -------------------------- load model weights -----------------------
loaded_model.load_weights("model.h5")

# ------------------------- make the prediction ----------------------
predictions = loaded_model.predict(transform_diabetes_df)

# ------------------------- round predictions to 1 or 0 ------------------
rounded = [int(round(x[0])) for x in predictions]

# ----------------------- add prediction to data frame
transform_diabetes_df['outcome'] = rounded

# ----------------------- append new scored recordes to db ----------------
transform_diabetes_df.to_sql(name = 'diabetes_scored', con=con, schema = 'public', if_exists='append', index=False)

print('the number of new scored rows is >>>>> {} <<<<<'.format(len(transform_diabetes_df.index)))

