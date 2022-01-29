
# Standard imports
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import ssl
ssl._create_default_https_context = ssl._create_unverified_context


# First we Import the Data

# Save filespaths as variables for easier access
#data_filepath = './data/data.csv'
data_filepath = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/msas.csv'

# read the data and store data in a DataFrame titled df
msas_csv_url = 'https://raw.githubusercontent.com/ivan-sepulveda/prometheus_insights/master/msas.csv'
msas_df = pd.read_csv(data_filepath, sep=",")
msas_df_records = msas_df.to_dict('records')
msas = {record['msa']: record['search'] for record in msas_df.to_dict('records')}

print(msas)
#msa_ids = msas_df['id'].to_list()


#single_row = msas_df[msas_df['id'] == 1]
#print(single_row.values.tolist())

#for msa_id in msa_ids:

 #   print("\n\nid: {0}\nmsa: {1}\nsearch: {2}".format(1, 1, 1))

