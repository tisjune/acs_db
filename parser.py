import csv, numpy as np, re, json
import pandas as pd # the best
from collections import defaultdict 

'''
	sort of hacky attempt to organize the ACS++ database.

	For ACS variables (as found in the codebook), I made a separate table
		for each topic with columns = all the variables belonging to the topic.
		Each table has zip code as primary key.

	For other variables that randomly show up, I define a few extra tables, found in 
		extra_schema.json.

	Details: The entire csv file is loaded into a pandas dataframe. We then spit out:
		- a script to create the database (script_out/create.sql)
		- csv files containing all the split up data (data_out/*.csv)
		- a script to load the data into the database (and handle nulls) (script_out/load.txt)

	Your computer will probably work hard for a few minutes. 

	Some issues:
		Sometimes pandas infers weird datatypes. Lots of reals which are actually ints. 
		This...shouldn't be a life-threatening issue but it might make consequent
			.csv outputs look strange.
'''

DATAFILE = "data/MasterDatabase_ZipCountyCrosswalk_Oct6.csv"
#DATAFILE = "data/acsdb_mini.csv"
EXTRA_SCHEMA_FILE = "data/extra_schema.json"
ACS_RE = r"[B|C]\d{5}"
PD_DTYPE_DICT = {
		'int64': 'integer',
		'float64': 'real',
		'object': 'text',
	}

if __name__ == '__main__':

	print 'read stuff'
	with open(DATAFILE,'rb') as acs_csv:
		acs_db = pd.DataFrame.from_csv(acs_csv)
	acs_db.set_index('zip',inplace=True)
	with open(EXTRA_SCHEMA_FILE, 'r') as f:
		extra_schema = json.load(f)

	print 'parse column headings'
	db_col_names = acs_db.columns

	table_name_dict = defaultdict(list)
	for i,name in enumerate(db_col_names):
		m = re.match(ACS_RE, name)
		if m:
			table_name_dict[name[:6]].append(name)
	for table, table_cols in extra_schema.iteritems():
		table_name_dict[table] = table_cols

	print 'write create.sql'
	with open('script_out/create.sql','w') as f:
		for table, cols in table_name_dict.iteritems():
			f.write('drop table if exists %s;\n' % (table))
			f.write('create table %s (\n'% (table))
			f.write('\tzip text primary key,\n')
			num_cols = len(cols)
			for i in xrange(num_cols):
				sqlite_type = PD_DTYPE_DICT[str(acs_db[cols[i]].dtype)]
				f.write('\t\t%s %s' % (cols[i],sqlite_type))
				if i < num_cols - 1:
					f.write(',\n')
			f.write(');\n')

	print 'dump data'
	for table, cols in table_name_dict.iteritems():
		subtable_file = 'data_out/%s.csv' % (table)
		with open(subtable_file,'w') as f:
			acs_db[cols].to_csv(f,na_rep='null')

	with open('script_out/load.txt','w') as f:
		f.write('.mode csv\n')
		for table, cols in table_name_dict.iteritems():
			f.write('.import data_out/%s.csv %s\n' %(table,table))
			for col in cols:
				f.write('update %s set %s=null where %s="null";\n' % (table, col, col))
			f.write('\n')
