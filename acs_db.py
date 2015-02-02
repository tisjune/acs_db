import os, sqlite3, sys, settings, csv2sqlite, json, csv

# initializes db and extra schema info
conn = sqlite3.connect(settings.db_path)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()
with open(settings.extra_schema_path,'r') as f:
	extra_schema = json.load(f)
table_membership = {}
for table, cols in extra_schema.iteritems():
	for col in cols:
		table_membership[col] = table

def find_fields(field_list):
	# uhhhh some error checking goes here maybe
	# unless that can be done on the frontend
	field_and_table = []
	for field in field_list:
		table = table_membership.get(field,None)
		if table:
			field_and_table.append((field,table))
		else:
			field_and_table.append((field,field[:6]))
	return field_and_table

def get_join_by_zip_query(field_and_table):
	# somehow I don't feel like this is the best way but ok

	
	tables_involved = list(set([t for _,t in field_and_table]))

	join_statement = ''
	for table in tables_involved:
		join_statement += ' left join %s using(zip)' % (table)

	from_statement = 'input_csv %s' % (join_statement)
	select_statement = 'input_csv.*, %s ' % (','.join([t+'.'+f for f,t in field_and_table]))

	query_statement = "select %s from %s" % (select_statement, from_statement)
	
	return query_statement

def join_by_zip(input_csv, field_list):
	field_and_table = find_fields(field_list)
	query = get_join_by_zip_query(field_and_table)

	print 'hi'
	csv2sqlite.convert(input_csv,settings.db_path,table='input_csv')
	join_result = cursor.execute(query)
	row_one = join_result.fetchone()
	with open('tmp/tmp.csv','w') as f:
		writer = csv.writer(f)
		writer.writerow(row_one.keys())
		writer.writerow(list(row_one))
		result = list(join_result)
		for r in result:
			writer.writerow(list(r))
	cursor.execute('drop table if exists input_csv')
	print 'here'
	return 0

