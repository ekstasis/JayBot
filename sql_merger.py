import sql_client as sc

source_table = 'Matches'
dest_table = 'merge_test'

qry = "SELECT * FROM "