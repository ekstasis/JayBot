import pandas as pd
import sql_client as db

qry = "select trade_id, date, time, maker_side, size, price from matches "
# qry = "select top 1000 * from matches"
result = db.do_query(qry)

df = pd.DataFrame(result)
df.set_index('trade_id', drop=True, inplace=True)
