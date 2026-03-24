import duckdb

con = duckdb.connect("data/warehouse/test.db")

con.execute("CREATE TABLE t (id INT)")
con.execute("INSERT INTO t VALUES (1)")
print(con.execute("SELECT * FROM t").fetchall())

con.close()