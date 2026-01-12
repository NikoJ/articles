from core.logical_expr import col, lit

predicate = (col("subs") >= lit(60)) & (col("name") == "Niko")
print(predicate)
# ((#subs >= 60) AND (#name = 'Niko'))

expr = (col("price") * 1.5) + 5
print(expr)
# ((#price * 1.5) + 5)
