import pandas as pd

# Завантаж обидва файли
input_df = pd.read_csv("input.csv")
db_df = pd.read_csv("db_export.csv")

# Якщо потрібно, видали зайві пробіли/перетвори типи
input_df = input_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
db_df = db_df.applymap(lambda x: x.strip() if isinstance(x, str) else x)

# Пошук рядків, яких немає у базі
not_loaded_df = pd.concat([input_df, db_df, db_df]).drop_duplicates(keep=False)

# Збереження результату
not_loaded_df.to_csv("not_loaded.csv", index=False)

print("Готово! Рядки, які не потрапили в базу, збережено у not_loaded.csv")
