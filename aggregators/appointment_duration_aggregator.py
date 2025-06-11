import os
import pandas as pd
import reports

folder_path = "reports"

for file_name in os.listdir(folder_path):
    if file_name.endswith(".csv"):
        file_path = os.path.join(folder_path, file_name)
        df = pd.read_csv(file_path)

        if "AppointmentDuration" in df.columns:
            df["AppointmentDuration"] = pd.to_numeric(df["AppointmentDuration"], errors="coerce").fillna(0)
            total = df["AppointmentDuration"].sum()
            row_count = len(df)  # Кількість рядків без заголовка
            print(f"{file_name}: Сума = {total}, Рядків = {row_count}")
        else:
            print(f"{file_name}: Колонка 'AppointmentDuration' не знайдена")



file_path = 'reports/appointments.csv'
df = pd.read_csv(file_path)
file_path.head()