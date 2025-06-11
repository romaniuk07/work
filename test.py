import os
import pandas as pd

file_path = r'C:\Users\hp\PycharmProjects\Work\aggregators\reports\appointments.csv'
df = pd.read_csv(file_path)
# for index,row in df.iterrows():
    # print(index)
    # print(row)
    # print("AppointmentInputDate of row:". row["AppointmentInputDate"] )


# print(df.tail)
# print(df.columns)
# print(df.index.tolist())

columns_list = df.columns.tolist()
# print(columns_list)

# for col in columns_list:
#     if len(col) <= 9:
#         print(f"Column: {col}")

# print(df.describe())
# print(df.nunique())

# print(df['AppointmentInputDate'].unique())
# print(df.size)


appointments = pd.read_csv('aggregators/reports/appointments.csv')
# print(appointments['AppointmentResource'])

# print(appointments.head())
# print(appointments.sample(5))
# print(appointments.loc[0])

# print(appointments.loc[5:9, ['AppointmentDate','AppointmentResource']])

# print(appointments.loc[:, [0,2]])

# print(appointments['AppointmentDuration'])

appointments.loc[1:3, "AppointmentDuration"] = 10
# print(appointments.loc[:, "AppointmentDuration"])

# print(appointments.iat[3,1])
print(appointments.AppointmentTypeCategory)