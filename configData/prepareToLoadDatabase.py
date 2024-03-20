import pandas as pd 

# Load the data
df = pd.read_csv('2024/01/csv/log1.csv')

y = []

for i in range(len(df)):
  x = tuple(df.iloc[i])
  y.append(x)
  
with open('log1.txt', 'w') as f:
  for item in y:
    f.write(str(item) + ',' + '\n')