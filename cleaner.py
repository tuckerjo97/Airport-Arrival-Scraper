import pandas as pd

df = pd.read_csv("airports.csv")
ap = ['medium_airport', "large_ariport"]
df = df[df['type'].isin(ap)]
df = df.loc[:, ['ident', 'municipality']]
df.columns = ['id', 'city']
df.to_csv('test.csv')