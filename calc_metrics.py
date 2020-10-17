#
# Use pandas data frame to simulate SQL calculations cause it has group by
#
# select avg(price), (sum(rating_five_count) / sum(rating_count)) * 100 as five_percentage, origin_country
# from summer_products
# group by origin_country
# order by origin_country

import pandas as pd

# Read subset of columns to use less memory
df = pd.read_csv("test-task_dataset_summer_products.csv",
                 index_col=['origin_country'],
                 usecols=['origin_country','price','rating_five_count','rating_count']
                 )

print('\n',df.head(5), df.shape)

# Do we have NaNs?
print('price', df['price'].isnull().sum())
print('rating_five_count', df['rating_five_count'].isnull().sum())
print('rating_count', df['rating_count'].isnull().sum())

df_result = df.groupby('origin_country', dropna=False).agg(
    avg_price=('price', 'mean'),
    rating_five_count=('rating_five_count', 'sum'),
    rating_count=('rating_count', 'sum')
)

# We possibly may have Division by Zero Error using rating_count
# but division on data frame's columns returns NaN which is kind of NULL in SQL
print('\n',df_result)

df_result['five_percentage'] = (df_result['rating_five_count'] / df_result['rating_count']) * 100
df_result.reset_index(inplace=True)
df_result = df_result.reindex(columns=['avg_price', 'five_percentage', 'origin_country'])

print('\n','Result table\n',df_result)
