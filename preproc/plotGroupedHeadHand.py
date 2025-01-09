import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

# Load the data from the CSV file
# Load the dataset
file_path = 'df_Global_block.csv'
df = pd.read_csv(file_path)

# Set the aesthetic style of the plots
sns.set(style="whitegrid")

# Create a scatter plot with regplot overlay for each GlobalBlock category
plt.figure(figsize=(10, 6))

# Loop through each category in GlobalBlock and plot
for block in df['GlobalBlock'].unique():
    subset = df[df['GlobalBlock'] == block]
    sns.scatterplot(x=subset['AppTimeCorr'], y=subset['head_Hand_dist'], label=f'Block {block}')
    sns.regplot(x=subset['AppTimeCorr'], y=subset['head_Hand_dist'], scatter=False, label=f'Reg. Line {block}')

plt.xlabel('AppTimeCorr')
plt.ylabel('Head-Hand Distance')
plt.title('Scatter Plot of Head-Hand Distance over Time Grouped by GlobalBlock')
plt.legend()

# Show the plot
plt.savefig("grouped_headHandOverTime.png")

import statsmodels.api as sm

# Define the independent variable (AppTimeCorr) and the dependent variable (head_Hand_dist)
X = df['AppTimeCorr']
y = df['head_Hand_dist']

# Add a constant term to the independent variable for the regression intercept
X = sm.add_constant(X)

# Perform the linear regression
model = sm.OLS(y, X).fit()

# Display the regression results
print(model.summary())


import matplotlib.pyplot as plt
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf

# Rolling statistics
df['Rolling Mean'] = df['head_Hand_dist'].rolling(window=12).mean()
df['Rolling Std'] = df['head_Hand_dist'].rolling(window=12).std()

# Plot rolling statistics
plt.figure(figsize=(14, 6))
plt.plot(df['AppTimeCorr'], df['head_Hand_dist'], label='Original')
plt.plot(df['AppTimeCorr'], df['Rolling Mean'], label='Rolling Mean', color='red')
plt.plot(df['AppTimeCorr'], df['Rolling Std'], label='Rolling Std', color='black')
plt.xlabel('AppTimeCorr')
plt.ylabel('Head-Hand Distance')
plt.title('Rolling Mean & Standard Deviation')
plt.legend()
plt.savefig('rollingStats.png')

# ACF and PACF plots
plt.figure(figsize=(14, 6))
plt.subplot(121)
plot_acf(df['head_Hand_dist'], lags=40, ax=plt.gca())
plt.title('Autocorrelation Function')

plt.subplot(122)
plot_pacf(df['head_Hand_dist'], lags=40, ax=plt.gca())
plt.title('Partial Autocorrelation Function')

plt.savefig('autocorrelation.png')


from statsmodels.tsa.stattools import adfuller

# Perform the Augmented Dickey-Fuller test
result = adfuller(df['head_Hand_dist'])

print('ADF Statistic:', result[0])
print('p-value:', result[1])
print('Critical Values:', result[4])

# Plot ADF test statistic and critical values
plt.figure(figsize=(10, 6))
plt.axhline(y=result[4]['1%'], color='r', linestyle='--', label='1% Critical Value')
plt.axhline(y=result[4]['5%'], color='g', linestyle='--', label='5% Critical Value')
plt.axhline(y=result[4]['10%'], color='b', linestyle='--', label='10% Critical Value')
plt.plot([0, 1], [result[0], result[0]], label='ADF Test Statistic', color='black')
plt.title('ADF Test Statistic vs Critical Values')
plt.legend()
plt.savefig('adfStat.png')

