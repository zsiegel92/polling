import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import numpy.polynomial as pol

data_path = "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/data/census_pre_fetch/Georgia_tract_ACS5.csv"

df = pd.read_csv(data_path)


census_variables = dict(zip(df.columns,pd.read_csv("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/data/census_pre_fetch/Georgia_tract_ACS5_raw.csv").columns))
# https://api.census.gov/data/2018/acs/acs5/groups/B08015.html
# https://api.census.gov/data/2019/acs/acs5/groups/B08006.html
# https://api.census.gov/data/2019/acs/acs5/groups/B25045.html
# https://api.census.gov/data/2019/acs/acs5/groups/B08014.html
vehicle_cols = [
	'Total_No_vehicle_available',
	'Total_1_vehicle_available',
	'Total_2_vehicles_available',
	'Total_3_vehicles_available',
	'Total_4_vehicles_available',
	'Total_5_or_more_vehicles_available',
	'Aggregate_number_of_vehicles_car,_truck,_o',
	'Total_Car,_truck,_or_van_',
	'Total_Public_transportation_excluding_taxi',
	'Total_Bicycle',
	'Total_Walked',
	'Total_Taxicab,_motorcycle,_or_other_means',
	'Total_Worked_from_home',
]


pop_cols = [
	'Total',
	'Median_household_income_in_the_past_12_mon',
	'Total_White_alone',
	'Total_Black_or_African_American_alone',
	'Total_American_Indian_and_Alaska_Native_al',
	'Total_Asian_alone',
	'Total_Native_Hawaiian_and_Other_Pacific_Is',
	'Total_Some_other_race_alone',
]

df = df.loc[df['Median_household_income_in_the_past_12_mon'] > 0]

x = df['Total_Black_or_African_American_alone']/df['Total']
# y = df['Total_No_vehicle_available']/df['Total'] # increasing
# y = df['Aggregate_number_of_vehicles_car,_truck,_o']/df['Total'] # decreasing

# y = df['Median_household_income_in_the_past_12_mon']
# y = df['Total_1_vehicle_available']/df['Total']
# y = df['Total_2_vehicles_available']/df['Total']
# y = df['Total_Car,_truck,_or_van_']/df['Total']
y = df['Total_Public_transportation_excluding_taxi']/df['Total']

nonnan = ~np.isnan(y)
x = x[nonnan]
y = y[nonnan]



b,m = np.polynomial.polynomial.polyfit(x,y,1)
# plt.scatter(x, y)
# plt.scatter(x, y)
# plt.scatter(x, y)
plt.scatter(x, y)
plt.plot(x, m * x + b,color="orange")
# plt.scatter(x, y)
plt.show(block=False)
