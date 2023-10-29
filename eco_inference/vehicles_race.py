import numpy as np
import pyei
import pandas as pd
import matplotlib.pyplot as plt
import os

# HAVE TO USE if __name__=="__main__" for pymc3!!
# https://github.com/pymc-devs/pymc3/issues/3140
if __name__=="__main__":
	# os.environ['MKL_THREADING_LAYER'] = 'sequential'
	# os.environ['OMP_NUM_THREADS'] = '1'

	acs_data_dir 	= "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/data/census_pre_fetch"
	acs_data 		= f"{acs_data_dir}/Georgia_tract_ACS5.csv"
	acs_data_raw 	= f"{acs_data_dir}/Georgia_tract_ACS5_raw.csv"



	data = pd.read_csv(acs_data)

	data['x'] = data['Total_Black_or_African_American_alone']/data['Total']
	data['t'] = 1- data['Total_No_vehicle_available']/data['Total']
	data['n'] = data['Total']

	df = data[['x','t','n','GEO_ID']]
	df = df.loc[df['n'] > 0]
	df = df.loc[df['t'] > 0]
	df = df.loc[df['t'] < 1]
	df = df.loc[df['x'] > 0]
	df = df.loc[df['x'] < 1]
	df.dropna()

	X = df['x'].values
	T = df['t'].values
	N = df['n'].values
	precinct_names = df['GEO_ID'].values
	demographic_group_name = "Black"
	candidate_name = "Vehicle Owners"

	ei = pyei.TwoByTwoEI(model_name="king99_pareto_modification", pareto_scale=8, pareto_shape=2)
	ei.fit(X, T, N, demographic_group_name=demographic_group_name, candidate_name=candidate_name, precinct_names=precinct_names)
	# precinct_level_estimates() method of pyei.two_by_two.TwoByTwoEI instance
	#     If desired, we can return precinct-level estimates
	#     Returns:
	#         precinct_posterior_means: num_precincts x 2 (groups) x 2 (candidates)
	#         precinct_credible_intervals: num_precincts x 2 (groups) x 2 (candidates) x 2 (endpoints)
	ests = ei.precinct_level_estimates()[0]
	beta_b_i = [est[0][0] for est in ests]
	beta_w_i = [est[1][0] for est in ests]


	diffs = sorted(abs(beta_b_i- df['t']),reverse=True)

	print(ei.summary())
	# Model: king99_pareto_modification
	#         Computed from the raw b_i samples by multiplying by population and then getting
	#         the proportion of the total pop (total pop=summed across all districts):
	#         The posterior mean for the district-level voting preference of
	#         Black for Vehicle Owners is
	#         0.967
	#         The posterior mean for the district-level voting preference of
	#         non-Black for Vehicle Owners is
	#         0.991
	#         95% equal-tailed Bayesian credible interval for district-level voting preference of
	#         Black for Vehicle Owners is
	#         [0.96540644 0.96964487]
	#         95% equal-tailed Bayesian credible interval for district-level voting preference of
	#         non-Black for Vehicle Owners is
	#         [0.9903971  0.99192843]
