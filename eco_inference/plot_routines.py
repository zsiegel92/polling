import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize
import os

from plot_utilities import plotter


@plotter
def plot_eco_advantage_B_vs_X(df):
	splot = sns.scatterplot(data=df,x='X',y='eco_improvement2_B')
	# splot.set(xscale='log')

@plotter
def plot_eco_advantage_W_vs_X(df):
	splot = sns.scatterplot(data=df,x='X',y='eco_improvement2_W')
	# splot.set(xscale='log')

@plotter
def plot_eco_advantage_vs_estimation_difference_B(df):
	splot = sns.scatterplot(data=df,x='estimation_difference_B',y='eco_improvement2_B',hue='X',legend='brief')

@plotter
def plot_eco_advantage_vs_estimation_difference_W(df):
	splot = sns.scatterplot(data=df,x='estimation_difference_W',y='eco_improvement2_W',hue='X',legend='brief')

def plot_some(df):
	plot_eco_advantage_B_vs_X(df)
	plot_eco_advantage_W_vs_X(df)
	plot_eco_advantage_vs_estimation_difference_B(df)
	plot_eco_advantage_vs_estimation_difference_W(df)


# def obj_col_name(ground_truth, membership, use_estimates, eco):
# 	return f'obj_groundtruth_{ground_truth}_membership_{membership}_estimates_{use_estimates}_eco_{eco}'

def obj_col_name(ground_truth, membership, use_estimates, eco):
	return f'obj_gt_{"T" if ground_truth else "F"}_memb_{"N" if membership is None else membership}_est_{"T" if use_estimates else "F"}_eco_{"T" if eco else "F"}'
def gen_precinct_plots(precincts_df,folderextra = '',do_general_lineplots=False,do_general_ratio_plots=False,do_eco_ratio_plots=True):
	xfeatures = ('T', 'X', 'beta_B', 'beta_W','ratio','pct_error_beta_B_using_T', 'pct_error_beta_W_using_T', 'pct_error_beta_B', 'pct_error_beta_W', 'eco_improvement2_B', 'eco_improvement2_W', 'estimation_difference_B', 'estimation_difference_W')

	if do_general_lineplots:
		folder = f"figures/{folderextra}floc_objectives_lineplot"
		if not os.path.isdir(folder):
			os.mkdir(folder)
		for xfeature in xfeatures:
			fig,ax = plt.subplots(figsize=(20,20))
			if xfeature in ('ratio',):
				ax.set(xscale='log')
			for use_estimates in (False,): #(True,False)
				for eco in (True,False):
					for membership in (0,1,None):
						for ground_truth in (True,): #(True, False)
							col_name = obj_col_name(ground_truth,membership, use_estimates,eco)
							sns.lineplot(data=precincts_df,x=xfeature,y=col_name,label=col_name,ax=ax)
			ax.set_title(f'Objectives vs. {xfeature}')
			plt.savefig(f'{folder}/objectives_vs_{xfeature}.pdf')
			plt.clf()
			plt.close('all')
			# plt.show(block=False)

	if do_general_ratio_plots:
		objective_colnames = []
		for use_estimates in (False,): #(True,False)
			for eco in (True,False):
				for membership in (0,1,None):
					for ground_truth in (True,): #(True, False)
						col_name = obj_col_name(ground_truth,membership, use_estimates,eco)
						objective_colnames.append(col_name)

		folder = f"figures/{folderextra}floc_objectives_ratios"
		if not os.path.isdir(folder):
			os.mkdir(folder)
		for i in range(len(objective_colnames)):
			for j in range(i+1,len(objective_colnames)):
				col1 = objective_colnames[i]
				col2 = objective_colnames[j]
				for xfeature in xfeatures:
					fig,ax = plt.subplots(figsize=(20,20))
					sns.lineplot(x=precincts_df[xfeature],y=precincts_df[col1]/precincts_df[col2],ax=ax)
					ax.set_title(f'{col1}  /  {col2} vs. {xfeature}\nMean Ratio: {(precincts_df[col1]/precincts_df[col2]).mean()}')
					if xfeature in ('ratio',):
						ax.set(xscale='log')
					plt.savefig(f'{folder}/ratio_{col1}__{col2}_vs_{xfeature}.pdf')
					plt.clf()
					plt.close('all')

	if do_eco_ratio_plots:
		folder = f"figures/{folderextra}floc_objectives_ratios_eco_vs_not"
		if not os.path.isdir(folder):
			os.mkdir(folder)
		for use_estimates in (False,): #(True,False)
			for membership in (0,1,None):
				for ground_truth in (True,): #(True, False)
					col1 = obj_col_name(ground_truth,membership, use_estimates,eco=False)
					col2 = obj_col_name(ground_truth,membership, use_estimates,eco=True)
					for xfeature in xfeatures:
						fig,ax = plt.subplots(figsize=(20,20))
						sns.lineplot(x=precincts_df[xfeature],y=precincts_df[col1]/precincts_df[col2],ax=ax)
						ax.set_title(f'{col1}  /  {col2} vs. {xfeature}\nMean Ratio: {(precincts_df[col1]/precincts_df[col2]).mean()}',fontdict=dict(fontsize=15))
						if xfeature in ('ratio',):
							ax.set(xscale='log')
						plt.savefig(f'{folder}/ratio_{col1}__{col2}_vs_{xfeature}.pdf')
						plt.clf()
						plt.close('all')
