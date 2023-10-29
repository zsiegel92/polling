import numpy as np
from scipy.special import binom
from numpy import floor
import pandas as pd
import pyei
from itertools import product
import seaborn as sns
import matplotlib.pyplot as plt
from matplotlib.colors import LogNorm, Normalize
import pyomo.environ as pyo
import pao
import os

# from scipy.stats import truncnorm
from trun_mvnt import rtmvn, rtmvt

from plot_utilities import plotter
from pyRserve_utilities import start_rserve_daemon, do_eco_inf, stop_rserve, create_df, conn, REvalError
import counting_utils
from counting_utils import rand, random_choices, random_choice, random_binary_choice
from stochastic_FLOC import stochastic_FLOC_model, set_bilevel, set_second_level
from robust_FLOC import robust_FLOC_model, set_bilevel_robust, set_second_level_robust
from pyomo_utils import solve_pyomo
from process_results import Solution
from plot_routines import plot_eco_advantage_B_vs_X, plot_eco_advantage_W_vs_X, plot_eco_advantage_vs_estimation_difference_B, plot_eco_advantage_vs_estimation_difference_W, plot_some, gen_precinct_plots


# import importlib
# import stochastic_FLOC
# import plot_routines
# importlib.reload(plot_routines)
# from stochastic_FLOC import stochastic_FLOC_model
def get_precincts_df(nPerPrecinct, nPrecincts, Bb, Bw, sigma_b, sigma_w, sigma_bw):
	Sigma = np.array([[sigma_b, sigma_bw], [sigma_bw, sigma_w]])
	reqPrecision = int(np.ceil(np.log10(nPerPrecinct)))
	betas = rtmvn(
		n=nPrecincts,
		Mean=np.array([Bb, Bw]),
		Sigma=Sigma,
		D=np.eye(2),
		lower=np.array([0, 0]),
		upper=np.array([1, 1]),
		burn=10,
		thin=1,
		ini=[]
		)
	nErrors = 0
	while nErrors < 10:
		try:
			betas = np.around(betas, decimals=reqPrecision) # car ownership given race
			X = np.around(
				np.random.rand(nPrecincts), decimals=reqPrecision
				) # eXplanatory variable (race)
			T = [beta_B*x + beta_W * (1-x) for x, (beta_B, beta_W) in zip(X, betas)]
			df = pd.DataFrame(
				data=dict(
					T=T,
					X=X,
					beta_B=[beta_B for (beta_B, beta_W) in betas],
					beta_W=[beta_W for (beta_B, beta_W) in betas]
					)
				)
			df['N'] = nPerPrecinct
			df['nScenarios_agg'] = np.array([
				binom(nPerPrecinct, floor(t * nPerPrecinct)) for t in T
				])
			df['nScenarios_eco'] = np.array([
				binom(floor(x * nPerPrecinct), floor(beta_B * x * nPerPrecinct)) *
				binom(floor((1-x) * nPerPrecinct), floor(beta_W * (1-x) * nPerPrecinct))
				for x, (beta_B, beta_W) in zip(X, betas)
				])
			df['ratio'] = df['nScenarios_agg'] / df['nScenarios_eco']
			N = df['N'].values
			df['pct_error_beta_B_using_T'] = (df['beta_B'] - df['T']) / df['beta_B']
			df['pct_error_beta_W_using_T'] = (df['beta_W'] - df['T']) / df['beta_W']
			add_eco_inference_rserve(df)
			break
		except REvalError:
			nErrors += 1
			print(f"Had {nErrors} ERRORS WITH ECO INFERENCE")
	if nErrors >= 10:
		raise Exception(
			"Attempted ecological inference 10 times with random parameters, failed every time. Try debugging."
			)
	print("Done with dataframe construction and ecological inference")
	print(
		df[[
			'T',
			'X',
			'beta_B',
			'beta_B_est',
			'beta_W',
			'beta_W_est',
			'pct_error_beta_B_using_T',
			'pct_error_beta_B',
			'pct_error_beta_W_using_T',
			'pct_error_beta_W',
			# 'abs_error_beta_B','abs_error_beta_W',
			'eco_improvement2_B',
			'eco_improvement2_W'
			]]
		)
	avg_error_beta_B = df.pct_error_beta_B.abs().mean()
	avg_error_beta_B_using_T = df.pct_error_beta_B_using_T.abs().mean()
	avg_error_beta_W = df.pct_error_beta_W.abs().mean()
	avg_error_beta_W_using_T = df.pct_error_beta_W_using_T.abs().mean()
	print(
		f"\n{avg_error_beta_B=:.3}, {avg_error_beta_B_using_T=:.3}, {avg_error_beta_W=:.3}, {avg_error_beta_W_using_T=:.3}\n"
		)
	df['x_BT'] = df['N'] * df['X'] * df['beta_B']
	df['x_WT'] = df['N'] * (1 - df['X']) * df['beta_W']
	df['x_BTnot'] = df['N'] * df['X'] * (1 - df['beta_B'])
	df['x_WTnot'] = df['N'] * df['X'] * (1 - df['beta_W'])
	return df


def add_eco_inference_rserve(df):
	# start_rserve_daemon()
	bb_out = do_eco_inf(df)
	df['beta_B_est'] = bb_out['betab']
	df['beta_W_est'] = bb_out['betaw']

	df['abs_error_beta_B'] = (df['beta_B_est'] - df['beta_B'])
	df['abs_error_beta_W'] = (df['beta_W_est'] - df['beta_W'])
	df['pct_error_beta_B'] = (df['beta_B_est'] - df['beta_B']) / df['beta_B']
	df['pct_error_beta_W'] = (df['beta_W_est'] - df['beta_W']) / df['beta_W']
	# df['eco_improvement_B'] = abs(df['pct_error_beta_B_using_T']) - abs(df['pct_error_beta_B'])
	# df['eco_improvement_W'] = abs(df['pct_error_beta_W_using_T']) - abs(df['pct_error_beta_W'])

	df['eco_improvement2_B'] = (abs(df['T'] - df['beta_B']) -
								abs(df['beta_B_est'] - df['beta_B'])) / abs(df['beta_B'])
	df['eco_improvement2_W'] = (abs(df['T'] - df['beta_W']) -
								abs(df['beta_W_est'] - df['beta_W'])) / abs(df['beta_W'])

	df['estimation_difference_B'] = abs(df['beta_B_est'] - df['T'])
	df['estimation_difference_W'] = abs(df['beta_W_est'] - df['T'])
	# return df

	df['x_BT_est'] = df['N'] * df['X'] * df['beta_B_est']
	df['x_WT_est'] = df['N'] * (1 - df['X']) * df['beta_W_est']
	df['x_BTnot_est'] = df['N'] * df['X'] * (1 - df['beta_B_est'])
	df['x_WTnot_est'] = df['N'] * df['X'] * (1 - df['beta_W_est'])
	# stop_rserve()


# beta_B and beta_W can be real parameters (to generate ground-truth) or estimates
def gen_scenarios(n, x, beta_B, beta_W):
	t = beta_B*x + beta_W * (1-x)
	n_BT = int(beta_B * x * n)
	n_WT = int(beta_W * x * n)
	random_choices(n, [beta_B * n, beta_W * n])
	row = df.loc[0]
	car_owner_scenario = random_choices(
		row.N, [row.beta_B * row.X * row.N, row.beta_W * (1 - row.X) * row.N]
		)
	for i in range(20):
		row = df.loc[i]
		non_car_owner_scenario = random_choices(
			row.N, [(1 - row.beta_B) * row.X * row.N, (1 - row.beta_W) * (1 - row.X) * row.N]
			)

		[len(l) for l in car_owner_scenario]
		[len(l) for l in non_car_owner_scenario]


def get_random_distmat(m, n, multiplier=1):
	mloc = rand.random((m, 2))
	nloc = rand.random((n, 2))
	return multiplier * np.array([[
		np.sqrt((mloc[i, 0] - nloc[j, 0])**2 + (mloc[i, 1] - nloc[j, 1])**2) for j in range(n)
		] for i in range(m)]), mloc, nloc


def get_solution(a_model, solver="gurobi"):
	optGap = {
		"baron": .05, # 0.10,
		"gurobi": 0.05,
		"multilevel": 0.05 # placeholder
		}[solver.lower()]
	multistart = False
	multistart_kwargs = dict(
		iterations=-1, HCS_max_iterations=20
		) #-1-> high confidence stopping. Default 10
	max_time = None
	solve_from_binary = False
	solver_params = dict(
		solver=solver,
		tee=False,
		keepfiles=True,
		warmstart=True,
		solve_from_binary=solve_from_binary,
		optGap=optGap,
		max_time=max_time,
		multistart=multistart,
		multistart_kwargs=multistart_kwargs
		)
	sol = Solution(*solve_pyomo(a_model, **solver_params))
	return sol


def obj_col_name(ground_truth, membership, use_estimates, eco):
	return f'obj_gt_{"T" if ground_truth else "F"}_memb_{"N" if membership is None else membership}_est_{"T" if use_estimates else "F"}_eco_{"T" if eco else "F"}'


def get_model(
	precinct,
	nFacilities=20,
	nFacilities_selected=4,
	J0_size=0,
	use_estimates=False,
	d0=None,
	eco=True,
	nScenarios=30,
	robust=False
	):
	m = int(precinct['N'])
	X = precinct['X']
	beta_B = precinct['beta_B']
	beta_W = precinct['beta_W']
	beta_B_est = precinct['beta_B_est']
	beta_W_est = precinct['beta_W_est']
	T = beta_B*X + beta_W * (1-X)
	T_est = beta_B_est*X + beta_W_est * (1-X)

	X_membership = random_binary_choice(m, X * m)
	X_inds = np.nonzero(X_membership)[0] # has length roughly X*m
	non_X_inds = np.nonzero(np.array(X_membership) == 0)[0] # has length roughly (1-X)*m
	T_B_ground_truth = random_choice(X_inds, beta_B * len(X_inds))
	T_W_ground_truth = random_choice(non_X_inds, beta_W * len(non_X_inds))
	T_ground_truth = np.zeros(m)
	T_ground_truth[T_B_ground_truth.tolist() + T_W_ground_truth.tolist()] = 1

	d_low = 1
	d_high = 3
	if d0 is None:
		d0 = get_random_distmat(m, nFacilities)

	capacities = [
		int(m / nFacilities_selected) + 1 for j in range(nFacilities)
		] #even loading capacities

	if robust:
		model_fn = robust_FLOC_model
		model_kwargs = {}
	else:
		model_fn = stochastic_FLOC_model
		model_kwargs = dict(nScenarios=nScenarios)

	model = model_fn(
		X_membership,
		beta_B,
		beta_W,
		d_low,
		d_high,
		d0,
		nFacilities,
		nFacilities_selected,
		capacities,
		T_ground_truth,
		J0=[i for i in range(J0_size)],
		eco=eco,
		**model_kwargs
		)
	return model


def get_objective(model, ground_truth=False, only_for_group=None):
	if ground_truth:
		if only_for_group is not None:
			return pyo.value(model.group_ground_truth_objectives[only_for_group])
		return pyo.value(model.ground_truth_objective)
	if only_for_group is not None:
		return pyo.value(model.group_objectives[only_for_group])
	return pyo.value(model.obj)


def get_ground_truth_assignments(model, solver="gurobi"):
	set_second_level(model)
	sol = get_solution(model, solver=solver)
	xVals = np.zeros((len(model.i), len(model.j)))
	for i in model.i:
		for j in model.j:
			xVals[i, j] = sol.model.x[i, j, 0].value
	set_bilevel(model)
	return xVals


def optimize_many_precincts(
	savefile_extra='',
	nPerPrecinct=50,
	nPrecincts=30,
	nFacilities=10,
	nFacilities_selected=2,
	nSimulations_per_precinct=3,
	Bb=0.5,
	Bw=0.95,
	nScenarios=30,
	robust=False,
	solver="gurobi"
	):
	precincts_df = get_precincts_df(
		nPerPrecinct=nPerPrecinct,
		nPrecincts=max(nPrecincts, 6),
		Bb=Bb,
		Bw=Bw,
		sigma_b=0.01,
		sigma_w=0.01,
		sigma_bw=0.008
		)
	precincts_df = precincts_df.loc[:(nPrecincts - 1)]
	for membership in (0, 1, None):
		for ground_truth in (True, False):
			for use_estimates in (True, False):
				for eco in (True, False):
					precincts_df[obj_col_name(ground_truth, membership, use_estimates, eco)] = 0.0

	# precinct = precincts_df.loc[0]
	for row_ind, precinct in precincts_df.iterrows():
		print(f"Processing precinct {row_ind+1}/{precincts_df.shape[0]}")
		for simulation_index in range(nSimulations_per_precinct):
			print(f"\tSIMULATION {simulation_index+1}/{nSimulations_per_precinct}")
			sols = optimize_one_precinct(
				precinct,
				nFacilities,
				nFacilities_selected,
				nScenarios=nScenarios,
				robust=robust,
				solver=solver
				)
			return sols
			for (use_estimates, eco), sol in sols.items():
				sol = sols[use_estimates, eco]
				for membership in (0, 1, None):
					for ground_truth in (True, False):
						precincts_df.loc[
							row_ind,
							obj_col_name(ground_truth, membership, use_estimates, eco)] += (
								1/nSimulations_per_precinct
								) * get_objective(
								sol.model, ground_truth=ground_truth, only_for_group=membership
								)
	precincts_df.to_csv(f'output/floc_precincts_df{savefile_extra}.csv', index=False)
	precincts_df.to_pickle(f'output/floc_precincts_df{savefile_extra}.pickle')
	precincts_df.to_excel(f'output/floc_precincts_df{savefile_extra}.xls', index=False)
	precincts_df.to_pickle(f'output/floc_precincts_df{savefile_extra}.pickle')
	return sols, precincts_df


def optimize_one_precinct(
	precinct, nFacilities, nFacilities_selected, nScenarios=30, robust=False, solver="gurobi"
	):
	d0, indiv_locs, fac_locs = get_random_distmat(int(precinct['N']), nFacilities, multiplier=100)
	sols = {}
	for use_estimates in (False,): #(True,False)
		for eco in (True, False):
			model = get_model(
				precinct,
				nFacilities=nFacilities,
				nFacilities_selected=nFacilities_selected,
				J0_size=0,
				d0=d0,
				use_estimates=use_estimates,
				eco=eco,
				nScenarios=nScenarios,
				robust=robust
				)
			return model
			sol = get_solution(model, solver=solver)
			sols[use_estimates, eco] = sol
	return sols


if __name__ == "__main__":
	trial_name = 'testing' #'5_simulations_40_scenarios_big_eco_gap_capacities'
	# sols, precincts_df =
	model = optimize_many_precincts(
		savefile_extra=trial_name,
		nPerPrecinct=10, #100
		nPrecincts=1,
		nFacilities=4, #10
		nFacilities_selected=2,
		nSimulations_per_precinct=1,
		Bb=0.5,
		Bw=0.9,
		nScenarios=40,
		robust=True,
		solver="multilevel"
		)
	model.display()
	sol = get_solution(model, solver="multilevel")

	# sol = sols[False, True] # use_estimates=False, eco=True
	# 5586644800
	# 5521259488
	# 5590306144

if False: # __name__ == "__main__":
	from robust_FLOC import *
	model = pyo.ConcreteModel()

	constrain = get_constraint_adder(model)

	model.x = pyo.Var(bounds=(2, 6), domain=pyo.Integers)
	model.y = pyo.Var(domain=pyo.Integers)
	model.sub = pao.pyomo.SubModel(fixed=[model.x, model.y])

	constrain_sub = get_constraint_adder(model.sub)

	model.sub.z = pyo.Var(bounds=(0, None), domain=pyo.Integers)
	model.o = pyo.Objective(expr=model.x + 3 * model.sub.z, sense=pyo.minimize)
	constrain([model.x + model.y == 10])

	constrain_sub([model.x + model.sub.z <= 8])
	constrain_sub([model.x + 4 * model.sub.z >= 8])
	constrain_sub([model.x + 2 * model.sub.z <= 13])

	model.sub.o = pyo.Objective(expr=model.sub.z, sense=pyo.maximize)
	mipsolver = pao.Solver('gurobi') #'cbc'
	options = dict(MIPGap=.05) #NonConvex=2,
	for k, v in options.items():
		mipsolver.solver.options[k] = v
	opt = pao.Solver("pao.pyomo.PCCG", mip_solver=mipsolver)
	results = opt.solve(model, tee=True)
	print(model.x.value, model.y.value, model.sub.z.value)
'''
NOTEs:
* See plot_eco_advantage_B_vs_X, plot_eco_advantage_W_vs_X: eco inference is most effective when

* Motivation is: you want to optimize over a handful of precincts     n though you have data (that informs ecological inference) for many precincts!

'''
