import numpy as np
import pandas as pd
# import pyei
# from itertools import product
# import seaborn as sns
# import matplotlib.pyplot as plt
# from matplotlib.colors import LogNorm, Normalize
import pyomo.environ as pyo
# import pao
import os
import pyomo

# from scipy.stats import truncnorm
# from trun_mvnt import rtmvn, rtmvt

from plot_utilities import plotter
# from pyRserve_utilities import start_rserve_daemon, do_eco_inf, stop_rserve, create_df, conn, REvalError
import counting_utils
from pyomo_utils import solve_gams_direct, add_constraints, solve_executable, solve_pyomo, get_constraint_adder, fix_all, unfix_all, dot_product
from process_results import Solution
from counting_utils import rand, random_choices, random_choice, random_binary_choice
# from stochastic_FLOC import stochastic_FLOC_model, set_bilevel, set_second_level


def generate_params(dimx, dimy, nConstr, n, n1, n2):
	c0 = abs(rand.random((dimx,)))
	c = [abs(rand.random((dimx,))) for _ in range(n)]
	d = abs(rand.random((dimy,)))
	b = rand.random((nConstr,))
	A = rand.random((nConstr, dimx))
	B = rand.random((nConstr, dimy))
	# nInt = int(frac_y_int * dimy)
	I1, I2 = counting_utils.random_choices(n, (n1, n2))
	return c0, c, d, A, B, b, I1, I2


def get_robust_one_group(c0, c, d, A, B, b, gamma):
	dimx = A.shape[1]
	dimy = B.shape[1]
	nConstr = A.shape[0]
	n = len(c)
	model = pyo.ConcreteModel(name='model')
	constrain = get_constraint_adder(model)
	model.x = pyo.Var(range(dimx), domain=pyo.NonNegativeReals, bounds=(None, None), initialize=0)
	model.y = pyo.Var(range(dimy), domain=pyo.Binary, bounds=(0, 1), initialize=0)
	model.rho = pyo.Var([0], domain=pyo.NonNegativeReals, bounds=(0, None), initialize=0)
	model.phi = pyo.Var(range(n), domain=pyo.NonNegativeReals, bounds=(0, None), initialize=0)
	obj_expr = dot_product(c0, model.x) + dot_product(
		d, model.y
		) + gamma * model.rho[0] + dot_product(np.ones(n), model.phi)
	model.obj = pyo.Objective(expr=obj_expr, sense=pyo.minimize)
	constrain([model.phi[i] >= dot_product(c[i], model.x) - model.rho[0] for i in range(n)])
	constrain([
		dot_product(A[i, :], model.x) + dot_product(B[i, :], model.y) == b[i]
		for i in range(nConstr)
		])
	return model


def get_robust_partitioned(c0, c, d, A, B, b, I1, I2, gamma1, gamma2):
	dimx = A.shape[1]
	dimy = B.shape[1]
	nConstr = A.shape[0]
	n = len(c)
	model = pyo.ConcreteModel(name='model')
	constrain = get_constraint_adder(model)
	model.x = pyo.Var(range(dimx), domain=pyo.NonNegativeReals, bounds=(None, None), initialize=0)
	model.y = pyo.Var(range(dimy), domain=pyo.Binary, bounds=(0, 1), initialize=0)
	model.rho = pyo.Var([0, 1], domain=pyo.NonNegativeReals, bounds=(0, None), initialize=0)
	model.phi = pyo.Var(range(n), domain=pyo.NonNegativeReals, bounds=(0, None), initialize=0)
	obj_expr = dot_product(c0, model.x) + dot_product(
		d, model.y
		) + gamma1 * model.rho[0] + gamma2 * model.rho[1] + dot_product(np.ones(n), model.phi)
	model.obj = pyo.Objective(expr=obj_expr, sense=pyo.minimize)
	constrain([
		dot_product(A[i, :], model.x) + dot_product(B[i, :], model.y) == b[i]
		for i in range(nConstr)
		])
	constrain([model.rho[0] >= 0])
	constrain([model.phi[i] >= dot_product(c[i], model.x) - model.rho[0] for i in I1])
	constrain([model.phi[i] >= dot_product(c[i], model.x) - model.rho[1] for i in I2])
	return model


def get_solution(model, optGap=0.05):
	sol_tuple = solve_pyomo(
		model,
		tee=False,
		solver='gurobi',
		keepfiles=False,
		warmstart=True,
		optGap=optGap,
		)
	return Solution(*sol_tuple, getDF=False)


def write_csv_with_indexing(df, index_cols, csv_basename):
	x = df.copy()
	x.reset_index()
	# put index columns first
	for i, colname in enumerate(index_cols):
		x['tmp'] = x[colname]
		del x[colname]
		x.insert(i, colname, x['tmp'])
		del x['tmp']
	x.sort_values(by=index_cols, inplace=True)
	x.to_csv(f"{csv_basename}.csv", index=False)
	# clear columns
	n_ind = len(index_cols)
	for clearcol in range(1, n_ind):
		# print(clearcol)
		repcols = index_cols[:-clearcol]
		# print(repcols)
		# print(index_cols[-clearcol - 1])
		clearcolname = repcols[-1]
		x.loc[x[repcols].duplicated(), clearcolname] = ''
	x.to_csv(f"{csv_basename}_masked.csv", index=False)
	return x


def do_trials():
	n = 50
	dimy = 50
	dimx = 100
	nConstr = 160
	sols_df = pd.DataFrame()
	for X in np.linspace(0.1, 0.9, 5):
		for beta1 in np.linspace(0.1, 0.9, 5):
			for beta2 in np.linspace(0.1, 0.9, 5):
				print(f"Processing Scenario {X=}, {beta1=}, {beta2=}")
				n1 = int(X * n)
				n2 = n - n1
				T = beta1*X + beta2 * (1-X)
				gamma = int(T * n)
				gamma1 = int(beta1 * n1)
				gamma2 = int(beta2 * n2)
				c0, c, d, A, B, b, I1, I2 = generate_params(
					dimx=dimx, dimy=dimy, nConstr=nConstr, n=n, n1=n1, n2=n2
					)
				model = get_robust_one_group(c0, c, d, A, B, b, gamma)
				partitioned_model = get_robust_partitioned(
					c0, c, d, A, B, b, I1, I2, gamma1, gamma2
					)
				sol = get_solution(model)
				sol_partitioned = get_solution(partitioned_model)
				soldict_single = {f"{k}_single": v for k, v in sol.to_dict().items()}
				soldict_partitioned = {
					f"{k}_partitioned": v for k, v in sol_partitioned.to_dict().items()
					}
				row = dict(
					X=X,
					beta1=beta1,
					beta2=beta2,
					n=n,
					n1=n1,
					n2=n2,
					T=T,
					gamma=gamma,
					gamma1=gamma1,
					gamma2=gamma2,
					dimx=dimx,
					dimy=dimy,
					**soldict_single,
					**soldict_partitioned
					)
				sols_df = sols_df.append(row, ignore_index=True)
	return sols_df


if __name__ == "__main__":
	df = do_trials()
	write_csv_with_indexing(df, ["X", "beta1", "beta2"], "one_vs_two_comparison")
	df_read = pd.read_csv("one_vs_two_comparison.csv")
