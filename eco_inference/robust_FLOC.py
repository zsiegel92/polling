import numpy as np
from math import prod
import pandas as pd
import numpy as np
import pyomo
import pyomo.environ as pyo
import pao
from pyomo_utils import solve_gams_direct, add_constraints, solve_executable, solve_pyomo, get_constraint_adder, fix_all, unfix_all
from counting_utils import random_choice


def set_bilevel_robust(model):
	model.obj.deactivate()
	del model.obj
	model.obj = pyo.Objective(expr=model.base_obj, sense=pyo.minimize)
	model.obj.activate()
	unfix_all(model.sub.y)
	unfix_all(model.z)


def set_second_level_robust(model):
	model.obj.deactivate()
	del model.obj
	model.obj = pyo.Objective(expr=model.ground_truth_objective, sense=pyo.minimize)
	fix_all(model.sub.y)
	fix_all(model.z)
	model.obj.activate()


# distance per individual is the objective
def get_objective(model, ground_truth=False, only_for_group=None):
	if only_for_group is None:
		indiv_indices = model.i
	else:
		indiv_indices = [i for i in model.i if model.X_membership[i] == only_for_group]

	if len(indiv_indices) == 0:
		return 0

	if ground_truth:
		return (1 / len(indiv_indices)) * sum(
			model.d0[i, j] * (model.d_low if model.T_ground_truth[i] == 1 else model.d_high) *
			(model.sub.x[i, j, 0] + model.sub.x[i, j, 1]) for i in indiv_indices for j in model.j
			)
	else:
		return (1 / len(indiv_indices)) * sum(
			model.d0[i, j] * (model.d_low * model.sub.x[i, j, 1] + model.d_high * model.sub.x[i, j, 0])
			for i in indiv_indices
			for j in model.j
			)


# X_membership = ground-truth of group membership. length-m binary array. Known!
# beta_B = P(vehicle owner | group B)
# beta_W = P(vehicle owner | group ~B)
# d_low, d_high = distance multipliers for vehicle/no vehicle. E.g. d_low = 1, d_high = 3
# d0 = mXn matrix of distances before distance multiplication
# J0 = indices of existing facilities (constrained to be open)
# nScenarios = number of scenarios to generate
# nFacilities = number of facilities that can be opened
# T_ground_truth = ground-truth of vehicle ownership. length-m binary array.
# Note that ground truth t (vehicle ownership) is not known.
def robust_FLOC_model(
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
	J0=[],
	eco=True
	):
	varnames = locals().copy() #stores *args and **kwargs

	model = pyo.ConcreteModel(name="model")
	# stores all arguments of this function
	# as attributes of model for access later on
	for k, v in varnames.items():
		setattr(model, k, v)

	m = d0.shape[0] # number individuals
	n = d0.shape[1] # number facilities
	X = sum(X_membership) / len(X_membership)
	T = beta_B * X + beta_W * (1 - X)
	m_BT = int(X * beta_B * m)
	m_WT = int((1 - X) * beta_W * m)

	model.i = pyo.RangeSet(0, m - 1)
	model.j = pyo.RangeSet(0, n - 1)
	model.v = pyo.RangeSet(0, 1) # 0->no vehicle, 1->vehicle
	constrain = get_constraint_adder(model)
	model.z = pyo.Var(model.i, domain=pyo.Binary, bounds=(0, 1), initialize=0) #can be pyo.Binary or pyo.NonNegativeReals?

	model.sub = pao.pyomo.SubModel(fixed=model.z)
	constrain_sub = get_constraint_adder(model.sub)
	model.sub.i = pyo.RangeSet(0, m - 1)
	model.sub.j = pyo.RangeSet(0, n - 1)
	model.sub.v = pyo.RangeSet(0, 1) # 0->no vehicle, 1->vehicle

	model.sub.x = pyo.Var(
		model.sub.i * model.sub.j * model.sub.v, domain=pyo.NonNegativeReals, bounds=(0, 1), initialize=0
		)
	model.sub.y = pyo.Var(model.sub.j, domain=pyo.Binary, initialize=0)

	if sum(X_membership) > 0:
		constrain_sub([sum(model.z[i] for i in model.i if X_membership[i] == 1) <= m_BT])
	if sum(X_membership) < len(X_membership):
		constrain_sub([sum(model.z[i] for i in model.i if X_membership[i] == 0) <= m_WT])

	constrain_sub([model.sub.x[i, j, 1] <= model.z[i] for i in model.i for j in model.j]) #car ownership
	constrain_sub([model.sub.x[i, j, 0] <= 1 - model.z[i] for i in model.i for j in model.j]) #car ownership

	constrain_sub([sum(model.sub.x[i, j, v]
		for j in model.j
		for v in model.v) == 1
		for i in model.i]) # each individual assigned a facility

	# constrain([sum(model.sub.x[i,j,p] for i in model.i) <= capacities[j] for j in model.j for p in model.p]) # capacity constraint
	# constrain([model.sub.x[i,j,p] <= model.sub.y[j] for i in model.i for j in model.j for p in model.p]) # facility-use constraint
	constrain_sub([
		sum(model.sub.x[i, j, v] for i in model.i for v in model.v) <= capacities[j] * model.sub.y[j] for j in model.j
		]) #combined capacity and facility-use constraint

	constrain_sub([model.sub.y[j] == 1 for j in J0])
	constrain_sub([sum(model.sub.y[j] for j in model.j) <= nFacilities_selected])

	model.sub.base_obj = get_objective(model, ground_truth=False)
	model.base_obj = get_objective(model, ground_truth=False)
	# ground_truth_objective = get_objective(model, ground_truth=True)
	# group_ground_truth_objectives = {
	# 	membership: get_objective(model, ground_truth=True, only_for_group=membership)
	# 	for membership in [0, 1]
	# 	}
	# group_objectives = {
	# 	membership: get_objective(model, ground_truth=False, only_for_group=membership)
	# 	for membership in [0, 1]
	# 	}

	model.sub.obj = pyo.Objective(expr=model.sub.base_obj, sense=pyo.minimize)
	model.obj = pyo.Objective(expr=model.base_obj, sense=pyo.maximize)
	return model
