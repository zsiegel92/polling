import numpy as np
from math import prod
import pandas as pd
import numpy as np
import pyomo.environ as pyo
from pyomo_utils import solve_gams_direct, add_constraints, solve_executable, solve_pyomo, get_constraint_adder, fix_all, unfix_all
from counting_utils import random_choice


# m = number of individuals
# nScenarios = number scenarios desired
# X_membership = ground-truth of group membership. length-m binary array. Known!
# beta_B (beta_W) = P(vehicle owner | group B (~B))
# returns an mXnScenarios matrix theta
# theta[i,p]=1 iff in scenario p, individual i owns a vehicle
# theta[i,p]=0 iff in scenario p, individual i does not own a vehicle
# Among members of group B, beta_B of them are assigned a vehicle in each scenario
# Among members of group W, beta_W of them are assigned a vehicle in each scenario
def get_scenario_types_eco(m, nScenarios, X_membership, beta_B, beta_W):
	theta = np.zeros((m, nScenarios))
	X_members = np.nonzero(X_membership)[0]
	X_not_members = np.nonzero(np.array(X_membership) == 0)[0]
	n_X_T = beta_B * len(X_members)
	n_notX_T = beta_W * len(X_not_members)
	for p in range(nScenarios):
		member_inds = random_choice(X_members, n_X_T)
		non_member_inds = random_choice(X_not_members, n_notX_T)
		theta[member_inds, p] = 1
		theta[non_member_inds, p] = 1
	return theta


# m = number of individuals
# nScenarios = number scenarios desired
# T = fraction of vehicle owners
# returns an mXnScenarios matrix theta
# theta[i,p]=1 iff in scenario p, individual i owns a vehicle
# theta[i,p]=0 iff in scenario p, individual i does not own a vehicle
# Fraction T of all individuals are assigned a vehicle in each scenario
def get_scenario_types_agg_only(m, nScenarios, T):
	theta = np.zeros((m, nScenarios))
	n_T = T * m
	for p in range(nScenarios):
		theta[random_choice(m, size=n_T), p] = 1
	return theta


def set_bilevel(model):
	model.obj.deactivate()
	del model.obj
	model.obj = pyo.Objective(expr=model.base_obj, sense=pyo.minimize)
	model.obj.activate()
	unfix_all(model.y)


def set_second_level(model):
	model.obj.deactivate()
	del model.obj
	model.obj = pyo.Objective(expr=model.ground_truth_objective, sense=pyo.minimize)
	fix_all(model.y)
	model.obj.activate()

	# model.ground_truth_objective = get_objective(model,ground_truth=True)


# distance per individual is the objective
def get_objective(model, ground_truth=False, only_for_group=None):
	if ground_truth:
		theta = np.reshape(model.T_ground_truth, (len(model.T_ground_truth), 1))
		nScenarios = 1
	else:
		theta = model.theta
		nScenarios = model.nScenarios
	if only_for_group is None:
		indiv_indices = model.i
	else:
		indiv_indices = [i for i in model.i if model.X_membership[i] == only_for_group]
	if len(indiv_indices) == 0:
		return 0
	return (1 / nScenarios) * (1 / len(indiv_indices)) * sum(
		model.d0[i, j] * (model.d_low * theta[i, p] + model.d_high * (1 - theta[i, p])) * model.x[i, j, p]
		for i in indiv_indices for j in model.j for p in range(nScenarios)
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
def stochastic_FLOC_model(
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
	nScenarios=30,
	J0=[],
	eco=True
	):
	varnames = locals().copy() #stores *args and **kwargs
	model = pyo.ConcreteModel()

	# stores all arguments of this function
	# as attributes of model for access later on
	for k, v in varnames.items():
		setattr(model, k, v)

	constrain = get_constraint_adder(model)

	X = sum(X_membership) / len(X_membership)
	T = beta_B * X + beta_W * (1 - X)

	m = d0.shape[0] # number individuals
	n = d0.shape[1] # number facilities

	if eco:
		model.theta = get_scenario_types_eco(m, nScenarios, X_membership, beta_B, beta_W)
	else:
		model.theta = get_scenario_types_agg_only(m, nScenarios, T)

	model.i = pyo.RangeSet(0, m - 1)
	model.j = pyo.RangeSet(0, n - 1)
	model.p = pyo.RangeSet(0, nScenarios - 1)
	model.x = pyo.Var(model.i * model.j * model.p, domain=pyo.NonNegativeReals, bounds=(0, 1), initialize=0)
	model.y = pyo.Var(model.j, domain=pyo.Binary, initialize=0)

	constrain([sum(model.x[i, j, p]
		for j in model.j) == 1
		for i in model.i
		for p in model.p]) # each individual assigned a facility in each scenario

	# constrain([sum(model.x[i,j,p] for i in model.i) <= capacities[j] for j in model.j for p in model.p]) # capacity constraint
	# constrain([model.x[i,j,p] <= model.y[j] for i in model.i for j in model.j for p in model.p]) # facility-use constraint
	constrain([sum(model.x[i, j, p]
		for i in model.i) <= capacities[j] * model.y[j]
		for j in model.j
		for p in model.p]) #combined capacity and facility-use constraint

	constrain([model.y[j] == 1 for j in J0])
	constrain([sum(model.y[j] for j in model.j) <= nFacilities_selected])

	model.base_obj = get_objective(model, ground_truth=False)
	model.ground_truth_objective = get_objective(model, ground_truth=True)
	model.group_ground_truth_objectives = {
		membership: get_objective(model, ground_truth=True, only_for_group=membership)
		for membership in [0, 1]
		}
	model.group_objectives = {
		membership: get_objective(model, ground_truth=False, only_for_group=membership)
		for membership in [0, 1]
		}
	model.obj = pyo.Objective(expr=model.base_obj, sense=pyo.minimize)
	return model
