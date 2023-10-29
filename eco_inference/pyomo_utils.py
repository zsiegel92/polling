# https://gams.com/latest/docs/API_PY_TUTORIAL.html#PY_GETTING_STARTED
# see /Users/zach/.pyenv/versions/3.8.10/lib/python3.8/site-packages/sitecustomize.py
# https://pyomo.readthedocs.io/en/stable/library_reference/solvers/gams.html#gams
# pyomo.solvers.plugins.solvers.GAMS.GAMSShell

# https://pyomo.readthedocs.io/en/stable/library_reference/solvers/gams.html#gams-writer
# pyomo.repn.plugins.gams_writer.ProblemWriter_gams

# License in /Users/zach/Library/Application Support/GAMS/gamslice.txt

import numpy as np
from math import prod
from time import time
import pandas as pd
import numpy as np

import pyomo.solvers.plugins.solvers
import pyomo.repn.plugins.gams_writer
import pyomo.environ as pyo
# from pyomo.environ import SolverFactory, ConcreteModel, RangeSet, Var, Objective, Constraint, value, NonNegativeReals, Integers, log, sqrt, summation, Binary
from pyomo.solvers.plugins.solvers.GAMS import GAMSDirect
from pyomo.solvers.plugins.solvers.gurobi_direct import GurobiDirect
from pyomo.contrib import appsi

import pao

import subprocess
from subprocess import Popen, PIPE
import os
import sys
# import gams


def fix_all(variable, val=None):
	if val is None:
		for k, component in variable.items():
			component.fix(component.value)
	else:
		for k, component in variable.items():
			component.fix(val)


def unfix_all(variable):
	for k, component in variable.items():
		component.unfix()


def dot_product(coefs, var):
	return sum(coefs[i] * var[i] for i in range(len(coefs)))


## Usage:
# constrain = get_constraint_adder(model.constr)
# constrain([model.x[i,j] <= 1 for i in model.i for j in model.j])
def get_constraint_adder(model):
	constraint_list_name = 'constr'
	while hasattr(model, constraint_list_name):
		constraint_list_name = constraint_list_name + "_prime"
	setattr(model, constraint_list_name, pyo.ConstraintList())
	constraintList = getattr(model, constraint_list_name)
	return lambda constraints_in_a_list: add_constraints(constraintList, constraints_in_a_list)


def add_constraints(constraintList, constraints_in_a_list):
	for constr in constraints_in_a_list:
		constraintList.add(constr)


def solve_executable(model, solver="couenne", tee=True, warmstart=True, max_time=None, **kwargs):
	executable_paths = {
		"couenne":
			'/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/covid_mitigation/misc/couenne-osx/couenne',
		"bonmin":
			'/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/covid_mitigation/misc/bonmin-osx/bonmin',
		}
	# executable=executable_paths[solver.lower()]
	assert solver.lower() in executable_paths
	binary_solver = pyo.SolverFactory(solver, tee=tee)
	if max_time is not None:
		print(f"Warning: 'max_time' not being set running from executable (pyomo_utils.py:46)")
	return binary_solver.solve(model, load_solutions=True) #io_options=dict(warmstart=warmstart)


def solve_gms_shell(filename=None):
	pwd = os.getcwd()
	if filename is None:
		filename = f"model.gms"
	print(f"Solving via `{subprocess.call('which gams',shell=True)} {pwd}/{filename}`")
	solver_capability = None # Not sure what this parameter does?
	# https://pyomo.readthedocs.io/en/stable/library_reference/solvers/gams.html#pyomo.solvers.plugins.solvers.GAMS.GAMSShell.executable
	io_options = {
		"symbolic_solver_labels": True,
		"solver": "DICOPT",
		"mtype": "minlp",
		"file_determinism": 2
		}
	pw = pyomo.repn.plugins.gams_writer.ProblemWriter_gams()
	pw(model, filename, solver_capability, io_options)
	output = subprocess.call(f'cd "{pwd}" && gams {filename}', shell=True)

	proc = Popen([f'cd "{pwd}" && gams {filename}'], shell=True, stdout=PIPE, encoding='utf-8')
	while proc.poll() is None:
		text = proc.stdout.readline()
		print(text)


def solve_bonmin_direct(model):
	sol = SolverFactory('bonmin', executable='/content/bonmin').solve(model).write()


def solve_gurobi_appsi(model, tee=False, keepfiles=False, warmstart=True):
	opt = appsi.solvers.Gurobi()
	opt.config.stream_solver = True
	opt.set_instance(m)
	opt.solver_options['PreCrush'] = 1
	opt.solver_options['LazyConstraints'] = 1
	res = opt.solve(m)
	return res


def solve_gurobi_direct(model, tee=False, keepfiles=False, warmstart=True):
	gd = GurobiDirect()
	assert gd.available()
	assert gd.warm_start_capable()
	# https://github.com/Pyomo/pyomo/blob/main/pyomo/solvers/plugins/solvers/gurobi_direct.py

	# io_options = dict(
	# 	symbolic_solver_labels=False,
	# 	file_determinism=2,
	# 	# skip_trivial_constraints=True,
	# 	add_options=[], #,"solvelink=5"
	# 	warmstart=warmstart,
	# 	NonConvex = 2
	# )
	# gd.options['NonConvex']=2
	return gd.solve(model, tee=tee, keepfiles=keepfiles, report_timing=True)


# https://support.gurobi.com/hc/en-us/community/posts/360074274611-Set-params-NonConvex-2-with-Pyomo
def solve_gurobi(
	model,
	tee=False,
	keepfiles=False,
	warmstart=True,
	optGap=0.98,
	max_time=None,
	multistart=False,
	multistart_kwargs=None
	):
	if not multistart:
		opt = pyo.SolverFactory("gurobi", solver_io="python") #
		options = dict(MIPGap=optGap) #NonConvex=2,
		if max_time is not None:
			options['TimeLimit'] = max_time
		for k, v in options.items():
			opt.options[k] = v
		print(f"SOLVING WITH GUROBI USING THE FOLLOWING OPTIONS:")
		print(opt.options)
		return opt.solve(model, tee=tee, keepfiles=keepfiles, report_timing=True)
	else:
		# https://github.com/Pyomo/pyomo/blob/4f6f523483ffbf0eade463b1872885a140b607f1/pyomo/contrib/multistart/multi.py
		options = dict(NonConvex=2, MIPGap=optGap)
		if max_time is not None:
			options['TimeLimit'] = max_time

		opt = pyo.SolverFactory("multistart")
		opt.CONFIG['solver'] = 'gurobi'
		opt.CONFIG['strategy'] = 'rand'
		opt.CONFIG['solver_args'] = dict(
			tee=tee, keepfiles=keepfiles, report_timing=True, options=options
			)
		if multistart_kwargs is not None:
			for k, v in multistart_kwargs.items():
				opt.CONFIG[k] = v
		# opt.CONFIG['iterations'] = -1 # high confidence stopping. Default 10
		# opt.CONFIG['HCS_max_iterations'] = 20
		# opt.CONFIG['HCS_tolerance'] = 100
		print("SOLVING MULTISTART WITH GUROBI")
		return opt.solve(model)


# https://pyomo.readthedocs.io/en/stable/library_reference/solvers/gams.html


# add_options=None
# List of additional lines to write directly into model file before the solve statement. For model attributes, <model name> is GAMS_MODEL.
## CPLEX Options in OptFile:
# https://pyomo.readthedocs.io/en/stable/library_reference/solvers/gams.html
# https://www.gams.com/latest/docs/S_CPLEX.html
## options should be in cplex.opt
## Parallelization options:
# https://www.gams.com/latest/docs/RN_cplex11.html
def solve_gams_direct(
	model,
	solver="DICOPT",
	tee=False,
	keepfiles=False,
	warmstart=True,
	optGap=None,
	max_time=None,
	multistart=False,
	multistart_kwargs=None
	):
	if not multistart:
		gd = GAMSDirect()
		assert gd.available()
		assert gd.warm_start_capable()

		io_options = dict(
			symbolic_solver_labels=False,
			solver=solver, #"BARON", # "DICOPT",
			mtype="minlp",
			file_determinism=2,
			# skip_trivial_constraints=True,
			add_options=["GAMS_MODEL.OptFile=1", f"option optcr={optGap};"], #,"solvelink=5"
			warmstart=warmstart
			)
		if max_time is not None:
			io_options['add_options'].append(
				f"option resLim={max_time};"
				) #default Default: 10000000000 # interfaces to MaxTime for BARON
		return gd.solve(
			model,
			tee=tee,
			io_options=io_options,
			keepfiles=keepfiles,
			tmpdir="gams_aux",
			report_timing=True
			)
	else:

		io_options = dict(
			symbolic_solver_labels=False,
			solver=solver, #"BARON", # "DICOPT",
			mtype="minlp",
			file_determinism=2,
			# skip_trivial_constraints=True,
			add_options=["GAMS_MODEL.OptFile=1", f"option optcr={optGap};"], #,"solvelink=5"
			warmstart=warmstart
			)
		if max_time is not None:
			io_options['add_options'].append(f"option resLim={max_time};") #

		opt = pyo.SolverFactory("multistart")
		opt.CONFIG['solver'] = 'gams'
		opt.CONFIG['strategy'] = 'rand'
		opt.CONFIG['solver_args'] = dict(
			tee=tee, io_options=io_options, keepfiles=keepfiles, tmpdir="gams_aux",
			report_timing=True
			)
		if multistart_kwargs is not None:
			for k, v in multistart_kwargs.items():
				opt.CONFIG[k] = v
		# opt.CONFIG['iterations'] = -1 # high confidence stopping. Default 10
		# opt.CONFIG['HCS_max_iterations'] = 20
		# opt.CONFIG['HCS_tolerance'] = 100
		print("SOLVING MULTISTART WITH GAMS (BARON)")
		return opt.solve(model)


def solve_multilevel(
	model,
	tee=False,
	mipsolver="gurobi", # 'gurobi' and 'cbc' work
	keepfiles=False,
	warmstart=True,
	solve_from_binary=False,
	optGap=0.98,
	max_time=None,
	**kwargs,
	):
	# https://pao.readthedocs.io/en/latest/solvers.html#the-solver-interface
	mipsolver = pao.Solver(mipsolver)
	options = dict(MIPGap=optGap) #NonConvex=2,
	if max_time is not None:
		options['TimeLimit'] = max_time
	for k, v in options.items():
		mipsolver.solver.options[k] = v
	opt = pao.Solver("pao.pyomo.PCCG") # , mip_solver=mipsolver

	print(f"SOLVING WITH {mipsolver} (for MIP) USING THE FOLLOWING OPTIONS:")
	print(mipsolver.solver.options)
	return opt.solve(model, tee=tee, keepfiles=keepfiles, report_timing=True)


def solve_pyomo(
	model,
	tee=False,
	solver="DICOPT",
	keepfiles=False,
	warmstart=True,
	solve_from_binary=False,
	optGap=0.98,
	max_time=None,
	multistart=False,
	multistart_kwargs=None,
	**kwargs,
	):
	print(f"Unused kwargs to 'solve_pyomo': {kwargs}")
	start = time()
	if solver.lower() == 'gurobi':
		sol = solve_gurobi(
			model,
			tee=tee,
			keepfiles=keepfiles,
			warmstart=warmstart,
			optGap=optGap,
			max_time=max_time,
			multistart=multistart,
			multistart_kwargs=multistart_kwargs
			)
	elif solver.lower() == 'multilevel':
		sol = solve_multilevel(
			model,
			tee=tee,
			keepfiles=keepfiles,
			warmstart=warmstart,
			optGap=optGap,
			max_time=max_time,
			)
	else:
		if solve_from_binary:
			sol = solve_executable(
				model,
				solver=solver,
				tee=tee,
				keepfiles=keepfiles,
				warmstart=warmstart,
				max_time=max_time
				)
		else:
			sol = solve_gams_direct(
				model,
				solver=solver,
				tee=tee,
				keepfiles=keepfiles,
				warmstart=warmstart,
				optGap=optGap,
				max_time=max_time,
				multistart=multistart,
				multistart_kwargs=multistart_kwargs
				)
	end = time()
	timeToSolve = end - start
	objVal = model.obj.expr()
	return objVal, timeToSolve, model, sol
