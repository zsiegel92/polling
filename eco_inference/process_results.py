import matplotlib as mpl
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.ticker import FuncFormatter
import seaborn as sns
import pandas as pd
import numpy as np
import pyomo.environ as pyo

# from model import get_objective_cumulative_TT_periods,get_cumulative_disease_cost, get_cumulative_policy_cost
# from model_bilinear import get_objective_cumulative_TT_periods_bilinear,get_cumulative_disease_cost_bilinear, get_cumulative_policy_cost_bilinear,get_cumulative_policy_cost_one_policy_per_period_bilinear

# from model_gurobi_capable import get_cumulative_disease_cost_gurobi_capable,get_cumulative_policy_cost_gurobi_capable,get_objective_cumulative_TT_periods_gurobi_capable,get_SIRD_model_gurobi_capable

# https://stackoverflow.com/questions/55427836/how-to-render-a-latex-matrix-by-using-matplotlib
# mpl.rcParams['font.size'] = 20
# mpl.rcParams['text.usetex'] = True
mpl.rcParams['text.latex.preamble'] = r'\usepackage{amsmath}'


# rcParams['font.monospace'] = ['Tahoma', 'DejaVu Sans',
#                                'Lucida Grande', 'Verdana']
def to_bmatrix(array2d):
	# mat = "\\\\".join(" & ".join(f"{el}" for el in row) for row in array2d)
	# matstring = f"$\\begin{{matrix}}{mat}\\end{{matrix}}$"
	# return r' {} '.format(matstring)
	return str(array2d).replace("\n", "")


def get_results_df(model):
	df = pd.DataFrame()
	return df


class Solution:
	def __init__(self, objVal, timeToSolve, model, sol):
		self.objVal = objVal
		self.timeToSolve = timeToSolve
		self.model = model
		self.sol = sol
		try:
			self.lb = sol['Problem'][0]['Lower bound']
			self.ub = sol['Problem'][0]['Upper bound']
			self.status = sol['Solver'][0]['Status']
		except Exception as e:
			print("Error construction Solution")
			print(e)
			self.lb = 1
			self.ub = 1
			self.status = None
		self.df = get_results_df(self.model)
		# self.otherModel=otherModel


class Solution:
	def __init__(
		self,
		objVal,
		timeToSolve,
		model,
		sol,
		otherModel=None,
		solver_params={},
		extra_metadata={},
		getDF=True
		):
		self.objVal = objVal
		self.timeToSolve = timeToSolve
		self.model = model
		self.sol = sol
		try:
			self.lb = sol['Problem'][0]['Lower bound']
			self.ub = sol['Problem'][0]['Upper bound']
			self.status = sol['Solver'][0]['Status']
		except Exception as e:
			print("Error construction Solution")
			print(e)
			self.lb = 1
			self.ub = 1
			self.status = None
		if getDF:
			self.df = get_results_df(self.model)
		else:
			self.df = None
		self.extra_metadata = extra_metadata
		self.solver_params = solver_params
		try:
			self.termination_condition = sol.Solver.termination_condition.value
			self.timed_out = (sol.Solver.termination_condition.value == 'maxTimeLimit')
		except:
			self.termination_condition = None
			self.timed_out = False

	def to_dict(self):
		return dict(
			objVal=self.objVal,
			# timeToSolve=self.timeToSolve,
			# LB=self.lb,
			# UB=self.ub,
			status=self.status,
			**self.solver_params,
			**self.extra_metadata
			)

	def drop_model(self):
		self.model = None
