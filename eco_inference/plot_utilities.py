from functools import wraps
from string import Formatter
import os

import numpy as np
from scipy.special import binom
from numpy import floor
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

wrapped = {}

def plotter(*aaargs,showing=True,saving=True,legend=True,subfolder=None,**function_set_kwargs):
	# print(f"aaargs is: {aaargs}, function_set_kwargs is: {function_set_kwargs}")
	if aaargs:
		if hasattr(aaargs[0],'__call__'):
			ff = aaargs[0]
			filename_template = ff.__name__
			# print(f"CASE1: filename_template={filename_template}")
		else:
			ff = None
			filename_template = aaargs[0]
			# print(f"CASE2: filename_template={filename_template}")
	else:
		ff = None
		filename_template = None
		# print(f"CASE3: filename_template={filename_template}")
	# print(f"OUTSIDE: Creating function with filename_template: {filename_template}\n\taaargs is: {aaargs}")
	if subfolder is not None:
		figure_folder = f"figures/{subfolder}"
		if not os.path.isdir(figure_folder):
			os.mkdir(figure_folder)
	else:
		figure_folder = f"figures"
	def decorator(f):
		nonlocal filename_template
		# print(f"INSIDE: calling function with filename_template: {filename_template}")
		if filename_template is None:
			# print(f"f is: {f}")
			filename_template = f.__name__
		# print(f"args is: {aaargs}, Filename template is: {filename_template}")
		fkeys = [fkey for _, fkey, _, _ in Formatter().parse(filename_template) if fkey is not None]
		nargs = fkeys.count('')
		fkeys = [fkey for fkey in fkeys if fkey]
		@wraps(f)
		def wrapper(*args,**kwargs):
			nonlocal showing, saving, legend,subfolder
			# print(f"fkeys is: {fkeys}")
			plt.close()
			plt.clf()
			fname = filename_template.format(*[args.pop() for _ in range(nargs)],**{k: kwargs.pop(k,f"__{k}?__") for k in fkeys})
			fname = f"{figure_folder}/{fname}"
			if fname.rsplit(".",1)[-1] not in ('pdf','png','jpg','jpeg'):
				fname = f"{fname}.pdf"
			saving = kwargs.pop('saving',saving)
			showing = kwargs.pop('showing',showing)
			legend = kwargs.pop('legend',legend)
			# print(f"Sending args and kwargs to {f.__name__} - args: {args}, kwargs: {kwargs}")
			output = f(*args,**kwargs)
			if legend:
				ax = plt.gca()
				try:
					legendtext = ax.legend_.get_title().get_text()
				except:
					legendtext = None
				if function_set_kwargs.get('combined_df',False):
					ax = plt.gca()
					ax.legend(handles=ax.legend_.legendHandles, labels=[t.get_text() for t in ax.legend_.texts],
					          title=legendtext,fontsize=8)#https://stackoverflow.com/questions/66623746/no-access-to-legend-of-sns-ax-used-with-data-normalisation
				else:
					plt.legend(fontsize=8, title=legendtext,)
			if saving:
				plt.savefig(fname)
			if showing:
				plt.show(block=False)
			return output
		for k,v in function_set_kwargs.items():
			setattr(wrapper,k,v)
		# wrapped.append(wrapper)
		wrapped[wrapper.__name__] = wrapper
		return wrapper
	if ff is not None:
		# used as @plotter
		return decorator(ff)
	else:
		# used as @plotter(*args, **kwargs)
		return decorator


plotter.wrapped = wrapped
