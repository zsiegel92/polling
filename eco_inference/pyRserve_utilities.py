import pyRserve
from pyRserve.rexceptions import REvalError
import subprocess as sp
import os
import pandas as pd
import warnings
from functools import wraps

from LazyObject import LazyObject

warnings.filterwarnings("ignore")


def conn_init():
	try:
		return pyRserve.connect()
	except:
		start_rserve_daemon()
		return pyRserve.connect()


conn = LazyObject(lambda: conn_init())
# def conned(f):
# 	@wraps(f)
# 	def wrapper(*args,**kwargs):
# 		if
# 		output = f(*args,**kwargs)
# 		return output
# 	return wrapper


def start_rserve_blocking():
	'''
	use
	```
	library(Rserve)
	run.Rserve()
	```
	'''
	sp.run(
		'R -e "library(Rserve); run.Rserve()"',
		shell=True,
		capture_output=False,
		stdin=sp.DEVNULL,
		stdout=sp.DEVNULL,
		stderr=sp.DEVNULL
		)


def start_rserve_daemon():
	'''
	```
	library(Rserve)
	Rserve()
	```
	the latter for daemon mode.
	One liner for daemon mode:
	```
	/Library/Frameworks/R.framework/Resources/bin/R CMD /Library/Frameworks/R.framework/Versions/4.1-arm64/Resources/library/Rserve/libs//Rserve
	```
	'''
	# global conn
	sp.run(
		"/Library/Frameworks/R.framework/Resources/bin/R CMD /Library/Frameworks/R.framework/Versions/4.1-arm64/Resources/library/Rserve/libs//Rserve",
		shell=True,
		capture_output=False
		)
	# conn = pyRserve.connect()


def kill_rserve():
	pids = [
		int(line.split(" ", 1)[0])
		for line in sp.run("ps ax | grep Rserve", shell=True, capture_output=True).stdout.decode().split("\n")
		if 'grep' not in line and line
		]
	for pid in pids:
		os.kill(pid, 9)


def stop_rserve():
	global conn
	if conn.isClosed:
		conn = pyRserve.connect()
	conn.shutdown()


# See TaggedList
# https://pyrserve.readthedocs.io/en/latest/manual.html#the-r-namespace-setting-and-accessing-variables-in-a-more-pythonic-way
def get_df(df_name):
	tmp = conn.r(df_name) # type pyRserve.taggedContainers.TaggedList
	return pd.DataFrame({k: v for k, v in zip(tmp.keys, tmp.values)})


def name_tmp(varname):
	return f"{varname}__"


def unname_tmp(tmp_varname):
	if tmp_varname[-2:] != "__":
		raise
	return tmp_varname[:-2]


def quoted(s):
	return f"\"{s}\""


def create_df(df, dfname='df', cols=None, col_aliases={}):
	if cols is None:
		cols = df.columns
	for col in cols:
		col_aliases[col] = col_aliases.get(col, col)
	for col in cols:
		setattr(conn.r, name_tmp(col), df[col].values)
	conn.r(f"{dfname}<- data.frame({','.join([name_tmp(col) for col in cols])})")
	conn.r(f"colnames({dfname})=c({','.join([quoted(col_aliases[col]) for col in col_aliases])})")
	for col in cols:
		conn.r(f"rm({name_tmp(col)})")


def do_eco_inf(df, XCol='X', TCol='T', NCol='N'):
	source_eir()
	create_df(df, 'eidf', cols=[XCol, TCol, NCol], col_aliases={XCol: 'x', TCol: 't', NCol: 'n'})
	# conn.r.x = df[XCol].values
	# conn.r.t = df[TCol].values
	# conn.r.n = df[NCol].values
	# conn.r("df<- data.frame(x,t,n)")
	cmds = '''
	formula= t ~ x
	dbuf = ei(formula=formula, total="n", data=eidf)
	# summary(dbuf)
	bb_out <- data.frame(eiread(dbuf, "betab", "betaw", "sbetab", "sbetaw")) #,"sigmaW","sigmaB"
	# b_i_B = bb_out$betab
	'''
	for line in cmds.split("\n"):
		conn.r(line)
	bb_out = get_df('bb_out')
	# bb_out = conn.r.bb_out
	return bb_out


def source_eir():
	cmds = '''
		library(ei)
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/bounds.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/ei.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/eiRxCplot.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/eiread.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/likelihoodfn.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/plot.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/summary.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/summary.R")
		source("/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference/misc/eir-master/R/zzz.R")
	'''
	for line in cmds.split("\n"):
		conn.r(line)


# https://github.com/ralhei/pyRserve
# https://pyrserve.readthedocs.io/en/latest/
def send_command_example():

	# conn = pyRserve.connect()
	conn.r("vec <- c(1, 2, 4)")
	print(conn.r.vec)
	print(conn.r.sum(conn.r.vec))
	conn.r.somenumber = 444
	conn.r("somenumber * 2")
	conn.shutdown()
