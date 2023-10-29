from pathos.multiprocessing import ProcessingPool as Pool


inp = list(range(10))
pool = Pool(8)
outp = pool.map(lambda x: -x, inp)
pool.close()
pool.join()
pool.terminate()
pool.restart()

# run in repl - works!
if False:
	inp2 = list(range(10))
	pool = Pool(8)
	outp = pool.map(lambda x: -x, inp2)
	pool.close()
	pool.join()
	pool.terminate()
	pool.restart()
