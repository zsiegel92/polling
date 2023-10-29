import numpy as np
from sortedcontainers import SortedList

rand = np.random.default_rng(
	seed=1
	) #https://stackoverflow.com/questions/40914862/why-is-random-sample-faster-than-numpys-random-choice

is_iterable = lambda obj: hasattr(obj, '__iter__') or hasattr(obj, '__getitem__')


def random_choice(list_like_or_int, size):
	return rand.choice(list_like_or_int, size=int(size), replace=False)


def random_binary_choice(number_to_choose_from, size):
	inds = rand.choice(number_to_choose_from, size=int(size), replace=False)
	output = np.zeros(number_to_choose_from)
	output[inds] = 1
	return output


def random_choices_shuffled(n, sampleSizes):
	assert sum(sampleSizes) <= n
	indices = np.arange(n)
	rand.shuffle(indices)
	nSampsTaken = 0
	choices = []
	for sampleSize in sampleSizes:
		choices.append(indices[nSampsTaken:(nSampsTaken + sampleSize)])
		nSampsTaken += sampleSize
	return choices


def random_choice_taboo(n, size, sorted_taboo):
	choice = []
	while len(choice) < size:
		new = [
			ind for ind in rand.choice(n, size=size - len(choice), replace=False)
			if ind not in sorted_taboo
			]
		sorted_taboo.update(new)
		# print(f"{len(choice)=}, {len(np.unique(choice))=}\nAdding {len(new)} elements to choice, of which {len([thing for thing in new if thing in choice])} are already in choice!")
		choice += new
	return choice, sorted_taboo


# use when the total number of samples is far less than n
def random_choices_low_memory(n, sampleSizes):
	assert sum(sampleSizes) <= n
	choices = []
	taboo = SortedList([])
	for sampleSize in sampleSizes:
		choice, taboo = random_choice_taboo(n, sampleSize, taboo)
		choices.append(choice)
	return choices


# n is the size of the population
# returns len(sampleSizes) lists, each of which has length determined by the entry
# in sampleSizes. These contain elements from 0,...,n-1 sampled uniformly.
def random_choices(n, sampleSizes):
	the_sample_sizes = [int(sample_size) for sample_size in sampleSizes]
	return [sorted(l) for l in random_choices_shuffled(n, the_sample_sizes)]
	# if sum(sampleSizes) < 1000:
	# 	return random_choices_shuffled(n,sampleSizes)
	# else:
	# 	return random_choices_low_memory(n,sampleSizes)


def test_sorted_list():
	from time import time
	from sortedcontainers import SortedList
	if True:
		nSamps = 2000000
		nTrials = 20
		sum_times_sortedsearch = 0
		sum_times_search = 0
		for _ in range(nTrials):
			xx = np.random.rand(nSamps)
			val = xx[nSamps // 2]
			start = time()
			val in xx
			end = time()
			sum_times_search += end - start

			start = time()
			yy = SortedList(xx)
			val in yy
			end = time()
			sum_times_sortedsearch += end - start
		avg_time_sortedsearch = sum_times_sortedsearch / nTrials
		avg_time_search = sum_times_search / nTrials
		print(
			f"Average time SORTED search: {avg_time_sortedsearch}\nAverage time REGULAR search: {avg_time_search}\n"
			)


def get_integer_partition(num, k):
	if k == 1:
		return [num]
	partitions = [0] + sorted(np.random.choice(num, k - 1, replace=False)) + [num]
	return [partitions[i + 1] - partitions[i] for i in range(len(partitions) - 1)]


def test_random_choices():
	from time import time
	import pandas as pd
	import seaborn as sns
	import matplotlib.pyplot as plt
	nTrials = 10
	ns = [10000, 100000, 1000000]
	sampleSizeFracs = [1 / 50, 1 / 20, 1 / 10, 1 / 5, 1 / 2] #[1000,2000,10000,50000,100000,500000]
	numbersOfChoices = list(range(2, 10))
	df = pd.DataFrame(
		columns=['n', 'sampleSizeFrac', 'sampleSizeTotal', 'numberOfChoices', 'function', 'time']
		)
	# sampleSizes = [5,100,1000,2000] #,50000,100000
	for n in ns:
		for sampleSizeFrac in sampleSizeFracs:
			sampleSizeTotal = int(sampleSizeFrac * n)
			for numberOfChoices in numbersOfChoices:
				sampleSizes = get_integer_partition(sampleSizeTotal, numberOfChoices)
				for fn in (random_choices_shuffled, random_choices_low_memory):
					trialSum = 0
					for _ in range(nTrials):
						start = time()
						choices = fn(n, sampleSizes)
						end = time()
						assert len(np.unique([ind for l in choices
												for ind in l])) == sum(sampleSizes)
						trialSum += end - start
					trialAvg = trialSum / nTrials
					print(
						f"Average time for {n=} with {sampleSizeTotal=} ({sampleSizeFrac:.2}) and {numberOfChoices=} with {fn.__name__} is: {trialAvg}"
						)
					df = df.append(
						dict(
							n=n,
							sampleSizeTotal=sampleSizeTotal,
							numberOfChoices=numberOfChoices,
							function=fn.__name__,
							time=trialAvg,
							sampleSizeFrac=sampleSizeFrac
							),
						ignore_index=True
						)
	g = sns.FacetGrid(df, row='n', col='numberOfChoices', sharey=True)
	g.map_dataframe(sns.lineplot, x='sampleSizeFrac', y='time', hue='function')
	g.add_legend()
	plt.savefig('figures/random_choices_speedtest.pdf')
	plt.show(block=False)
	return df


if __name__ == "__main__":
	# test_random_choices()
	# df = test_random_choices()
	a = random_choices(10, (5, 5))
