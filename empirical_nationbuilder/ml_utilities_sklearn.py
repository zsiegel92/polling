from sklearn.linear_model import LogisticRegression



def get_lr(pdf,y):
	# https://scikit-learn.org/stable/modules/generated/sklearn.linear_model.LogisticRegression.html
	clf = LogisticRegression(random_state=0).fit(pdf, y)
	return clf
