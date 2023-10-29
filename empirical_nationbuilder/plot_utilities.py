from sklearn.metrics import roc_curve
import matplotlib.pyplot as plt

def plot_roc(targets,scores, AUC=None,ax=None,xlabel='False Positive Rate',ylabel='True Positive Rate',title="ROC Curve",nlabels=5,saveTitle=None):
	if ax is None:
		fig,ax = plt.subplots()

	fprs,tprs,thresh = roc_curve(targets,scores)
	label_inds = [i for i in range(0,len(thresh),len(thresh)//nlabels)]
	if len(thresh)-1 not in label_inds:
		label_inds.pop()
		label_inds.append(len(thresh)-1)
	labelx = fprs[label_inds]
	labely = tprs[label_inds]
	labeltext = thresh[label_inds]
	if AUC is not None:
		ax.plot(fprs, tprs, lw=2, label= f'AUC= {AUC : 0.04f}')
		ax.legend(loc="lower right",fontsize="x-small")
	else:
		ax.plot(fprs, tprs, lw=2)
	ax.scatter(labelx,labely,marker="x")
	for x,y,lab in zip(labelx,labely,labeltext):
		ax.text(x+0.02,y-0.04,f"{lab:.02}",fontsize=12)
	ax.grid(color='0.7', linestyle='--', linewidth=1)
	# ax.set_xlim([-0.1, 1.1])
	# ax.set_ylim([0.0, 1.05])
	ax.set_xlabel(xlabel,fontsize=15)
	ax.set_ylabel(ylabel,fontsize=15)
	ax.set_title(title)
	for label in ax.get_xticklabels()+ax.get_yticklabels():
		label.set_fontsize(15)
	if saveTitle is not None:
		plt.savefig(saveTitle)


def plot_hist(df,column,title,filename,bins=100):
	# tract_df.rename(columns={0: 'Census_Tract', 1: "Count"},inplace=True)
	hist = df.hist(column=column,bins=bins)
	hist[0][0].set_title(title)
	plt.savefig(filename)



