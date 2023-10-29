import numpy as np
# from scipy.stats import truncnorm
from trun_mvnt import rtmvn, rtmvt

## From eiR output:
# $`Truncated psi's (ultimate scale)`
#         BB        BW       SB        SW       RHO
#  0.9656009 0.9902505 0.033126 0.0136933 0.2010252

nPerPrecinct = 100 # make multiple of 10 to always have compatible fractions
nPrecincts = 150
reqPrecision = np.ceil(np.log10(nPerPrecinct))




Bb, Bw = 0.7, 0.95 # 0.965, 0.991 = estimates from tract data

sigma_b = 2.72 # .033
sigma_w = 2.65 # .014
sigma_bw = 0.40 # .20
Sigma = np.array([[sigma_b, sigma_bw],[sigma_bw, sigma_w]])

betas = np.around(
            rtmvn(n=nPrecincts, Mean=np.array([Bb,Bw]), Sigma=Sigma, \
                D=np.eye(2), \
                lower=np.array([0,0]), upper=np.array([1,1]), \
                burn=10, thin=1, ini=[]),
          decimals=reqPrecision) # car ownership given race

X = np.around(np.random.rand(nPrecincts),decimals=reqPrecision) # eXplanatory variable (race)
# T = np.random.rand(nPrecincts) # dependent Turnout variable (car ownership)
T = [beta_B*x+ beta_W*(1-x) for x,(beta_B, beta_W) in zip(X,betas)]



car_owners = [np.random.choice(nPerPrecinct, size=T[i]*nPerPrecinct) for i in range(nPrecincts)]

car_owner_guesses = [np.random.choice(nPerPrecinct, size=T[i]*nPerPrecinct) for i in range(nPrecincts)]
