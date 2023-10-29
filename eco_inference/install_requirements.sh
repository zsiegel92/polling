cd "/Users/zach/UCLA Anderson Dropbox/Zach Siegel/UCLA_classes/research/Polling/src/eco_inference"
python -m pip install cython --no-use-pep517
python -m pip install numpy --no-use-pep517
python -m pip install pandas --no-use-pep517

# https://stackoverflow.com/questions/65745683/how-to-install-scipy-on-apple-silicon-arm-m1
python -m pip install pybind11
brew install openblas gfortran
brew install lapack
export PKG_CONFIG_PATH="/opt/homebrew/opt/lapack/lib/pkgconfig $PKG_CONFIG_PATH"
# export LDFLAGS="-L/opt/homebrew/opt/lapack/lib"
# export CPPFLAGS="-I/opt/homebrew/opt/lapack/include"
export OPENBLAS="`brew --prefix openblas`/lib"
export CFLAGS="-falign-functions=8 ${CFLAGS}"
python -m pip install pythran
pip3 install --no-binary :all: --no-use-pep517 scipy


python -m pip install pymc3==3.9.3
python -m pip install arviz==0.11.1 --no-use-pep517
python -m pip install scikit-learn --no-use-pep517
python -m pip install matplotlib --no-use-pep517
python -m pip install pandas --no-use-pep517
python -m pip install seaborn --no-use-pep517
python -m pip install graphviz --no-use-pep517


python -m pip install -r requirements.txt


python -m pip install sklearn --no-use-pep517



export HDF5_DIR=/opt/homebrew/opt/hdf5
pip install netCDF4
python -m pip install h5py
brew install libjpeg
python -m pip install pillow

python -m pip install kiwisolver

pip install git+git://github.com/mggg/ecological-inference.git

# If float128 error:
# in /Users/zach/.pyenv/versions/3.8-dev/lib/python3.8/site-packages/pymc3/distributions/dist_math.py
# change "float128" to "longdouble"
