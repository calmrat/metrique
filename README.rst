.. image:: src/metriqued/metriqued/static/img/metrique_logo.png

Metrique
========

Python/MongoDB Data Warehouse and Data Glue

*Metrique can help bring data into an intuitive, indexable 
data object collection that supports transparent 
historical version snapshotting, advanced ad-hoc 
server-side querying, including (mongodb) aggregations 
and (mongodb) mapreduce, along with python, ipython, 
pandas, numpy, matplotlib, and so on, is well integrated 
with the scientific python computing stack. 

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique


Installation
------------

`metrique <https://github.com/drpoovilleorg/metrique/tree/master/src/metrique>`_

`metriqued <https://github.com/drpoovilleorg/metrique/tree/master/src/metriqued>`_

`metriquec <https://github.com/drpoovilleorg/metrique/tree/master/src/metriquec>`_

General
-------

Make sure you have the following *OS stuff* installed 
before doing anything else. The examples given below 
are fedora rpm package names::

    yum install git python python-devel python-setuptools gcc gcc-c++ 

Also, make sure python pip, distribute and setuptools 
are installed up2date:: 

    easy_install -U pip
    pip install -U distribute
    pip install -U setuptools


VirtualENV
----------
We strongly suggest install metrique* into
a virtual environment. If you don't understand
what this means or are only interested to
install the metrique client to interact with
an existing metrique host, skip this. These
steps are entirely optional.

To install virtualenv, run:: 

    pip install virtualenv

To create a new virtual environment to install metrique into::

    mkdir ~/virtenvs
    virtualenv --no-site-packages ~/virtenvs/metriqueenv

    # activate the virtual environment
    source ~/virtenvs/metriqueenv/bin/activate

    # add the following line to .bashrc to quickly enable the virtenv
    # echo alias met='source ~/virtenvs/metriqueenv/bin/activate' >> ~/.bashrc

    # then prepare the environment; pip install metrique, ...

Ipython
-------
We also strongly suggest installing and using IPython 
notebook instead of standard python shell for 
interactive data exploration.

To install ipython notebook install the following 
*OS* packages::

    yum install libpng-devel freetype-devel 

Then install ipython with pip::

    pip install ipython


If you see any error, not otherwise mentioned here, Google.

