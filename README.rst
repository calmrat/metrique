.. image:: src/metriqued/metriqued/static/img/metrique_logo.png
   :target: https://github.com/kejbaly2/metrique

Metrique
========

.. image:: https://travis-ci.org/kejbaly2/metrique.png
   :target: https://travis-ci.org/kejbaly2/metrique

.. image:: https://badge.fury.io/py/metrique.png
    :target: http://badge.fury.io/py/metrique

.. image:: https://pypip.in/d/metrique/badge.png
   :target: https://crate.io/packages/metrique

.. image:: https://d2weczhvl823v0.cloudfront.net/kejbaly2/metrique/trend.png
   :target: https://d2weczhvl823v0.cloudfront.net/kejbaly2/metrique

.. image:: https://coveralls.io/repos/kejbaly2/metrique/badge.png 
   :target: https://coveralls.io/r/kejbaly2/metrique

Python/MongoDB Data Warehouse and Information Platform

metrique is what happens when you need to load data from many
disperate sources, iteratively, rapidly and reproducibly, knowing
you'll need to produce a wide variety of analysis and reports,
large and small. 

The main feature of metrique is 

else and 
pytho bring data into an intuitive, indexable 
data object collection that supports transparent 
historical version snapshotting, advanced ad-hoc 
server-side querying, including (mongodb) aggregations 
and (mongodb) mapreduce, along with python, ipython, 
pandas, numpy, matplotlib, and so on, is well integrated 
with the scientific python computing stack. 

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/kejbaly2/metrique


Quick Install (auto-deploy -> virtenv)
--------------------------------------

The examples given below use yum and assume fedora rpm package names::

    # prerequisite *os* packages
    sudo yum install python python-devel python-setuptools python-pip
    sudo yum install git gcc gcc-c++ gcc-gfortran
    sudo yum install freetype-devel libpng-devel # matplotlib deps

    # metriqued - mongodb expected to be running; kerberos is optional
    sudo yum install mongodb mongodb-server krb5-devel

    # metriqued - nginx is optional
    sudo yum install nginx 

    # additional python global dependencies, from pip
    sudo pip install pip-accel  # faster cached pip installed

    # make sure our core package managers are up2date
    sudo pip-accel install -U distribute setuptools

    # our installation directory is always a python virtualenv
    sudo pip-accel install virtualenv

    # get the metrique sources
    git clone https://github.com/kejbaly2/metrique.git
    cd metrique

    # deploy metrique master branch into a virtual environment
    # including dependencies. WARNING: This takes 10-15 minutes!
    ./manage.py -V ~/virtenv-metrique deploy --ipython

    # activate the virtual environment
    source ~/virtenv-metrique/bin/activate

    # deploy metrique environment to a new virtual environment
    ./manage.py deploy ~/metrique.master --ipython

    # optionally, start mongodb and metriqued
    ./manage.py mongodb start --fast
    ./manage.py metriqued start

    # launch ipython, connect to a metriqued instance and start mining!
