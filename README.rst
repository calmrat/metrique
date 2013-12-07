.. image:: src/metriqued/metriqued/static/img/metrique_logo.png
   :target: https://github.com/drpoovilleorg/metrique

Metrique
========

.. image:: https://travis-ci.org/drpoovilleorg/metrique.png
   :target: https://travis-ci.org/drpoovilleorg/metrique

.. image:: https://badge.fury.io/py/metrique.png
    :target: http://badge.fury.io/py/metrique

.. image:: https://pypip.in/d/metrique/badge.png
   :target: https://crate.io/packages/metrique

.. image:: https://d2weczhvl823v0.cloudfront.net/drpoovilleorg/metrique/trend.png
   :target: https://d2weczhvl823v0.cloudfront.net/drpoovilleorg/metrique

.. image:: https://coveralls.io/repos/drpoovilleorg/metrique/badge.png 
   :target: https://coveralls.io/r/drpoovilleorg/metrique

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


Quick Install (auto-deploy -> virtenv)
--------------------------------------

The examples given below use yum and assume fedora rpm package names::

    # prerequisite *os* packages
    sudo yum install python python-devel python-setuptools
    sudo yum install git gcc gcc-c++ gcc-gfortran
    sudo yum install hdf5-devel # apt-get: libhdf5-serial-dev


    # get the sources
    git clone https://github.com/drpoovilleorg/metrique.git
    cd metrique

    # deploy metrique master branch into a virtual environment
    # including dependencies
    ./manage.py deploy ~/virtenv-metrique --pandas --matplotlib --ipython

    # activate the virtual environment
    source ~/virtenv-metrique/bin/activate

