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

    # prerequisite *py packages
    easy_install -U pip
    pip install -U distribute setuptools argparse virtualenv

    # get the sources
    git clone https://github.com/drpoovilleorg/metrique.git
    cd metrique

    # deploy metrique master branch into a virtual environment
    # including dependencies
    ./deploy ~/virtenv-metrique

    # activate the virtual environment
    source ~/virtenv-metrique/bin/activate

    # WITH IPYTHON
    # additional *os* prerequisites
    # sudo yum install libpng-devel freetype-devel 
    # ./deploy --ipython ~/virtenv-metrique

    # CLIENT + SITE CUBES ONLY
    # ./deploy --ipython -P metrique -P metriquec ~/virtenv-metrique

    # SERVER ONLY
    # ./deploy --ipython -P metriqued ~/virtenv-metrique

.. image:: https://d2weczhvl823v0.cloudfront.net/drpoovilleorg/metrique/trend.png

   :alt: Bitdeli badge
   :target: https://bitdeli.com/free

