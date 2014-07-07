.. image:: static/src/metrique_logo.png
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

Python Data Warehouse and Information Platform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

metrique provides a simple python API to support
ETL workloads for extracting data from disperate sources, 
iteratively, rapidly and reproducibly, with transparent,
historical object persistence and tight clientside 
integration with popular python scientific computing libraries 
to faciliate creation and publication of a wide variety of analysis 
and reports, large and small. 

Backends currently supported are as follows:
 * PostgreSQL (sqlalchemy)
 * SQLite (sqlalchemy)
 * MongoDB (pymongo)

**Author:** "Chris Ward" <cward@redhat.com>
**Sources:** https://github.com/kejbaly2/metrique


Quick Install (auto-deploy -> virtenv)
--------------------------------------

The instructions given below assume fedora rpm package names::

    # prerequisite *os* packages
    sudo yum install python python-devel python-setuptools python-pip
    sudo yum install openssl git gcc gcc-c++ gcc-gfortran
    sudo yum install freetype-devel libpng-devel # matplotlib deps

    # optional MongoDB
    sudo yum install postgresql postgresql-devel postgresql-server
    # optional PostgreSQL
    sudo yum install mongodb mongodb-server

    # make sure our core package managers are up2date
    sudo pip install -U distribute setuptools

    # our installation directory should always be a py virtualenv
    sudo pip install virtualenv

    # get metrique sources
    git clone https://github.com/kejbaly2/metrique.git
    cd metrique

    # deploy metrique master branch into a virtual environment,
    # including dependencies. 
    # NOTE this can take ~5-10 minutes to compile everything from source!
    ./metrique.py -V ~/metrique.master deploy --all --develop

    # activate the virtual environment
    source ~/metrique.master/bin/activate

    ./metrique.py firstboot metrique

    # optional: setup default postgresql environment and start
    ./metrique.py firstboot postgresql
    # optional: edit ~/.metrique/postgresql_db/*.conf
    ./metrique.py postgresql start

    # optional: setup default mongodb environment and start
    ./metrique.py firstboot mongodb
    ./metrique.py mongodb start

    # launch ipython and start mining!
