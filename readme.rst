Metrique
========

Python/MondoDB Information Platform and Data Warehouse

**Author:** "Chris Ward" <cward@redhat.com>, 

**Contributors:** 
 * "Jan Grec" <jgrec@redhat.com>
 * "Juraj Niznan" <jniznan@redhat.com>
 * "Gal Leibovici" <gleibovi@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique

*Metrique helps you bring data, structured and unstructured into an 
intuitive, indexable data object collection that supported transparent
timebased snapshotting, advanced ad-hoc querying and is fully integrated 
with the scientific python computing stack.*

Installation
------------

Install virtualenv, then create a new virtual environment, as such::

    cd $HOME
    virtualenv vroot --no-site-packages --distribute

Activate the virtualenv::

    source $HOME/vroot/bin/activate

Install dependencies::

    easy_install -U distribute    
    python bin/pip install requests
    python bin/pip install simplejson
    python bin/pip install tornado
    python bin/pip install futures
    python bin/pip install pymongo
    python bin/pip install bson
    python bin/pip install pql
    python bin/pip install python-dateutil
    python bin/pip install decorator
    python bin/pip install numpy
    python bin/pip install pandas
    python bin/pip install psycopg2  # requires postgresql-devel
    python bin/pip install MySQL-python  # requires mysql-devel
    python bin/pip install gitdb

.. note::
     If you see 'gcc' error, try installing gcc and python-devel libraries first

.. note::
     If you see 'Connection reset by peer' error, try option: --use-mirrors
