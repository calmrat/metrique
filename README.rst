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

General
~~~~~~~
If you are interested only to connect to an existing 
metriqued instance, follow the instructions under
#Client section below.

If you only want to deploy your own `metriqued`
server, follow the instructions under the #Server
section too.

(suggested) Install virtualenv and create a new virtual 
environment for metrique; use --no-site-packages option. 
Call bin/activate, once installed. Then, use `bin/pip`
for pip installs.

Installing via pypi, downloads, compiles and installs
**quite** a few dependencies.  Be prepared to wait
10 minutes for installation to finish.

Make sure you have the following *os stuff* installed
first, before pip-in'.  The examples given below are 
fedora rpm package names (eg, `yum install ...`)::

    git python python-devel python-pip python-setuptools 
    gcc gcc-c++ libpng-devel freetype-devel kerberos-devel

If you see any error, not referenced in-line, google.


Client
~~~~~~

In addition to General, make sure you have the following 
*os stuff* installed.  The examples given below are 
fedora rpm package names::

    postgresql postgresql-devel mysql-devel

Install `metrique` via pypi::

    $> pip install metrique # client


FYI: Default installed cubes are found 
at `$PY_SITE_PACKAGES/metrique/cubes/` and 
`$HOME/.metrique/cubes`.

If you have any of your own cubes to install, i suggest
copying them to your user cubes directory now.

Then,  launch a python shell, like ipython notebook
and start metrique'ing::

    $> ipython notebook --pylab=inline

To launch a metrique client in ipython::

    >>> from metrique import pyclient

Then, to load a new pyclient instance::

    >>> m = pyclient(host="$METRIQUE_HOST", port="$METRIQUE_PORT")

Ping the server to ensure your connected. If all 
is well, metriqe server should pong your ping!::

    >>> m.ping()
    >>> PONG ($HOSTNAME)

Then, assuming there is data in the metriqued host you're
connected to, try running an example `git_commit` query job,
like the following::

    >>> q = m.fetch(cube='git_commit', fields='author, committer_ts') 
    >>> q.groupby(['author']).size().plot(kind='barh')
    >>> <matplotlib.axes.AxesSubplot at 0x6f77ad0>




Server
~~~~~~

If you are interested to run your own metriqued instance(s),
the following describes how to install and configure
a `metriqued` instance.

In addition to General, make sure you have the following 
*os stuff* installed.  The examples given below are 
fedora rpm package names::

    mongodb-server 

**mongodb**

You must first install MongoDB. Google for instructions,
depending on your OS. Then, make sure it's started.

**metriqued**

Install `metriqued` via pypi::

    $> pip install metriqued # server

Run `metriqued-setup` if you changed any defaults.

To start metrique, run::
    
    $> metrique-server start

Then, after launching a pyclient instance (see Client above),
you can try running an example `gitrepo_commit` extract job::

    ...
    >>> m = pyclient(..., cube='gitrepo_commit')
    >>> m.extract(uri='https://github.com/drpoovilleorg/metrique.git')
