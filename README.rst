.. image:: src/metriqued/metriqued/static/img/metrique_logo.png

Metrique
========

Python/MongoDB Information Platform and Data Warehouse

*Metrique is dataglue. It can help bring data into an 
intuitive, indexable data object collection that 
supports transparent historical version snapshotting, 
advanced ad-hoc server-side querying, including (mongodb) 
aggregations and (mongodb) mapreduce, along with python, 
ipython, pandas, numpy, matplotlib, and so on, is well
integrated with the scientific python computing stack. 

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique


Installation
------------

You must first install MongoDB. Then, make sure it's started.


**Metrique**
(suggested) Install virtualenv and create a new virtual 
environment for metrique. Activate it. 

Make sure you have the following *stuff* installed. The 
examples given below are fedora rpm package names::

    mongodb-server git python python-setuptools 
    gcc gcc-c++ python-devel libpng-devel freetype-devel
    postgresql postgresql-devel kerberos-devel
    mysql-devel

Install metrique and metriqued::

    pip install metrique metriqued

If you see any error, Google.

Otherwise, you should now be ready to go. 

Run metriqued-config if you changed any defaults.

To start metrique, run::
    
    $> metriqued-server start [2|1|0] [1|0]

Where argv are debug on+/on/off and async on/off respectively.


**Client**
If the metrique server is running on anything other than 
`http://127.0.0.1`, run `metrique-setup`.

Then,  launch a python shell. We suggest ipython notebook. 

As of this time, :mod:cubes can be found in global
metrique namespace or local to the running user. 

Default: `~/.metrique/cubes`

If you have any of your own cubes to install, i suggest
copying them there now.

To start using them::

    IN  [] from metrique import pyclient

Then, to load a cube for extraction, query or administration,
import::

    IN  [] g = pyclient(cube="gitrepo_commit"")

Ping the server to ensure your connected. If all 
is well, metriqe server should pong your ping!::

    IN  [] g.ping()
    OUT [] pong!

Try running an example ::mod:git_commit etl job, for example::

    IN  [] g.extract(uri='https://github.com/drpoovilleorg/metrique.git')

Then, analyse away::

    IN  [] q = c.query.fetch('git_commit', 'author, committer_ts') 
    IN  [] q.groupby(['author']).size().plot(kind='barh')
    OUT [] <matplotlib.axes.AxesSubplot at 0x6f77ad0>
