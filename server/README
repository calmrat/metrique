.. image:: metrique/server/static/img/metrique_logo.png

Metrique
========

Python/MongoDB Information Platform and Data Warehouse

*Metrique help bring data into an intuitive, indexable 
data object collection that supports quick snapshotting, 
advanced ad-hoc querying, including (mongodb) aggregations
and mapreduce, along with python, ipython, pandas,
numpy, matplotlib, and so on, is fully integrated 
with the scientific python computing stack. I hope
so anyway. :)*

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique


Installation
------------

You must first install MongoDB. Then, to continue, 
make sure it's started.


**Metrique**
(suggested) Install virtualenv and create a new virtual 
environment for metrique. Activate it. 

Install metrique::

    python-pip install metrique -r requirements.txt

.. note::
     Make sure you have gcc and python-devel libraries installed

.. note::
     If you see 'Connection reset by peer' error, try option: --use-mirrors

.. note::
     If you see any other error, Google.

You should now be ready to go. 

Run metrique-server-config.py if you changed any defaults.

To start metrique, run::
    
    $[/metrique/server/bin] metrique-server start [2|1|0] [1|0]

Where argv are debug on+/on/off and async on/off respectively.

It's suggested to run :mod:metrique-server-setup after install
as well, especially if you changed any default values of your
mongo or metrique servers, they're hosted on a different
ip than `localhost`. 


**Client**
If the metrique server is running on anything other than 
`http://127.0.0.1`, run `metrique-client-setup`.

Then,  launch a python shell. We suggest ipython notebook. 

As of this time, :mod:cubes can be found in global
metrique namespace or local to the running user. 

Default: `~/.metrique/cubes`

To quickly make those cubes available in sys.path::

    IN  [] from metrique.client.cubes import set_cube_path
    IN  [] set_cube_path()  # defaults to '~/.metrique/cubes'

Then, to load a cube for extraction, query or administration,
import::

    IN  [] from git_repo.gitrepo import Commit
    IN  [] g = Commit(config_file=None, uri=None)

Ping the server to ensure your connected. If all 
is well, metriqe server should pong your ping!::

    IN  [] g.ping()
    OUT [] pong!

Try running an example ::mod:git_commit etl job, for example::

    IN  [4] g.extract("git_commit")

Then, analyse away::

    IN  [5] q = c.query.fetch('git_commit', 'author, committer_ts') 
    IN  [6] q.groupby(['author']).size().plot(kind='barh')
    OUT [6] <matplotlib.axes.AxesSubplot at 0x6f77ad0>
