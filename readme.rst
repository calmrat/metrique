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

**MongoDB**
We assume you have a MongoDB server up and running. If it's running
binded to 127.0.0.1 with noauth = true and ssl = false, metrique
should find it automatically.

If it's not localhost, then you must remember to run metrique-server-setup after installing metrique.

**Metrique**
(optional) Install virtualenv and create a new virtual environment for metrique.

Then, install metrique. 

    python-pip install metrique

.. note::
     If you see 'gcc' error, try installing gcc and python-devel libraries first

.. note::
     If you see 'Connection reset by peer' error, try option: --use-mirrors

At this point, if you have default mongob setup and it's started, you 
should be ready to go. Otherwise, run metrique-server-config.py.

Run::
    
    $> metrique-server start [1|0] [1|0]

Where 1|0 for argv 0 and 1 are debug on and async on respectively


**Client**

Metrique offers a http api, with a convenient pyclient that understands that api.

Assuming your server is configured and metrique-server is running, launch ipython.

>>> from metrique.client import pyclient
>>> c = pyclient()
>>> c.ping()
[m] pong!

If all is well, the server server should return your ping.
