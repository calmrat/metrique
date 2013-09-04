.. image:: ../metriqued/metriqued/static/img/metrique_logo.png

Metrique Client
===============

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique


Installation
------------

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

Ipython
-------
We strongly suggest installing and using IPython 
notebook instead of standard python shell for 
interactive data exploration.

To install ipython notebook install the following 
*OS* packages::

    yum install libpng-devel freetype-devel 

Then install ipython with pip::

    pip install ipython

If you see any error, not otherwise mentioned here, Google.



Client
~~~~~~

Install metrique::

    pip install metrique

To start using `metrique`, in ipython notebook, for example::

    $> ipython notebook --pylab=inline
    ...
    >>> from metrique import pyclient

Then, to load a new pyclient instance for querying::

    >>> m = pyclient(host='http://127.0.0.1')

Ping the server to ensure your connected. If all 
is well, metriqe server should pong your ping!::

    >>> m.ping()
        PONG ($METRIQUED_HOSTNAME)

Now you can start exploring what data already exists 
on the host::

    >>> m.list_cubes()
    ...
    >>> m.list_cube_fields('cube_name_here')
    ...

And assuming you have a metriqued host to connect to,
with data in already, you can get to work!::

    >>> q = m.fetch('gitrepo_commit', 'author, committer_ts') 
    >>> q.groupby(['author']).size().plot(kind='barh')
        <matplotlib.axes.AxesSubplot at 0x6f77ad0>


**If you plan to extract data (optional)**, see
`metriquec <https://github.com/drpoovilleorg/metrique/tree/master/src/metriquec>`_ to install metriques default cubes.


Known Issues
------------

Import Warnings
~~~~~~~~~~~~~~~
When running (eg) `from metrique import pyclient` for the
first time in a session, you might see a warning like::

    Module bson was already imported from ...

This warning can be safely **ignored**. It's only a warning.

Configuration
~~~~~~~~~~~~~
When loading a server instance, if you get an error about loading
`http_api.json`, run `metrique-setup`
