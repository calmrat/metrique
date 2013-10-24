.. image:: ../metriqued/metriqued/static/img/metrique_logo.png

Metrique Server
===============

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique


Install
~~~~~~~

**Make sure you have read the `General Install Guide <https://github.com/drpoovilleorg/metrique/tree/master/README.rst>`_.**

Install the following *OS stuff* installed. The examples given 
below are fedora rpm package names::

    yum install krb5-devel

Make sure you have MongoDB installed. Instructions 
can be found on the web.  For Fedora, for example, 
see `10-gen installation instructions <http://bit.ly/1dFqC1y>`_

A default mongodb.conf file is available 
`here <https://github.com/drpoovilleorg/metrique/blob/master/src/metriqued/confs/mongodb.conf>`_

Launch Mongodb. 

Install metriqued with pip::

    pip install metriqued

Then, start `metriqued` by running::
    
    $> metriqued start

Assuming you have 2.7+, you can try running a `gitrepo_commit` etl 
job, for example, in ipython::

    $> ipython notebook --pylab=inline
    ...
    >>> from metrique import pyclient
    >>> m = pyclient(cube='gitrepo_commit')
    >>> m.ping()
    >>> m.extract(uri='https://github.com/drpoovilleorg/metrique.git')
    >>> q = m.find('gitrepo_commit', 'author, committer_ts') 
    >>> q.groupby(['author']).size().plot(kind='barh')
        <matplotlib.axes.AxesSubplot at 0x6f77ad0>

Or you can analyse data from apache's jenkins instance (or other)::

    $> ipython notebook --pylab=inline
    ...
    >>> from metrique import pyclient
    >>> m = pyclient(cube='jkns_build')
    >>> # WARNING THIS WILL TAKE A WHILE!
    >>> # Just let it run for a minute, then kill
    >>> # it... as a demo.
    >>> m.extract(uri='http://builds.apache.org')
    >>> builds = m.find(fields='__all__', limit=100) 

Known Issues
------------

None at this time
