.. image:: ../metriqued/metriqued/static/img/metrique_logo.png

Metrique Client
===============

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/kejbaly2/metrique


Install
~~~~~~~

Install metrique::

    pip install metrique

To start using `metrique`, in ipython notebook, for example::

    $> ipython notebook --pylab=inline
    ...
    >>> from metrique import pyclient

Then, to load a new pyclient instance for querying::

    >>> m = pyclient(cube='csvdata')

Now you can start exploring what data already exists 
on the host::

    >>> m.ls()
    ...
    >>> m.sample_cube_fields()
    ...

And assuming you have a metriqued host to connect to,
with data in already, you can get to work!::

    >>> q = m.find('gitrepo_commit', 'author, committer_ts') 
    >>> q.groupby(['author']).size().plot(kind='barh')
        <matplotlib.axes.AxesSubplot at 0x6f77ad0>



Known Issues
------------

Import Warnings
~~~~~~~~~~~~~~~
When running (eg) `from metrique import pyclient` for the
first time in a session, you might see a warning like::

    Module bson was already imported from ...

This warning can be safely **ignored**. It's only a warning.
