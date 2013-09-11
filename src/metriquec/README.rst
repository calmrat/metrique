.. image:: ../metriqued/metriqued/static/img/metrique_logo.png

Metrique Cubes
==============

**Author:** "Chris Ward" <cward@redhat.com>

**Sources:** https://github.com/drpoovilleorg/metrique

This repo contains default metrique cubes.
        
At this time, there is support for exracting from the 
following sources.
        
- JSON

- CSV

- PostgreSQL
- TEIID

Git
- commit

Jenkins
- build


Install
~~~~~~~

**Make sure you have read the `General Install Guide <https://github.com/drpoovilleorg/metrique/tree/master/README.rst>`_.**

Install the following *OS stuff* installed. The examples given 
below are fedora rpm package names::

    %> yum install postgresql postgresql-devel

Then install `metriquec` with::

    pip install metriquec

Known Issues
------------

None at this time
