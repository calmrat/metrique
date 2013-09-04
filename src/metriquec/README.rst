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
-------

First, make sure you have the following *OS stuff*
installed.

%> yum install postgresql postgresql-devel

Then install `metriquec` with::

    pip install metriquec
