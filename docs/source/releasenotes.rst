Metrique Release Notes
======================

0.1.3
-----

* server no longer runs cube extractions;
  instead, server exports save_objects and
  clients are expected to push data through
  the api
* refactored/updated client/server apis
* enhanced debuging
* basic auth
* SSL support
* tons of docstring updates
* update source code autodoc sphinx building
* csv now gets its URI on .extract() not __init__()
* basecube refactor
* built-in cubes: jkns, CSV, JSON, git
* client side celery periodic, automated extraction
  integration
* tons of client pandas/plotting additions to help
  with analysis of historical data, including a
  metrique result object (subclass of pandas.dataframe)
* enhanced logging
* new .distinct() method to return all unique values 
  of a given field and given collection of documents
* json decoder for handing ObjectIds and datetimes
* JSONConfig now acts more like a dictionary
* version and requirements (pypi) info included
