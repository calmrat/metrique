Release Notes
=============
0.3.2
-----
* refactored MetriqueObject class -> simplier metrique_object function
* removed mongodb support

0.3.1
-----
* data typecasting and normalization happens now at the container level
* and a bunch of bug fixes
* simplified, consistent object type/state checker functions added to utils
* more tests!
* default container backend is sqlite+sqlalchemy
* updated and abstracted mql parser
* from __future__ import unicode_literals, absolute_imports

0.3.0
-----
* metriqued server dropped; metriquet -> github.com/kejbaly2/tornadohttp
* new MetriqueObject and MetriqueContainer Mapping classes
* refactored (moved into) MongoDBProxy and MongoDBContainer Mapping classes
* remaining metrique libraries merged into single metrique package
* using joblib for parallel job execution (not futures)

0.2.6
-----
* auto-deploy and service management scripts added (`metrique`)
* mongodb replicaset support
* metriqued server failover pool support
* py26 no longer supported
* metriquet - generic, reusable tornado server class
* object consistent hashing bugfix

0.2.*
-----
* metriquec.cubes module added with generic cube reference classes
* initial test framework added (pytest)
* plotrique imported into source repo
* user and cube profile api added
* authentication api added
* basesql activity import improvements (parallel support, single pass)
* object consistent hashing support
* cube refactors (use pandas.csv_import, etc)
* lots of docs added (still much  more to go!)
* app universal default paths set (~/.metrique)

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
