Extractor
=========
A simple framework to extract data from entities and export it to the
App Engine blobstore or Google Cloud Storage.

About
-----
This is a simple framework to systematically map over data using a
user-provided query, pass it to a user-defined filter/parser function that will
emit a string to be appended to the output, or None to indicate no output is
desired.

The framework provides a simple facility to allow user-defined sharding of the
"scan" space, to improve performance.  Note that the data is scanned and output
in parallel, so the output may not be in order of the scanned data.


NOTICE
======
Requirements to commit here:
  - Branch off master, PR back to master.
  - Your code should pass [Flake8](https://github.com/bmcustodio/flake8).
  - Good docstrs are required.
  - Good [commit messages](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html) are required.

