#### Improvements

* VFS-5989 Regular files can now be shared. Also both files and
  directories can be shared multiple times. Due to those changes
  share REST API was reworked.
* VFS-5901 Application config can now be customized with arbitrary number
  of config files added to /etc/op_worker/config.d/ directory.


#### Bugfixes


#### Removals

* VFS-5989 Removed old share REST API operating on file paths/ids.
