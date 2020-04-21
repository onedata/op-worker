#### Improvements

* VFS-6140 Added new REST api for file operations, in both normal mode
  and share mode, accessible under `/data/{fileId}` path. Also added
  `/lookup-file-id/{filePath}` endpoint allowing to resolve file path
  into fileId.
* VFS-5989 Regular files can now be shared. Also both files and
  directories can be shared multiple times. Due to those changes
  share REST API was reworked.
* VFS-5901 Application config can now be customized with arbitrary number
  of config files added to /etc/op_worker/config.d/ directory.


#### Bugfixes


#### Removals & Deprecations

* VFS-6140 Old file related REST api operations become deprecated and will
  be removed in next major release. They are replaced with new data api,
  for more see `Improvements`.
* VFS-5989 Removed old share REST API operating on file paths/ids.
