#### Improvements

* VFS-6140 Added new REST api for file operations, in both normal mode
  and share mode, accessible under `/data/{fileId}` path. Also added
  `/lookup-file-id/{filePath}` endpoint allowing to resolve file path
  into fileId.


#### Bugfixes


#### Removals & Deprecations

* VFS-6140 Old file related REST api operations become deprecated and will
  be removed in next major release. They are replaced with new data api,
  for more see `Improvements`.
