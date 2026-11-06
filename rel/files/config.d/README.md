This directory can be used to add custom modifications to the op-worker app config.
Any file matching '*.config' placed in this directory will be read on op-worker startup.
Files are ordered alphabetically, with later files overriding values from earlier.
Sample file names:
```
01-low-priority.config
50 medium-priority.config
90_high-priority.config
```

Settings placed in overlay.config in the /etc/op_worker/ directory take precedence over any
settings defined here.
