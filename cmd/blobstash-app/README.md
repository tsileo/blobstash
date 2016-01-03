# blobstash-app

**Work in progress**.

## Quick start

```shell
# List your apps
$ blobstash-app apps

# Register a app as `myappid`
$ blobstash-app register myappid /path/to/file.lua

# Register a "public" app
$ blobstash-app -public register myappid /path/to/file.lua

# Register a "public" app, but don't save it, just keep it in memory
# (will be lost at next restart)
$ blobstash-app -public in-mem register myappid /path/to/file.lua

# Display basic stats for the given appID
$ blobstash-app stats myappid

# Show logs for the given appID
$ blobstash-app logs myappid
```
