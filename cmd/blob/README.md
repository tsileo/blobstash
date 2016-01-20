# blob

**blob** is a tiny command line too to interact with BlobStash blob store.

 - read from STDIN, upload and output hash
 - output raw blob from hash

 That's it!

## Basic Usage

### Upload a blob from STDIN

```shell
$ echo ok | blob -
c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5
```

### Output raw blob

```shell
$ blob c0f1480a26c2fd4deb8e738a52b7530ed111b9bcd17bbb09259ce03f129988c5
ok
```

## Advanced Usage

Actually, **blob** can do a little more than uploading from STDIN and outputting raw blobs.

### Namespace

The `-ns` flag set the namespace for the upload.

```shell
$ echo ok | blob -ns myns -
```

### Logs

You can keep tracks of your updated blobs from the CLI (with an optional comments)

```shell
$ cat .zshrc | blob -comment "my zshrc file" -save -
```
