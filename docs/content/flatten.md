---
title: "Flatten"
description: "Flatten the file hierarchy into fixed-size file names"
versionIntroduced: "v1.76"
---

# Flatten

The `flatten` backend flattens the file hierarchy: files keep their full
hierarchical names (`dir/sub/file.txt`) when accessed through this backend,
but are stored in the wrapped remote as flat, fixed-size file names at the
root of the remote. This is useful for storage systems that do not support
hierarchical names, or where uniform-length flat names are desirable.

The mapping between the real names and the flat names is stored in an index
file (`.rclone-flatten-index.json`) kept at the root of the wrapped remote.

## Configuration

To use it, first set up the underlying remote following the configuration
instructions for that remote. You can also use a local pathname instead of
a remote.

Now configure `flatten` using `rclone config`. We will call this one
`overlay` to separate it from the `remote` itself.

```text
No remotes found, make a new one?
n) New remote
s) Set configuration password
q) Quit config
n/s/q> n
name> overlay
Option selected.
Type of storage to configure.
Choose a number from below, or type in your own value
[snip]
XX / Flatten the file hierarchy into fixed-size file names
   \ "flatten"
[snip]
Storage> flatten
Remote to flatten.
Normally should contain a ':' and a path, e.g. "myremote:path/to/dir",
"myremote:bucket" or maybe "myremote:" (not recommended).
Enter a string value. Press Enter for the default ("").
remote> remote:path
Length of the flat file names generated in the wrapped remote.
Must be between 8 and 40. Flat names are derived from a hash of the
virtual path so they always have the same size. Increase this if you
have a very large number of files.
Enter a integer. Press Enter for the default (16).
name_length> 16
```

## Limitations

Since directories only exist virtually, some operations have
restrictions:

- Directories are created on the fly when files are uploaded; they
  cannot be created explicitly with `rclone mkdir`.
- `rclone rmdir` only succeeds on a directory that appears to be empty
  (it never removes anything, as directories do not exist in the
  wrapped remote).
- `rclone purge` removes all mapped files under a directory and their
  index entries.
- Renaming a file (`rclone moveto`) is cheap as it only rewrites the
  index entry, but the flat name in the wrapped remote is unchanged.
- Server-side copy and move are not used; copies are streamed through
  rclone.
- The index file is visible as a regular file in the wrapped remote
  but is hidden from listings in the flatten backend.
- If the index file is lost or damaged, the mapping is lost; the
  virtual files are then unreadable, even though their contents still
  exist in the wrapped remote.
