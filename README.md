# Introduction

A ceph mgr plugin/module for computing max stable line, msller.

# Installing

Just copy it into ceph mgr module path:

```
cp msller -r /usr/lib/ceph/mgr/
```

You can get your mgr module path by:

```
ceph daemon osd.0 config show | grep mgr_module_path
```

# Reference

1. Ceph Documentation: [CEPH-MGR PLUGIN AUTHOR GUIDE](https://docs.ceph.com/docs/mimic/mgr/plugins/)
