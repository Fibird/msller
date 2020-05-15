#!/bin/bash

ceph mgr module disable msller

sleep 1

ceph mgr module enable msller --force
