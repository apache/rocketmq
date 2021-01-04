#!/usr/bin/env bash
if [ "$1" = "" ] || [ "$1" = "-h" ]; then
  echo "Usage: bump-version.sh version"
  exit -1
fi

version=$1
mvn versions:set -DnewVersion=$version
find . -name *.versionsBackup -exec rm {} \;
