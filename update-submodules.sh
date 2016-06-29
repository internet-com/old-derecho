#!/bin/bash
#Shortcut to update all submodules to their latest version

git submodule foreach git pull origin master
git add rdmc sst mutils serialization
