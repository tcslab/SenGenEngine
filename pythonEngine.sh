#!/bin/bash

git pull origin master
rm nohup.out
nohup python SenGenEngine.py&
