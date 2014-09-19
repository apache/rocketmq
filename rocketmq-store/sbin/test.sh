#!/bin/sh
nohup sh put.sh > put.txt &
nohup sh showload.sh > load.txt &
nohup sh showiostat.sh > iostat.txt &
nohup sh showmaptotal.sh > map.txt &
nohup sh showcpu.sh > cpu.txt &