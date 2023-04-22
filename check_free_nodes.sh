rm -f test_pbs.json
pbsnodes -F json -a >> test_pbs.json
python util_pbs.py