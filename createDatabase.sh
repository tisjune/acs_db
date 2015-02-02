echo "parsing data"
python parser.py
echo "creating DB"
sqlite3 data/ACS.db < script_out/create.sql 
echo "loading data"
sqlite3 data/ACS.db < script_out/load.txt
