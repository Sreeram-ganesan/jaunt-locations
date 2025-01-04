### Developer Wiki

#### Overview
1. Require - duckdb, go(1.16 and above), python(3.10.12 and above)
2. Get the locations.db file from repo owner and place in data/ folder
3. Get the json file dumps of locations as json file to be inserted into local duck db for using the script from jaunt postgres tables(t_content); currently two file are require - t_content_202501021703_new.jsonl, t_content_rawdata_202412011408.jsonl and make sure its in data/ folder
3. run -  go mod init jaunt
4. run - go mod tidy
5. use pip to install the required libs in 10_filterNYlocations.py
6. run - go run cmd/main.gp
7. python scripts/10_filterNYLocations.py to obtain the maps for visualization of dupes