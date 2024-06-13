# Export Subsidies
Script to export subsidy-related data in preparation for app-subsidiepunt.
### CLI options
```
usage: main.py [-h] [-s SPARQL_ENDPOINT] [-l LOKET_PATH] [-e EENHEDEN] [-p PROCESSES]

Export subisidy data

options:
  -h, --help            show this help message and exit
  -s SPARQL_ENDPOINT, --sparql_endpoint SPARQL_ENDPOINT
                        Loket Database SPARQL endpoint
  -l LOKET_PATH, --loket_path LOKET_PATH
                        Absolute path to loket application
  -e EENHEDEN, --eenheden EENHEDEN
                        Dump data for a specific list eenheden; provide uuids
  -p PROCESSES, --processes PROCESSES
                        This provides the number of parallel processes (default 1)
```
## Run
### Docker
```
docker build -t cecemel/export-subsidies:latest .
```
### Adding this to docker-compose loket
In the `docker-compose.override.yml` in `app-digitaal-loket`
```
# (...)
export-subsidy-data:
  image: cecemel/export-subsidies:latest
  volumes:
    -  ./:/data/app-digitaal-loket
    - ./export-subsidy-data:/app/output
  entrypoint: [ "python", "main.py", "-p", "4" ]
```
#### Example
On my machine I run. This is an example, your parameters may vary.
```
# (...)
export-subsidy-data:
  image: cecemel/export-subsidies:latest
  volumes:
    -  ./:/data/app-digitaal-loket
    - ./export-subsidy-data:/app/output
  entrypoint: [ "python", "main.py", "-s", "http://virtuoso:8890/sparql", "-l", "/data/app-digitaal-loket", "-e", "6463c55877def582f642cb5b3b4a7eab60d8d8dd9779fc45d87e4c8aa5d9a07e"]
```
### Tips
#### changing the script => requires new build
As the title says.
