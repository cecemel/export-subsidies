# Export Subsidies
Script to export subsidy-related data in preparation for app-subsidiepunt.
## Run
```
docker build -t cecemel/export-subsidies:latest .
# run with cli (-h for arguments)
docker run -it --rm -v "$PWD"/output:/app/output cecemel/export-subsidies:latest python main.py
```
#### Example
On my machine I run. This is an example, your parameters may vary.
Remember; localhost != localhost in the container.
```
docker run -it --rm -v "$PWD"/output:/app/output cecemel/export-subsidies:latest python main.py -s http://172.29.0.2:8892/sparql -l /home/felix/tmp/remote-loket -e 6463c55877def582f642cb5b3b4a7eab60d8d8dd9779fc45d87e4c8aa5d9a07e
```
### CLI options
```
usage: main.py [-h] [-s SPARQL_ENDPOINT] [-l LOKET_PATH] [-e EENHEDEN]

Export subisidy data

options:
  -h, --help            show this help message and exit
  -s SPARQL_ENDPOINT, --sparql_endpoint SPARQL_ENDPOINT
                        Loket Database SPARQL endpoint
  -l LOKET_PATH, --loket_path LOKET_PATH
                        Absolute path to loket application
  -e EENHEDEN, --eenheden EENHEDEN
                        Dump data for a specific list eenheden; provide uuids

```

### Tips
#### Getting IP Virtuoso on Charlie
```
virtuoso_name=`docker ps --filter "label=com.docker.compose.project=app-digitaal-loket" --filter "label=com.docker.compose.service=virtuoso" --format "{{.Names}}"`
virtuoso_ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{println .IPAddress}}{{end}}'  $virtuoso_name |head -n1`
echo $virtuoso_ip
```
Please explore this snippet to figure out the variables depending on the server.
