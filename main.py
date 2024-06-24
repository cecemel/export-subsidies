import requests
import os
from datetime import datetime
import csv
import shutil
import pdb
import shutil
import argparse
import time
from functools import wraps
import multiprocessing

HOST="http://virtuoso:8890/sparql"
PATH_LOKET = "/data/app-digitaal-loket"

def retry_on_exception(retries: int = 3, delay: int = 1):
    """
    Decorator that retries a function if an exception occurs.
    Used mainly for fetching data from virtuoso
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            while attempts < retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    attempts += 1
                    if attempts >= retries:
                        raise
                    print(f"Exception occurred: {e}. Retrying {attempts}/{retries} in {delay} seconds...")
                    time.sleep(delay)
        return wrapper
    return decorator

@retry_on_exception(retries=3, delay=60)
def get_csv(sparql_endpoint, query, out_folder, filename):
    response = requests.get(
        sparql_endpoint,
        headers={'Accept': 'text/csv'},
        params={'query': query}
    )

    if response.status_code == 200:
        with open(f"{out_folder}/{filename}", 'a') as file:
            file.write(response.text)
    else:
        print(f"Request failed with status code: {response.status_code}")

@retry_on_exception(retries=3, delay=60)
def get_ttl(sparql_endpoint, query, out_folder, filename):
    response = requests.post(
        sparql_endpoint,
        headers={'Accept': 'text/plain'},
        data={'query': query}
    )

    if response.status_code == 200:
        with open(f"{out_folder}/{filename}", 'a') as file:
            file.write(response.text)
    else:
        print(f"Request failed with status code: {response.status_code}")

def get_public_graph_data(sparql_endpoint, out_folder, filename):
    query = """
       PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>

       CONSTRUCT {
         ?s ?p ?o.
       }
       WHERE {
        VALUES ?type {
          <http://www.w3.org/ns/prov#Location>
          <http://mu.semte.ch/vocabularies/ext/BestuurseenheidClassificatieCode>
          <http://data.vlaanderen.be/ns/besluit#Bestuursorgaan>
          <http://mu.semte.ch/vocabularies/ext/BestuursorgaanClassificatieCode>
          <http://publications.europa.eu/ontology/euvoc#Country>
          <http://data.vlaanderen.be/ns/besluit#Bestuurseenheid>
          <http://www.w3.org/2004/02/skos/core#ConceptScheme>
          <http://www.w3.org/2004/02/skos/core#Concept>
          <http://lblod.data.gift/vocabularies/subsidie/SubsidiemaatregelConsumptieStatus>
          <http://data.vlaanderen.be/ns/subsidie#SubsidiemaatregelAanbod>
          <http://lblod.data.gift/vocabularies/subsidie/SubsidiemaatregelAanbodReeks>
          <http://lblod.data.gift/vocabularies/subsidie/ApplicationFlow>
          <http://lblod.data.gift/vocabularies/subsidie/ApplicationStep>
          <http://data.vlaanderen.be/ns/subsidie#Subsidieprocedurestap>
          <http://data.europa.eu/m8g/PeriodOfTime>
          <http://data.europa.eu/m8g/Criterion>
          <http://data.europa.eu/m8g/RequirementGroup>
          <http://data.europa.eu/m8g/CriterionRequirement>
          <http://data.europa.eu/m8g/Requirement>
          <http://www.w3.org/ns/org#Organization>
        }

        GRAPH <http://mu.semte.ch/graphs/public> {
          ?s a ?type ;
            ?p ?o .
        }
      }
    """
    get_ttl(sparql_endpoint, query, out_folder, filename)
    # add a graph file
    graph_file = replace_extension(filename, '.graph')
    with open(f"{out_folder}/{graph_file}", 'w') as file:
        file.write("http://mu.semte.ch/graphs/public")

def get_bestuurseenheden_uuid(sparql_endpoint, out_folder, filename):
   query ="""
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX besluit: <http://data.vlaanderen.be/ns/besluit#>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>

      SELECT DISTINCT ?uuid
      WHERE {
       ?eenheid a besluit:Bestuurseenheid;
         mu:uuid ?uuid.

       FILTER NOT EXISTS {
         VALUES ?class {
          <http://data.lblod.info/vocabularies/erediensten/BestuurVanDeEredienst>
          <http://data.lblod.info/vocabularies/erediensten/CentraalBestuurVanDeEredienst>
          <http://data.lblod.info/vocabularies/erediensten/RepresentatiefOrgaan>
         }
         ?eenheid a ?class.
         }
      }
      ORDER BY ?uuid
   """
   get_csv(sparql_endpoint, query, out_folder, filename)

   results = []
   with open(f"{out_folder}/{filename}", mode='r') as file:
       csv_reader = csv.reader(file)
       rows = list(csv_reader)[1:] # skip first row
       for row in rows:
           results.append(row[0]) # unnest

   return results

def get_subsidies_graph(sparql_endpoint, out_folder, filename, org_uri, target_org_uri):
    query = """
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      CONSTRUCT {{
        ?s ?p ?o.
       }}
       WHERE {{
         VALUES ?g {{
           <{0}>
         }}
         GRAPH ?g {{
           ?s ?p ?o
         }}
       }}
    """.format(org_uri)

    get_ttl(sparql_endpoint, query, out_folder, filename)
    # add a graph file
    graph_file = replace_extension(filename, '.graph')
    with open(f"{out_folder}/{graph_file}", 'w') as file:
        file.write(target_org_uri)

def get_users_linked_to_subsidy_graph(sparql_endpoint, out_folder, filename, subsidy_uri, org_uri):
    query = """
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      CONSTRUCT {{

         ?s a <http://xmlns.com/foaf/0.1/Person>;
           ?p ?o.
         ?s <http://xmlns.com/foaf/0.1/account> ?account.
         ?account ?accountP ?accountO.

         ?s <http://www.w3.org/ns/adms#identifier> ?identifier.
         ?identifier ?identifierP ?identifierO.

       }}
       WHERE {{
         VALUES ?orgGraph {{
           <{0}>
         }}

         GRAPH ?orgGraph {{
            ?s a <http://xmlns.com/foaf/0.1/Person>;
              ?p ?o.

            OPTIONAL {{
              ?s <http://xmlns.com/foaf/0.1/account> ?account.
              ?account ?accountP ?accountO.
            }}

            OPTIONAL {{
              ?s <http://www.w3.org/ns/adms#identifier> ?identifier.
              ?identifier ?identifierP ?identifierO.
            }}

         }}

         VALUES ?subsidyGraph {{
           <{1}>
         }}
         GRAPH ?subsidyGraph {{
           ?source ?sourceP ?s
         }}

       }}
    """.format(org_uri, subsidy_uri)

    get_ttl(sparql_endpoint, query, out_folder, filename)
    # add a graph file
    graph_file = replace_extension(filename, '.graph')
    with open(f"{out_folder}/{graph_file}", 'w') as file:
        file.write(org_uri)

def get_physical_files_in_subsidy_graph(sparql_endpoint, out_folder, filename, org_uri):
    query = """
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      SELECT DISTINCT ?s
       WHERE {{
         VALUES ?g {{
           <{0}>
         }}
         GRAPH ?g {{
           ?s <http://www.semanticdesktop.org/ontologies/2007/01/19/nie#dataSource> ?logicalFile;
              a <http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#FileDataObject>.
         }}
       }}
    """.format(org_uri)
    results = []
    get_csv(sparql_endpoint, query, out_folder, filename)
    with open(f"{out_folder}/{filename}", mode='r') as file:
        csv_reader = csv.reader(file)
        rows = list(csv_reader)[1:] # skip first row
        for row in rows:
            results.append(row[0]) # unnest

    return results

def get_mock_accounts(sparql_endpoint, out_folder, filename, graph_uri):
    query = """
      PREFIX ext: <http://mu.semte.ch/vocabularies/ext/>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      PREFIX foaf: <http://xmlns.com/foaf/0.1/>

      CONSTRUCT {{
        ?person a foaf:Person ;
          mu:uuid ?uuidPerson ;
          foaf:account ?account ;
          foaf:familyName ?label ;
          foaf:firstName ?classificationLabel ;
          foaf:member ?eenheid .

        ?account a foaf:OnlineAccount ;
          mu:uuid ?uuidAccount ;
          ext:sessionRole "SubsidiepuntGebruiker" ;
          foaf:accountServiceHomepage <https://github.com/lblod/mock-login-service> .
       }}
       WHERE {{

         VALUES ?orgGraph {{
           <{0}>
         }}

         GRAPH ?orgGraph {{
            ?person a foaf:Person ;
              mu:uuid ?uuidPerson ;
              foaf:account ?account ;
              foaf:familyName ?label ;
              foaf:firstName ?classificationLabel ;
              foaf:member ?eenheid .

            ?account a foaf:OnlineAccount ;
              mu:uuid ?uuidAccount ;
              ext:sessionRole ?role ;
              foaf:accountServiceHomepage <https://github.com/lblod/mock-login-service> .
         }}

         FILTER NOT EXISTS {{
           VALUES ?class {{
            <http://data.lblod.info/vocabularies/erediensten/BestuurVanDeEredienst>
            <http://data.lblod.info/vocabularies/erediensten/CentraalBestuurVanDeEredienst>
            <http://data.lblod.info/vocabularies/erediensten/RepresentatiefOrgaan>
           }}
           ?eenheid a ?class.
         }}

       }}
    """.format(graph_uri)

    get_ttl(sparql_endpoint, query, out_folder, filename)
    # add a graph file
    graph_file = replace_extension(filename, '.graph')
    with open(f"{out_folder}/{graph_file}", 'w') as file:
        file.write(graph_uri)

def process_data_for_bestuurseenheid(uuid, index, all_uuids, HOST, migrations_folder, csv_folder, PATH_LOKET, data_folder):
    print(f"Fetching all data for bestuurseenheden with uuid: {uuid}")
    print(f"This is {index + 1} of {len(all_uuids)}")

    orig_subsidy_graph = f"http://mu.semte.ch/graphs/organizations/{uuid}/LoketLB-subsidies"
    target_subsidy_graph = = f"http://mu.semte.ch/graphs/organizations/{uuid}/SubsidiepuntGebruiker"
    subsidy_ttl = get_timestamped_file_name(f'dump-graph-subsidies-{uuid}.ttl')
    get_subsidies_graph(HOST, migrations_folder, subsidy_ttl, orig_subsidy_graph, target_subsidy_graph)
    print("Dumped subsidy graph")

    users_graph = f"http://mu.semte.ch/graphs/organizations/{uuid}"
    users_ttl = get_timestamped_file_name(f'dump-graph-users-{uuid}.ttl')
    get_users_linked_to_subsidy_graph(HOST, migrations_folder, users_ttl, orig_subsidy_graph, users_graph)
    print("Dumped users data")

    mock_users_ttl = get_timestamped_file_name(f'mock-users-{uuid}.ttl')
    get_mock_accounts(HOST, migrations_folder, mock_users_ttl, users_graph)
    print("Dumped mock accounts for org graph")

    print("Starting with attachments")
    share_uris_csv = get_timestamped_file_name(f'physical-files-{uuid}.csv')
    file_uris = get_physical_files_in_subsidy_graph(HOST, csv_folder, share_uris_csv, orig_subsidy_graph)
    print(f"Found: {len(file_uris)} attachments for {uuid}")
    for i, share_uri in enumerate(file_uris):
        print(f"Copying is {i + 1} of {len(file_uris)} attachments for {uuid}")
        copy_bijlage(share_uri, PATH_LOKET, data_folder)

    print(f"Finished fetching all data for bestuurseenheden with uuid: {uuid}")
    print(f"This was for {index + 1} of {len(all_uuids)}")

def copy_bijlage(share_uri, path_loket, target_folder):
    # note mounting a folder over sshfs
    # mkdir /home/felix/tmp/remote-loket
    # sshfs charlie:/data/app-digitaal-loket /home/felix/tmp/remote-loket
    if not share_uri.startswith("share://"):
        return

    file_name = share_uri.removeprefix("share://")
    full_target = f"{target_folder}/{file_name}"

    if(os.path.exists(full_target)):
        print(f"{full_target} already downloaded...")
        return

    source_path = f"{path_loket}/data/files/{file_name}"
    if(os.path.exists(source_path)):
        shutil.copy2(source_path, full_target)
    else:
        print(f"Bijlage on {source_path} not found.")

def copy_remaining_files(path_loket, target_folder):
    copy_files_skip_existing(f"{path_loket}/data/files/subsidies", f"{target_folder}/subsidies")

def replace_extension(file_path, new_extension):
    base = os.path.splitext(file_path)[0]
    return f"{base}{new_extension}"

def get_timestamped_file_name(file_name):
    current_date = datetime.now()
    time.sleep(1) # to ensure time stamp move
    timestamp = current_date.strftime('%Y%m%d%H%M%S')
    file_name = f"{timestamp}-{file_name}"
    return file_name

def ensure_folder_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)

def copy_files_skip_existing(src, dest):
    ensure_folder_exists(dest)

    for item in os.listdir(src):
        src_path = os.path.join(src, item)
        dest_path = os.path.join(dest, item)

        if os.path.isdir(src_path):
            copy_files_skip_existing(src_path, dest_path) # recursive call
        else:
            if not os.path.exists(dest_path):
                shutil.copy2(src_path, dest_path)
            else:
                print(f"skipping {dest_path}, because it exists")

def parse_cli_arguments():
    def comma_separated_list(value):
        return value.split(',')

    parser = argparse.ArgumentParser(description='Export subisidy data')

    parser.add_argument('-s', '--sparql_endpoint', type=str, help='Loket Database SPARQL endpoint')
    parser.add_argument('-l', '--loket_path', type=str, help='Absolute path to loket application')
    parser.add_argument('-e', '--eenheden', type=comma_separated_list, help='Dump data for a specific list eenheden; provide uuids')
    parser.add_argument('-p', '--processes', type=int, help='This provides the number of parallel processes (default 1)')
    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = parse_cli_arguments()
    if args.sparql_endpoint:
        HOST = args.sparql_endpoint

    if args.loket_path:
        PATH_LOKET = args.loket_path

    print(f"Running with {HOST}, {PATH_LOKET}")

    out_folder = './output'
    ensure_folder_exists(out_folder)
    csv_folder = f"{out_folder}/csv"
    ensure_folder_exists(csv_folder)
    data_folder = f"{out_folder}/data/files"
    ensure_folder_exists(data_folder)
    migrations_folder = f"{out_folder}/migrations"
    ensure_folder_exists(migrations_folder)

    print("Getting bestuurseenheden UUID's")
    uuids_file = get_timestamped_file_name('bestuurseenheden_uuid.csv')
    uuids = get_bestuurseenheden_uuid(HOST, csv_folder, uuids_file)
    print(f"Found {len(uuids)} bestuurseenheden")

    print("Dumping public graph")
    public_graph_file = get_timestamped_file_name(f'dump-graph-subsidies-public.ttl')
    get_public_graph_data(HOST, migrations_folder, public_graph_file)
    print("Finished dumping public graph")

    all_uuids = uuids
    if args.eenheden:
        all_uuids = args.eenheden

    # START: parallelize the processing
    tasks = []
    for index, uuid in enumerate(all_uuids):
        tasks.append((uuid, index, all_uuids, HOST,
                      migrations_folder, csv_folder,
                      PATH_LOKET, data_folder))

    number_of_processes = 1
    if(args.processes):
        print(f"Running number of processes {args.processes}")
        number_of_processes = args.processes

    pool = multiprocessing.Pool(processes=number_of_processes)
    results = pool.starmap(process_data_for_bestuurseenheid, tasks)

    pool.close()
    pool.join()
    # END: parallelize the processing

    print("Finished org specific stuff.")

    mock_users_ttl = get_timestamped_file_name(f'mock-users-public.ttl')
    get_mock_accounts(HOST, migrations_folder, mock_users_ttl, "http://mu.semte.ch/graphs/public")
    print("Dumped mock accounts for public graph")

    print("Copying the remaining files")
    copy_remaining_files(PATH_LOKET, data_folder)
    print("Done")
