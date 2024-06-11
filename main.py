import requests
import os
from datetime import datetime
import csv
import shutil
import pdb

HOST="http://localhost:8892/sparql"
PATH_LOKET = ""

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
        file.write("<http://mu.semte.ch/graphs/public>")

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
   """
   get_csv(sparql_endpoint, query, out_folder, filename)

   results = []
   with open(f"{out_folder}/{filename}", mode='r') as file:
       csv_reader = csv.reader(file)
       header = next(csv_reader)
       next(csv_reader) # skip  firstrow
       for row in csv_reader:
           results.append(row[0])

   return results

def get_subsidies_graph(sparql_endpoint, out_folder, filename, org_uri):
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
        file.write(org_uri)

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
        header = next(csv_reader)
        next(csv_reader) # skip  firstrow
        for row in csv_reader:
            results.append(row[0])

    return results

def replace_extension(file_path, new_extension):
    base = os.path.splitext(file_path)[0]
    return f"{base}{new_extension}"

def get_timestamped_file_name(file_name):
    current_date = datetime.now()
    timestamp = current_date.strftime('%Y%m%d%H%M%S')
    file_name = f"{timestamp}-{file_name}"
    return file_name

def ensure_folder_exists(path):
    if not os.path.exists(path):
        os.makedirs(path)
    print(f"Folder '{path}' is ready.")

if __name__ == "__main__":
    out_folder = './output'
    ensure_folder_exists(out_folder)
    csv_folder = f"{out_folder}/csv"
    ensure_folder_exists(csv_folder)
    data_folder = f"{out_folder}/data"
    ensure_folder_exists(data_folder)
    migrations_folder = f"{out_folder}/migrations"
    ensure_folder_exists(migrations_folder)

    uuids_file = get_timestamped_file_name('bestuurseenheden_uuid.csv')
    uuids = get_bestuurseenheden_uuid(HOST, csv_folder, uuids_file)

    public_graph_file = get_timestamped_file_name(f'dump-graph-subsidies-public.ttl')
    get_public_graph_data(HOST, migrations_folder, public_graph_file)

    for uuid in [uuids[0]]:
        subsidy_graph = f"http://mu.semte.ch/graphs/organizations/{uuid}/LoketLB-subsidies"
        subsidy_ttl = get_timestamped_file_name(f'dump-graph-subsidies-{uuid}.ttl')
        get_subsidies_graph(HOST, migrations_folder, subsidy_ttl, subsidy_graph)

        users_graph = f"http://mu.semte.ch/graphs/organizations/{uuid}"
        users_ttl = get_timestamped_file_name(f'dump-graph-users-{uuid}.ttl')
        get_users_linked_to_subsidy_graph(HOST, migrations_folder, users_ttl, subsidy_graph, users_graph)

        share_uris_csv = get_timestamped_file_name(f'physical-files-{uuid}.csv')
        get_physical_files_in_subsidy_graph(HOST, csv_folder, share_uris_csv, subsidy_graph)
