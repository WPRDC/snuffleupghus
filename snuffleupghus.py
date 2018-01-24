import sys, re, csv, json, time, ckanapi, requests
from marshmallow import fields, pre_load, post_load

from pprint import pprint

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl

from parameters.local_parameters import SETTINGS_FILE, DATA_PATH

class EventsSchema(pl.BaseSchema): 
    event_name = fields.String(allow_none=False)
    recurrence = fields.String(allow_none=True)
    program_or_facility = fields.String(allow_none=True)
    neighborhood = fields.String(allow_none=True)
    address = fields.String(allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization = fields.String(allow_none=True)
    category = fields.String(allow_none=True)
    recommended_for = fields.String(allow_none=True)
    requirements = fields.String(allow_none=True)
    event_phone = fields.String(allow_none=True)
    event_narrative = fields.String(allow_none=True)
    schedule = fields.String(allow_none=True)
    holiday_exception = fields.String(allow_none=True)
    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more apropriate.
    class Meta:
        ordered = True

    @pre_load
    def get_lat_and_lon(self, data):
    # Split geocoordinates field ("Program Lat and Long") into new latitude and longitude fields.
        latpart, lonpart = data['lat_and_lon'].split(',')
        _, latitude = latpart.split(': ')
        _, longitude = lonpart.split(': ')
        data['latitude'] = float(latitude)
        data['longitude'] = float(longitude)
        del data['lat_and_lon']

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def get_package_parameter(site,package_id,parameter,API_key=None):
    # Some package parameters you can fetch from the WPRDC with
    # this function are:
    # 'geographic_unit', 'owner_org', 'maintainer', 'data_steward_email',
    # 'relationships_as_object', 'access_level_comment',
    # 'frequency_publishing', 'maintainer_email', 'num_tags', 'id',
    # 'metadata_created', 'group', 'metadata_modified', 'author',
    # 'author_email', 'state', 'version', 'department', 'license_id',
    # 'type', 'resources', 'num_resources', 'data_steward_name', 'tags',
    # 'title', 'frequency_data_change', 'private', 'groups',
    # 'creator_user_id', 'relationships_as_subject', 'data_notes',
    # 'name', 'isopen', 'url', 'notes', 'license_title',
    # 'temporal_coverage', 'related_documents', 'license_url',
    # 'organization', 'revision_id'
    try:
        ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
        metadata = ckan.action.package_show(id=package_id)
        desired_string = metadata[parameter]
        #print("The parameter {} for this package is {}".format(parameter,metadata[parameter]))
    except:
        raise RuntimeError("Unable to obtain package parameter '{}' for package with ID {}".format(parameter,package_id))

    return desired_string
    
def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def parse_file(filepath,basename):
    replacement_headers = {"Recurring, One-Time or One-on-One?": "recurrence",
                            "Program (Facility) Name": "program_or_facility",
                            "Program Neighborhood": "neighborhood",
                            "Program Address": "address",
                            "Program Lat and Long": "lat_and_lon",
                            "Organization Name": "organization",
                            "(Event) Requirements": "requirements",
                            "(Event) Recommended For :": "recommended_for"}

    # Use local file
    f = open(filepath,'r', newline='')
    # "If newline='' is not specified, newlines embedded inside quoted fields will not be interpreted correctly,..."
    #   - the official Python documentation
    # This unfortunately didn't have the desired effect as there's still a lingering newline in 
    # the last field read.
    f.readline() #f.next() # Skip the first line, since it gives the delimiter.
    reader = csv.DictReader(f, delimiter='|', quotechar='"') 
    list_of_dicts = list(reader) 
    f.close()
    f = open(filepath,'r')
    _ = f.readline()
    headers = f.readline()[:-1].split('|')

    new_headers = []
    for header in headers:
        if header in replacement_headers.keys():
            old_header = header
            header = replacement_headers[header]
            for x in list_of_dicts:
                x[header] = x[old_header]
                del x[old_header]
        new_headers.append(header)
    print("new_headers = {}".format(new_headers))
    dpath = '/'.join(filepath.split("/")[:-1]) + '/'
    if dpath == '/':
        dpath = ''
    outputfilepath = "{}tmp/{}.csv".format(dpath,basename)
    # [ ] If the temp directory doesn't exist, create it. 
    write_to_csv(outputfilepath,list_of_dicts,new_headers)
    return list_of_dicts, headers, outputfilepath

def fuse_cats(e,d):
    # Combine Category One and Category Two into a |-delimited category field
    cat1 = e['Category One']
    cat2 = e['Category Two'] # Empty fields are empty strings (since we're loading a CSV file).
    if len(cat2) == 0:
        if len(cat1) == 0:
            d['category'] = None
        else:
            d['category'] = cat1
    elif len(cat1) == 0:
        d['category'] = cat2
    else:
        d['category'] = "{}|{}".format(cat1,cat2)
    return d

def transmit(**kwargs):
    target = kwargs.pop('target') # raise ValueError('Target file must be specified.')
    update_method = kwargs.pop('update_method','upsert')
    if 'schema' not in kwargs:
        raise ValueError('A schema must be given to pipe the data to CKAN.')
    schema = kwargs.pop('schema')
    key_fields = kwargs['key_fields']
    if 'fields_to_publish' not in kwargs:
        raise ValueError('The fields to be published have not been specified.')
    fields_to_publish = kwargs.pop('fields_to_publish')
    server = kwargs.pop('server', 'your-new-favorite-dataset') #'production')
    pipe_name = kwargs.pop('pipe_name', 'generic_pipeline_name')
    clear_first = kwargs.pop('clear_first', False) # If this parameter is true,
    # the datastore will be deleted (leaving the resource intact).

    log = open('uploaded.log', 'w+')


    # There's two versions of kwargs running around now: One for passing to transmit, and one for passing to the pipeline.
    # Be sure to pop all transmit-only arguments off of kwargs to prevent them being passed as pipepline parameters.

    # Code below stolen from prime_ckan/*/open_a_channel() but really from utility_belt/gadgets
    #with open(os.path.dirname(os.path.abspath(__file__))+'/ckan_settings.json') as f: # The path of this file needs to be specified.
    with open(SETTINGS_FILE) as f:
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']

    if 'resource_name' in kwargs:
        resource_specifier = kwargs['resource_name']
    #    original_resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)
    else:
        resource_specifier = kwargs['resource_id']
    #    original_resource_id = kwargs['resource_id']

    #try:
    #    original_url = get_resource_parameter(site,original_resource_id,'url',API_key)
    #except RuntimeError:
    #    original_url = None
    # It's conceivable that original_resource_id may not match resource_id (obtained
    # below), in instances where the resource needs to be created by the pipeline.
        # Does this original_url stuff need to be done here?
            # Let's assume that it doesn't for now.

    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,resource_specifier,package_id,site))
    time.sleep(1.0)

    print("fields_to_publish = {}".format(fields_to_publish))
    a_pipeline = pl.Pipeline(pipe_name,
                              pipe_name,
                              log_status=False,
                              settings_file=SETTINGS_FILE,
                              settings_from_file=True,
                              start_from_chunk=0
                              ) \
        .connect(pl.FileConnector, target, encoding='utf-8') \
        .extract(pl.CSVExtractor, firstline_headers=True) \
        .schema(schema) \
        .load(pl.CKANDatastoreLoader, server,
              fields=fields_to_publish,
              clear_first=clear_first,
              #package_id=package_id,
              #resource_id=resource_id,
              #resource_name=resource_name,
              #key_fields=['dtd','lien_description','tax_year','pin','block_lot','assignee'],
              # A potential problem with making the pin field a key is that one property
              # could have two different PINs (due to the alternate PIN) though I
              # have gone to some lengths to avoid this.
              method=update_method,
              **kwargs).run()

    if 'resource_name' in kwargs:
        resource_id = find_resource_id(site,package_id,kwargs['resource_name'],API_key)
    else:
        resource_id = kwargs['resource_id']

    if a_pipeline.upload_complete:
        print("Piped data to {} on the {} server".format(resource_specifier,server))
        log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),resource_specifier))
        log.close()
        return resource_id
    else:
        print("Something went wrong.")
        return None

def main(**kwargs):

    # Down here in the main function, fetch all three CSV files with requests.
    #   events.csv, safePlaces.csv, services.csv
    # Then process them according to their needs.

    if len(sys.argv) in [0,1,2,3,4]:
        # Interpret command-line arguments as local filenames to use.
        events_shelf, events_headers, events_file_path = parse_file(sys.argv[1],'events') # Where a shelf is a list of dictionaries

    else:
        r = requests.get("http://bigburgh.com/csvdownload/events.csv")
        events_shelf = r.json()

    #"http://bigburgh.com/csvdownload/safePlaces.csv"
    #"http://bigburgh.com/csvdownload/services.csv"

    events = [] 
    # Reformat their field names.
    print("events_shelf[0].keys() = {}".format(events_shelf[0].keys()))
    for e in events_shelf:
        d = {'event_name': e['Event Name'],
            'recurrence': e['recurrence'],
            'program_or_facility': e['program_or_facility'],
            'neighborhood': e['neighborhood'],
            'address': e['address'],
            'organization': e['organization'],
            'recommended_for': e['recommended_for'],
            'requirements': e['requirements'],
            'event_phone': e['Event Phone'],
            'event_narrative': e['Event Narrative'],
            'schedule': e['Schedule'],
            'holiday_exception': e['Holiday Exception']
            }
        d = fuse_cats(e,d)

        events.append(d)
    #events_fields = ['event_name','recurrence','program_or_facility','neighborhood','address','latitude','longitude','organization','category','recommended_for','event_phone','event_narrative','schedule','holiday_exception']
    # Then bring in the schema and ETL framework.
    pprint(events[0])
    schema = EventsSchema
    events_fields = schema().serialize_to_ckan_fields() 
    resource_id = transmit(target = events_file_path, update_method = 'upsert', schema = schema, 
        fields_to_publish = events_fields, key_fields = ['event_name'],
        pipe_name = 'BigBurghEvents', resource_name = 'Events from BigBurgh')
        

if __name__ == '__main__':
    print(len(sys.argv))
    if len(sys.argv) == 2:
        main() # Make this the default.
