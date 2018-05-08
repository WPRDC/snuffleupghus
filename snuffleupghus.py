import sys, re, csv, json, time, ckanapi, requests, traceback
from marshmallow import fields, pre_load, post_load

from datetime import datetime
from pprint import pprint

# Note that the wprdc-etl documentation claims that it can handle |-delimited files,
# if the pipeline is properly configured.
sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl

from parameters.local_parameters import SETTINGS_FILE, DATA_PATH
from notify import send_to_slack

# The three base schema could certainly be refactored, but it's not clear
# if it's worth doing.
class EventsSchema(pl.BaseSchema): 
    event_name = fields.String(allow_none=False)
    recurrence = fields.String(allow_none=True)
    program_or_facility = fields.String(allow_none=True)
    program_neighborhood = fields.String(dump_to='neighborhood',allow_none=True)
    program_address = fields.String(dump_to='address',allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization_name = fields.String(dump_to="organization",allow_none=True)
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
        if 'program_lat_and_long' not in data or data['program_lat_and_long'] is None:
            data['latitude'] = None
            data['longitude'] = None
        else:
            latpart, lonpart = data['program_lat_and_long'].split(',')
            _, latitude = latpart.split(': ')
            _, longitude = lonpart.split(': ')
            data['latitude'] = float(latitude)
            data['longitude'] = float(longitude)
        if 'program_lat_and_long' in data:
            del data['program_lat_and_long']

    @pre_load
    def fuse_cats(self,data):
        # Combine Category One and Category Two into a |-delimited category field
        if 'category_one' not in data and 'category_two' not in data:
            return None
        elif 'category_two' not in data:
            return data['category_one']
        elif 'category_one' not in data:
            return data['category_two']

        cat1 = data['category_one']
        cat2 = data['category_two'] # Empty fields are empty strings (since we're loading a CSV file).
        if cat2 is None or len(cat2) == 0:
            if cat1 is None or len(cat1) == 0:
                data['category'] = None
            else:
                data['category'] = cat1
        elif cat1 is None or len(cat1) == 0:
            data['category'] = cat2
        else:
            data['category'] = "{}|{}".format(cat1,cat2)

        del data['category_one']
        del data['category_two']

class EventsArchiveSchema(EventsSchema):
    year_month = fields.String(allow_none=False)

    @pre_load
    def add_year_month(self, data):
        data['year_month'] = datetime.strftime(datetime.now(),"%Y%m")

class SafePlacesSchema(pl.BaseSchema): 
    safe_place_name = fields.String(allow_none=False)
    program_or_facility = fields.String(allow_none=True)
    program_neighborhood = fields.String(dump_to='neighborhood',allow_none=True)
    program_address = fields.String(dump_to='address',allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization_name = fields.String(dump_to="organization",allow_none=True)
    recommended_for = fields.String(allow_none=True)
    requirements = fields.String(allow_none=True)
    safe_place_phone = fields.String(dump_to="phone",allow_none=True)
    safe_place_narrative = fields.String(dump_to="narrative",allow_none=True)
    schedule = fields.String(allow_none=True)
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
        if 'program_lat_and_long' not in data or data['program_lat_and_long'] is None:
            data['latitude'] = None
            data['longitude'] = None
        else:
            latpart, lonpart = data['program_lat_and_long'].split(',')
            _, latitude = latpart.split(': ')
            _, longitude = lonpart.split(': ')
            data['latitude'] = float(latitude)
            data['longitude'] = float(longitude)
        if 'program_lat_and_long' in data:
            del data['program_lat_and_long']

class SafePlacesArchiveSchema(SafePlacesSchema):
    year_month = fields.String(allow_none=False)

    @pre_load
    def add_year_month(self, data):
        data['year_month'] = datetime.strftime(datetime.now(),"%Y%m")

class ServicesSchema(pl.BaseSchema): 
    service_name = fields.String(allow_none=False)
    program_or_facility = fields.String(allow_none=True)
    program_neighborhood = fields.String(dump_to='neighborhood',allow_none=True)
    program_address = fields.String(dump_to='address',allow_none=True)
    latitude = fields.Float(allow_none=True)
    longitude = fields.Float(allow_none=True)
    organization_name = fields.String(dump_to="organization",allow_none=True)
    category = fields.String(allow_none=True)
    recommended_for = fields.String(allow_none=True)
    requirements = fields.String(allow_none=True)
    service_phone = fields.String(dump_to="phone",allow_none=True)
    service_narrative = fields.String(dump_to="narrative",allow_none=True)
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
        if 'program_lat_and_long' not in data or data['program_lat_and_long'] is None:
            data['latitude'] = None
            data['longitude'] = None
        else:
            try:
                latpart, lonpart = data['program_lat_and_long'].split(',')
            except:
                print(data['program_lat_and_long'])
                latpart, lonpart = data['program_lat_and_long'].split(',,') # Klugy workaround to deal with
                # one case of latitude and longitude being separated by two commas.
            _, latitude = latpart.split(': ')
            _, longitude = lonpart.split(': ')
            data['latitude'] = float(latitude)
            data['longitude'] = float(longitude)
        if 'program_lat_and_long' in data:
            del data['program_lat_and_long']

    @pre_load
    def fuse_cats(self,data):
        # Combine Category One and Category Two into a |-delimited category field
        if 'category_one' not in data and 'category_two' not in data:
            return None
        elif 'category_two' not in data:
            return data['category_one']
        elif 'category_one' not in data:
            return data['category_two']

        cat1 = data['category_one']
        cat2 = data['category_two'] # Empty fields are empty strings (since we're loading a CSV file).
        if cat2 is None or len(cat2) == 0:
            if cat1 is None or len(cat1) == 0:
                data['category'] = None
            else:
                data['category'] = cat1
        elif cat1 is None or len(cat1) == 0:
            data['category'] = cat2
        else:
            data['category'] = "{}|{}".format(cat1,cat2)

        del data['category_one']
        del data['category_two']

class ServicesArchiveSchema(ServicesSchema):
    year_month = fields.String(allow_none=False)

    @pre_load
    def add_year_month(self, data):
        data['year_month'] = datetime.strftime(datetime.now(),"%Y%m")

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

def open_a_channel(settings_file_path,server):
    # Get parameters to communicate with a CKAN instance
    # from the specified JSON file.
    with open(settings_file_path) as f:
        settings = json.load(f)
    site = settings['loader'][server]['ckan_root_url']
    package_id = settings['loader'][server]['package_id']
    API_key = settings['loader'][server]['ckan_api_key']

    return site, API_key, package_id

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
    #
    return desired_string
    
def find_resource_id(site,package_id,resource_name,API_key=None):
    # Get the resource ID given the package ID and resource name.
    resources = get_package_parameter(site,package_id,'resources',API_key)
    for r in resources:
        if r['name'] == resource_name:
            return r['id']
    return None

def query_resource(site,query,API_key=None):
    # Use the datastore_search_sql API endpoint to query a CKAN resource.

    # Note that this doesn't work for private datasets. 
    # The relevant CKAN GitHub issue has been closed.
    # https://github.com/ckan/ckan/issues/1954
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)

    response = ckan.action.datastore_search_sql(sql=query)
    # A typical response is a dictionary like this
    #{u'fields': [{u'id': u'_id', u'type': u'int4'},
    #             {u'id': u'_full_text', u'type': u'tsvector'},
    #             {u'id': u'pin', u'type': u'text'},
    #             {u'id': u'number', u'type': u'int4'},
    #             {u'id': u'total_amount', u'type': u'float8'}],
    # u'records': [{u'_full_text': u"'0001b00010000000':1 '11':2 '13585.47':3",
    #               u'_id': 1,
    #               u'number': 11,
    #               u'pin': u'0001B00010000000',
    #               u'total_amount': 13585.47},
    #              {u'_full_text': u"'0001c00058000000':3 '2':2 '7827.64':1",
    #               u'_id': 2,
    #               u'number': 2,
    #               u'pin': u'0001C00058000000',
    #               u'total_amount': 7827.64},
    #              {u'_full_text': u"'0001c01661006700':3 '1':1 '3233.59':2",
    #               u'_id': 3,
    #               u'number': 1,
    #               u'pin': u'0001C01661006700',
    #               u'total_amount': 3233.59}]
    # u'sql': u'SELECT * FROM "d1e80180-5b2e-4dab-8ec3-be621628649e" LIMIT 3'}
    data = response['records']
    return data

def query_private_resource(site,resource_id,filters,API_key=None,limit=99999999999):
    # Private resources can be queried using the datastore_search API 
    # endpoint, which supports filters.
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    response = ckan.action.datastore_search(id=resource_id,filters=filters,limit=limit)
    data = response['records']
    return data

def query_any_resource(site,query,resource_id,filters,API_key=None,limit=99999999999):
    ckan = ckanapi.RemoteCKAN(site, apikey=API_key)
    # From resource ID determine package ID.
    package_id = ckan.action.resource_show(id=resource_id)['package_id']
    # From package ID determine if the package is private.
    private = ckan.action.package_show(id=package_id)['private']
    if private:
        #print("As of February 2018, CKAN still doesn't allow you to run a datastore_search_sql query on a private dataset. Sorry. See this GitHub issue if you want to know a little more: https://github.com/ckan/ckan/issues/1954")
        #raise ValueError("CKAN can't query private resources (like {}) yet.".format(resource_id))
        return query_private_resource(site,resource_id,filters,API_key,limit)
    else:
        return query_resource(site,query,API_key)

def parse_file(filepath,basename):
    replacement_headers = {"Recurring, One-Time or One-on-One?": "recurrence",
                            "Program (Facility) Name": "program_or_facility",
                            "(Event) Recommended For :": "recommended_for",
                            "(Event) Requirements": "requirements",
                            "(Safe Place) Recommended For :": "recommended_for",
                            "(Safe Place) Requirements": "requirements",
                            "(Service) Recommended For :": "recommended_for",
                            "(Service) Requirements": "requirements"
                            }

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
    dpath = '/'.join(filepath.split("/")[:-1]) + '/'
    if dpath == '/':
        dpath = ''
    outputfilepath = "{}tmp/{}.csv".format(dpath,basename)
    # [ ] If the temp directory doesn't exist, create it. 
    write_to_csv(outputfilepath,list_of_dicts,new_headers)
    return list_of_dicts, headers, outputfilepath

def transmit(**kwargs):
    target = kwargs.pop('target') # raise ValueError('Target file must be specified.')
    update_method = kwargs.pop('update_method','insert')
    if 'schema' not in kwargs:
        raise ValueError('A schema must be given to pipe the data to CKAN.')
    schema = kwargs.pop('schema')
    key_fields = kwargs['key_fields']
    if 'fields_to_publish' not in kwargs:
        raise ValueError('The fields to be published have not been specified.')
    fields_to_publish = kwargs.pop('fields_to_publish')
    server = kwargs.pop('server', 'secret-cool-data') #'production')
    pipe_name = kwargs.pop('pipe_name', 'generic_pipeline_name')
    clear_first = kwargs.pop('clear_first', False) # If this parameter is true,
    # the datastore will be deleted (leaving the resource intact).

    log = open('uploaded.log', 'w+')


    # There's two versions of kwargs running around now: One for passing to transmit, and one for passing to the pipeline.
    # Be sure to pop all transmit-only arguments off of kwargs to prevent them being passed as pipepline parameters.

    site, API_key, package_id = open_a_channel(SETTINGS_FILE,server)

    if 'resource_name' in kwargs:
        resource_specifier = kwargs['resource_name']
   
    else:
        resource_specifier = kwargs['resource_id']
    
    print("Preparing to pipe data from {} to resource {} package ID {} on {}".format(target,resource_specifier,package_id,site))

    a_pipeline = pl.Pipeline(pipe_name,
                              pipe_name,
                              log_status=False,
                              settings_file=SETTINGS_FILE,
                              settings_from_file=True
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

    print("Piped data to {} on the {} server".format(resource_specifier,server))
    log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),resource_specifier))
    log.close()
    return resource_id
    #if a_pipeline.upload_complete: # This doesn't work on earlier versions of wprdc-etl
    #    print("Piped data to {} on the {} server".format(resource_specifier,server))
    #    log.write("Finished {}ing {}\n".format(re.sub('e$','',update_method),resource_specifier))
    #    log.close()
    #    return resource_id
    #else:
    #    print("Something went wrong.")
    #    return None

def clear_and_upload(kwparams):
    resource_id = transmit(**kwparams) # This is a hack to get around the ETL framework's limitations. 1) Update (or create) the resource. 
    time.sleep(0.5)
    kwparams['clear_first']=True                  # Then...
    resource_id = transmit(**kwparams) # Clear the datastore and upload the data again.
    print("(Yes, this data is being deliberately piped to the CKAN resource twice. It has something to do with using the clear_first parameter to clear the datastore, which can only be done if the datastore has already been created, since the ETL framework is flawed.)")
    return resource_id

def get_nth_file_and_insert(fetch_files,n,table,key_fields,resource_designation,server,add_to_archive=False):
    # Fetch all three CSV files with requests.
    #   events.csv, safePlaces.csv, services.csv
    # Then process them according to their needs.
    resource_name = "Current List of {}".format(resource_designation)
    archive_resource_name = "{} Archive (Cumulative)".format(resource_designation)

    if not fetch_files and len(sys.argv) > n+2:
        # Interpret command-line arguments (after fetch_files and server) as local filenames to use.
        print("Obtaining {} from local files.".format(table))
        events_shelf, events_headers, events_file_path = parse_file(sys.argv[n+2],table) # Where a shelf is a list of dictionaries
    elif fetch_files:
        print("Getting {} from bigburgh.com".format(table))
        r = requests.get("http://bigburgh.com/csvdownload/{}.csv".format(table))
        dpath = '/'.join(DATA_PATH.split("/")[:-1]) + '/'
        if dpath == '/':
            dpath = ''
        basename = "pipeorama"
        pipe_delimited_file_path = "{}tmp/{}-with-pipes.csv".format(dpath,table) # [ ] Eventually delete these files.
        with open(pipe_delimited_file_path,'wb') as f:
            f.write(r.content)
        events_shelf, events_headers, events_file_path = parse_file(pipe_delimited_file_path,table) # Where a shelf is a list of dictionaries

    #print("events_file_path = {}".format(events_file_path)) This is in tmp/tmp/
    else: # fetch_files is false but the nth file location was not given
        print("The location of file {} was not specified.".format(n))
        return

    schema = schema_dict[table]
    events_fields = schema().serialize_to_ckan_fields()
    kwparams = dict(target = events_file_path, update_method = 'insert', schema = schema, 
        fields_to_publish = events_fields, key_fields = key_fields,
        pipe_name = 'BigBurghPipe{}'.format(n), resource_name = resource_name,
        server = server)

    resource_id = clear_and_upload(kwparams)

    if add_to_archive:
        site, API_key, package_id = open_a_channel(SETTINGS_FILE,server)
       
        # Add to the aggregate archive of all months.
        # Check if there's already enough records in the resource (archive_resource_name)
        archive_resource_id = find_resource_id(site,package_id,archive_resource_name,API_key)
        current_year_month = datetime.strftime(datetime.now(),"%Y%m")
        if archive_resource_id is not None:
            query = "SELECT * FROM \"{}\" WHERE year_month = \'{}\' LIMIT 999999".format(archive_resource_id,current_year_month)
            loaded_data = query_any_resource(site, query, archive_resource_id, {'year_month': current_year_month}, API_key)
            # Eventually the API key won't be needed here, once the dataset is public.
            number_of_records = len(loaded_data)
            # [ ] Check whether any of the loaded_data collides with the new data. (It probably does.)

            if number_of_records >= len(events_shelf): # Assume that the data for the month is sufficiently complete:
                archive = False
            elif number_of_records == 0: # Definitely insert the new data into the archive.
                archive = True
                archive_update_method = 'insert'
            else:
                archive = True
                #archive_update_method = 'insert'
                # On second thought, let's always insert these records until we find an issue with this.
                archive_update_method = 'insert'
                # But in these instances, let's send a message that this should be investigated:
                msg = "number_of_records = {}, while len(events_shelf) = {}. Inserting new records into {}, but it would be a good idea to check manually for conflicts and see if this is (in general) a good solution.".format(number_of_records, len(events_shelf), archive_resource_name)
                send_to_slack(msg,username='snuffleupghus',channel='@david',icon=':snuffleupagus:')
        else:
            archive = True
            archive_update_method = 'insert'


        archive_schema = schema_dict[table+'_archive']
        events_fields = archive_schema().serialize_to_ckan_fields()
        events_fields = [events_fields[-1]] + events_fields[:-1]
        if archive:
            # Add year_month field to data through the schema.
            resource_id = transmit(target = events_file_path, update_method = archive_update_method, 
                schema = archive_schema, fields_to_publish = events_fields, key_fields = key_fields,
                pipe_name = 'BigBurghArchivePipe{}'.format(n), 
                resource_name = archive_resource_name, server = server)
        
        ##############################################################################################
        # Also, and in any event, reate a new archive for just that month (if it doesn't already exist).
        # Add year_month field to data through the schema.
        now = datetime.now()
        year = now.year
        month = datetime.strftime(now,"%m")
        month_archive_resource_name = "{}-{} {} Archive".format(year,month,resource_designation)

        kwparams = dict(target = events_file_path, update_method = 'insert', schema = archive_schema, 
            fields_to_publish = events_fields, key_fields = [],
            pipe_name = 'BigBurghArchivePipe{}'.format(n), 
            resource_name = month_archive_resource_name, server = server)
            # Note that inserting records where key_fields = [] allows all records
            # to be added, whereas if you set the key field to be the name of the
            # event, a duplicate (or near duplicate) causes an error when insertions
            # are attempted.
        resource_id = clear_and_upload(kwparams)
        print("============================================================")

schema_dict = {'events': EventsSchema,
                'events_archive': EventsArchiveSchema,
                'safePlaces': SafePlacesSchema,
                'safePlaces_archive': SafePlacesArchiveSchema,
                'services': ServicesSchema,
                'services_archive': ServicesArchiveSchema
                }

def main(**kwargs):
    server = kwargs.pop('server', 'secret-cool-data') #'production')
    try:
        fetch_files = kwargs.get('fetch_files',False)
        # No primary key is used since we can't guarantee that there won't be collisions.
        get_nth_file_and_insert(fetch_files,1,'events',key_fields = [], resource_designation = "Events", server = server, add_to_archive=True)
        get_nth_file_and_insert(fetch_files,2,'safePlaces',key_fields = [], resource_designation = "Safe Places", server = server, add_to_archive = True)
        get_nth_file_and_insert(fetch_files,3,'services',key_fields = [], resource_designation = "Services", server = server, add_to_archive=True)
        # The main scheme for ensuring that no duplicate rows are archived is running the 
        # script once per month, but it's necessary to make sure that it actually runs successfully!
        
        # Well, we got to the end of this block, so it seems like the script executed successfully.
        # Let's add a metadata tag to the package that indicates the last time the ETL script 
        # ran successfully:

        # extras metadata dictionary is implemented in CKAN 2.7:
        # extras['etl_last_successfully_run'] = datetime.now().isoformat()
    except:
        e = sys.exc_info()[0]
        print("Error: {} : ".format(e))
        exc_type, exc_value, exc_traceback = sys.exc_info()
        lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
        traceback_msg = ''.join('!! ' + line for line in lines)
        print(traceback_msg)  # Log it or whatever here
        msg = "snuffleupghus.py ran into an error: {}.\nHere's the traceback:\n{}".format(e,traceback_msg)
        mute_alerts = kwargs.get('mute_alerts',False)
        if not mute_alerts:
            send_to_slack(msg,username='snuffleupghus',channel='@david',icon=':snuffleupagus:')

if __name__ == '__main__':
    print(sys.argv)
    if len(sys.argv) in [1,2]:
        print("At a minimum, the fetch_files and server parameters must be specified as the first two command-line parameters.")
    else:
        fetch_files = (sys.argv[1].lower() == 'true')
        mute_alerts = False
        if len(sys.argv) > 3:
            mute_alerts = bool(sys.argv[3])
        main(fetch_files=fetch_files,server=sys.argv[2],mute_alerts=mute_alerts)
