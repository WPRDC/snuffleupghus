import sys, csv, requests
from marshmallow import fields, pre_load, post_load

from pprint import pprint

sys.path.insert(0, '/Users/drw/WPRDC/etl-dev/wprdc-etl') # A path that we need to import code from
import pipeline as pl

def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)


def parse_file(filepath):
    # Use local file
    f = open(filepath,'r')
    f.readline() #f.next() # Skip the first line, since it gives the delimiter.
    reader = csv.DictReader(f, delimiter='|', quotechar='"') 
    list_of_dicts = list(reader) 
    f.close()
    f = open(filepath,'r')
    _ = f.readline()
    headers = f.readline().split('|')
    #write_to_csv('output.csv',list_of_dicts,headers)
    return list_of_dicts, headers

def get_lat_and_lon(e,d):
    # Split geocoordinates field ("Program Lat and Long") into new latitude and longitude fields
    # 
    latpart, lonpart = e['Program Lat and Long'].split(',')
    _, latitude = latpart.split(': ')
    _, longitude = lonpart.split(': ')
    d['longitude'] = longitude
    d['latitude'] = latitude
    return d

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

def main(**kwargs):
    if 'events_fields' in kwargs:
        events_fields = kwargs['events_fields']
    else:
        events_fields = None


    # Down here in the main function, fetch all three CSV files with requests.
    #   events.csv, safePlaces.csv, services.csv
    # Then process them according to their needs.

    if len(sys.argv) in [0,1,2,3,4]:
        # Interpret command-line arguments as local filenames to use.
        events_shelf, events_headers = parse_file(sys.argv[1]) # Where a shelf is a list of dictionaries

    else:
        r = requests.get("http://bigburgh.com/csvdownload/events.csv")
        events_shelf = r.json()

    #"http://bigburgh.com/csvdownload/safePlaces.csv"
    #"http://bigburgh.com/csvdownload/services.csv"

    events = [] 
    # Reformat their field names.
    for e in events_shelf:
        d = {'event_name': e['Event Name'],
            'recurrence': e["Recurring, One-Time or One-on-One?"],
            'program_or_facility': e['Program (Facility) Name'],
            'neighborhood': e['Program Neighborhood'],
            'address': e['Program Address'],
            'organization': e['Organization Name'],
            'recommended_for': e['(Event) Recommended For :'],
            'event_phone': e['Event Phone'],
            'event_narrative': e['Event Narrative'],
            'schedule': e['Schedule'],
            'holiday_exception': e['Holiday Exception']
            }
        d = get_lat_and_lon(e,d)
        d = fuse_cats(e,d)

        events.append(d)
    #events_fields = ['event_name','recurrence','program_or_facility','neighborhood','address','latitude','longitude','organization','category','recommended_for','event_phone','event_narrative','schedule','holiday_exception']
    # Then bring in the schema and ETL framework.
    schema = EventsSchema
    events_fields = schema().serialize_to_ckan_fields() 



class EventsSchema(pl.BaseSchema): 
    event_name = fields.String(allow_none=False)
    recurrence = fields.String()
    program_or_facility = fields.String()
    neighborhood = fields.String()
    address = fields.String()
    latitude = fields.Float()
    longitude = fields.Float()
    organization = fields.String()
    category = fields.String()
    recommended_for = fields.String()
    event_phone = fields.String()
    event_narrative = fields.String()
    schedule = fields.String()
    holiday_exception = fields.String()
    # Never let any of the key fields have None values. It's just asking for
    # multiplicity problems on upsert.

    # [Note that since this script is taking data from CSV files, there should be no
    # columns with None values. It should all be instances like [value], [value],, [value],...
    # where the missing value starts as as a zero-length string, which this script
    # is then responsible for converting into something more appropriate.
    class Meta:
        ordered = True

schema = EventsSchema
fields0 = schema().serialize_to_ckan_fields()
fields_to_publish = fields0
print("fields_to_publish = {}".format(fields_to_publish))

if __name__ == '__main__':
    print(len(sys.argv))
    if len(sys.argv) == 2:
        main(events_fields=fields0) # Make this the default.
