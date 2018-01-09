import sys, csv



def write_to_csv(filename,list_of_dicts,keys):
    with open(filename, 'wb') as output_file:
        dict_writer = csv.DictWriter(output_file, keys, extrasaction='ignore', lineterminator='\n')
        dict_writer.writeheader()
        dict_writer.writerows(list_of_dicts)

filepath = sys.argv[1]
f = open(filepath,'r')
f.next() # Skip the first line, since it gives the delimiter.
reader = csv.DictReader(f, delimiter='|', quotechar='"') 
list_of_dicts = list(reader) 
f.close()
f = open(filepath,'r')
_ = f.readline()
headers = f.readline().split('|')
write_to_csv('output.csv',list_of_dicts,headers)

# Split geocoordinates field into new latitude and longitude fields



# Down here in the main function, fetch all three CSV files with requests.

# Then process them according to their needs.

# Reformat their field names.

#for events.csv
#Event Name|Recurring, One-Time or One-on-One?|Program (Facility) Name|Program Neighborhood|Program Address|Program Lat and Long|Organization Name|Category One|Category Two|(Event) Recommended For :|(Event) Requirements|Event Phone|Event Narrative|Schedule|Holiday Exception ==> 
#event_name, recurrence, program_or_facility, neighborhood, address, latitude, longitude, organization, category1
