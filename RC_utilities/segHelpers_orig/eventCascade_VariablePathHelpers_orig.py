# eventCascade_VariablePathHelpers.py
import os

def eventParserFolderCreatePart1(dataDir, segmentType):
	logging_dir = os.path.join(dataDir, segmentType, "PreprocLogging")
	os.makedirs(logging_dir, exist_ok=True)
	output_dataDir = os.path.join(dataDir, segmentType, 'Events')
	os.makedirs(output_dataDir, exist_ok=True)
	flat_outputEvents_csv = os.path.join(dataDir, segmentType, 'Events_Flat_csv')
	flat_outputEvents_json = os.path.join(dataDir, segmentType, 'Events_Flat_json')
	os.makedirs(flat_outputEvents_csv, exist_ok=True)
	os.makedirs(flat_outputEvents_json, exist_ok=True)
	flat_outputMetaData = os.path.join(dataDir, segmentType, 'MetaData_Flat')
	os.makedirs(flat_outputMetaData, exist_ok=True)

	return {
		"logging_dir": logging_dir,
		"output_dataDir": output_dataDir,
		"flat_outputEvents_csv": flat_outputEvents_csv,
		"flat_outputEvents_json": flat_outputEvents_json,
		"flat_outputMetaData": flat_outputMetaData,
		}



def nestedEventParserDirs_v1(dataDir, output_dir, out_source_file, segmentType):
    events_csv_path = os.path.join(output_dir, f"{out_source_file}_events.csv")
    events_json_path = os.path.join(output_dir, f"{out_source_file}_events.json")

    flat_outputEvents_csv = os.path.join(dataDir, segmentType, 'EventsFlat_csv')
    flat_outputEvents_json = os.path.join(dataDir, segmentType, 'EventsFlat_json')

    eventsFlat_csv_path = os.path.join(flat_outputEvents_csv, f"{out_source_file}_events.csv")
    eventsFlat_json_path = os.path.join(flat_outputEvents_json, f"{out_source_file}_events.json")

    if segmentType == 'full':
    	metaData_json_path = os.path.join(output_dir, f"{out_source_file}_meta.json")
    	flat_outputMetaData = os.path.join(dataDir, 'MetaData_Flat')
    	metaDataFlat_json_path = os.path.join(flat_outputMetaData, f"{out_source_file}_meta.json")
    else: 
    	metaData_json_path = None
    	flat_outputMetaData = None
    	metaDataFlat_json_path = None

    return {
	    "events_csv_path": events_csv_path, 
	    "events_json_path": events_json_path, 
	    "eventsFlat_csv_path": eventsFlat_csv_path,
	    "eventsFlat_json_path": eventsFlat_json_path, 
	    "metaData_json_path": metaData_json_path, 
	    "flat_outputMetaData": flat_outputMetaData,
	    "metaDataFlat_json_path": metaDataFlat_json_path
    }

def nestedEventParserDirs(dataDir, output_dir, out_source_file, segmentType):
    events_csv_path = os.path.join(output_dir, f"{out_source_file}_events.csv")
    events_json_path = os.path.join(output_dir, f"{out_source_file}_events.json")

    flat_outputEvents_csv = os.path.join(dataDir, segmentType, 'EventsFlat_csv')
    flat_outputEvents_json = os.path.join(dataDir, segmentType, 'EventsFlat_json')

    eventsFlat_csv_path = os.path.join(flat_outputEvents_csv, f"{out_source_file}_events.csv")
    eventsFlat_json_path = os.path.join(flat_outputEvents_json, f"{out_source_file}_events.json")

    metaData_json_path = os.path.join(output_dir, f"{out_source_file}_meta.json")
    flat_outputMetaData = os.path.join(dataDir, 'MetaData_Flat')
    metaDataFlat_json_path = os.path.join(flat_outputMetaData, f"{out_source_file}_meta.json")

    return {
	    "events_csv_path": events_csv_path, 
	    "events_json_path": events_json_path, 
	    "eventsFlat_csv_path": eventsFlat_csv_path,
	    "eventsFlat_json_path": eventsFlat_json_path, 
	    "metaData_json_path": metaData_json_path, 
	    "flat_outputMetaData": flat_outputMetaData,
	    "metaDataFlat_json_path": metaDataFlat_json_path
    }