import json


jsonfile= "/Users/zach/Documents/UCLA_classes/research/Polling/results/sample_civic_responses.json"


def pretty_print(path_to_file,write=False):
	with open(path_to_file, "r") as content:
		string = content.read()
	string = string.replace("'",'"')
	string = string.replace("True",'true')
	string = string.replace("False",'false')
	geojson = json.loads(string)
	if write:
		newfilename = path_to_file.rsplit(".",1)[0] + "_pretty_printed.json"
		with open(newfilename,"w") as f:
			f.write(json.dumps(geojson,indent=5))
	return geojson
if __name__=="__main__":
	props = pretty_print(jsonfile,write=True)
