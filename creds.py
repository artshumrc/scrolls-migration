def creds():
	import json

	creds_file = open(".creds.json")
	creds = json.load(creds_file)

	return creds
