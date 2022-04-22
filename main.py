import sys, os, shutil, pprint, csv, zipfile, time, datetime, pip
from keboola import docker
from ftplib import FTP
pip.main(['install', 'parsedatetime'])
import parsedatetime

# Getting configuration for accessing Silverpop
cfg = docker.Config('/data/')
parameters = cfg.get_parameters()
config = {}
configFields = ['bucket', 'table', 'host', 'username', '#password', 'folder', 'entity', 'since', 'until']

for field in configFields:
	config[field] = parameters.get(field)

	if not config[field]:
		raise Exception('Missing mandatory configuration field: '+field)

def processFile(fileName):
	tableDestination = "/data/out/tables/in.c-"+config['bucket']+"."+config['table']

	zip_ref = zipfile.ZipFile("/tmp/"+fileName, 'r')
	zip_ref.extractall('.')
	zip_ref.close()

	fileName = fileName.replace(".zip",".csv")

	writeHeader = True
	skipHeader = False

	if os.path.isfile(tableDestination):
		writeHeader = False
		skipHeader = True

	with open(fileName, 'r', encoding='utf-8') as fin, open(tableDestination, 'a', encoding='utf-8') as fout:
		for line in fin:
			if writeHeader == True:
				header = line.replace(" ", "_")
				header = header.replace("(", "")
				header = header.replace(")", "")
				header = header.replace(":", "")
				header = header.replace("-", "")
				fout.write(header)
				writeHeader = False

			elif skipHeader == True:
				skipHeader = False

			else:
				fout.write(line)

def isIn(fileName):
	dateIsIn = False
	entityIsIn = False

	for date in dates:
		if fileName.find(date) != -1:
			dateIsIn = True

	if fileName.find(config['entity']) != -1:
		entityIsIn = True

	if dateIsIn == True and entityIsIn == True:
		return True

	return False

def downloadFiles():
	ftp = FTP(config['host'])
	ftp.login(user=config['username'], passwd = config['#password'])

	ftp.cwd(config['folder'])

	files = ftp.nlst()

	for f in files:
		if (f[-4:]) == '.zip' and isIn(f):
			print("Loading: "+f)
			localfile = open('/tmp/'+f, 'wb')
			ftp.retrbinary('RETR ' + f, localfile.write, 1024)
			localfile.close()
			processFile(f)

	ftp.quit()

cal = parsedatetime.Calendar()
since, _ = cal.parseDT(datetimeString=str(config['since']))
until, _ = cal.parseDT(datetimeString=str(config['until']))
dates = []
now = since
while now <= until:
	dates.append(now.strftime("%Y%m%d"))
	now += datetime.timedelta(hours=1)

downloadFiles()
