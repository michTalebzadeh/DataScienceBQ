import yaml
with open("../conf/config.yml", 'r') as file:
  config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  #print(config.keys())
  #print(config['hiveVariables'].items())

"""
#print(data_map)
  data_map = yaml.safe_load(file)
font = data_map["plot_fonts"]["font"]
font_small = data_map["plot_fonts"]["font_small"]
#Hive
DSDB = data_map["hiveVariables"]["DSDB"]
regionname = data_map["hiveVariables"]["regionname"]
Boston_csvlocation = data_map["hiveVariables"]["Boston_csvlocation"]
London_csvLocation = data_map["hiveVariables"]["London_csvLocation"]
settings = data_map["hiveVariables"]["settings"]
print(settings)
#GCP
projectId = data_map["GCPVariables"]["projectId"]
datasetLocation = data_map["GCPVariables"]["datasetLocation"]
bucketname = data_map["GCPVariables"]["bucketname"]
sourceDataset = data_map["GCPVariables"]["sourceDataset"]
sourceTable = data_map["GCPVariables"]["sourceTable"]
inputTable = data_map["GCPVariables"]["inputTable"]
sourceDataset = data_map["GCPVariables"]["sourceDataset"]
targetDataset = data_map["GCPVariables"]["targetDataset"]
targetTable = data_map["GCPVariables"]["targetTable"]
outputTable = data_map["GCPVariables"]["outputTable"]
fullyQualifiedoutputTableId = data_map["GCPVariables"]["fullyQualifiedoutputTableId"]
tmp_bucket = data_map["GCPVariables"]["tmp_bucket"]
jsonKeyFile = data_map["GCPVariables"]["jsonKeyFile"]
DB = data_map["GCPVariables"]["DB"]
"""






