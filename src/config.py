import yaml
with open("../conf/config.yml", 'r') as file:
  config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  #print(config.keys())
  #print(config['hiveVariables'].items())
  hive_url = "jdbc:hive2://" + config['hiveVariables']['hiveHost'] + ':' + config['hiveVariables']['hivePort'] + '/default'
  oracle_url = "jdbc:oracle:thin:@" + config['OracleVariables']['oracleHost'] + ":" + config['OracleVariables']['oraclePort'] + ":" + config['OracleVariables']['oracleDB']







