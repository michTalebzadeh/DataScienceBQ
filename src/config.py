import yaml
with open("../conf/config.yml", 'r') as file:
  config: dict = yaml.load(file.read(), Loader=yaml.FullLoader)
  #print(config.keys())
  #print(config['hiveVariables'].items())
  oracle_url = "jdbc:oracle:thin:@" + config['OracleVariables']['oracleHost'] + ":" + config['OracleVariables']['oraclePort'] + ":" + config['OracleVariables']['oracleDB']







