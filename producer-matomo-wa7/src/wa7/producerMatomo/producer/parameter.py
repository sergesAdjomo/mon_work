import wa7.producerMatomo.tools.configuration as cfg 
from wa7.producerMatomo.producer import common

args=cfg.checkParameters()
config=cfg.configInstance()
yesterday=common.yesterdayDatectrlm(args.datectrlm)
today=common.todayDatectrlm(args.datectrlm)

matomo_list_id_site = args.matomo_id_site.strip().split(",")
matomo_api_url = args.matomo_api_url
matomo_api_token = args.matomo_api_token
matomo_sps_user = args.matomo_sps_user
matomo_sps_mdp = args.matomo_sps_mdp


start_date = config.get('DEFAULT','START_DATE')
matomo_filter_limit = int(config.get('DEFAULT','MATOMO_FILTER_LIMIT'))
file_path_last_offset = config.get('DEFAULT','FILE_PATH_LAST_OFFSET')
file_path_filterdate = config.get('DEFAULT','FILE_PATH_FILTERDATE')
nbre_jour = config.get('DEFAULT','NBRE_JOUR')

kafka_server = config.get('KAFKA','KAFKA_SERVERS')
topic = config.get('KAFKA','KAFKA_TOPIC')

sqlite_location = config.get('SQLITE','FILE_LOCATION')

