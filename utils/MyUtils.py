import configparser


def myConfigParser():
    config = configparser.ConfigParser()
    config.read(r'configs/config.ini')
    return config

schema = "id String,name string, address String, birth_date Date, email String, first_visit String, gender String, latitude String, longitude String,phone_number String, ssn String, state String, zip String"