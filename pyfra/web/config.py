import os
basedir = os.getcwd()

class Config(object):

    SQLALCHEMY_DATABASE_URI = f'sqlite:///{basedir}/app.db'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = 'FILL_THIS_IN'
    SMTP_SERVER = "smtp.gmail.com"
    SMTP_PORT = 587
    SMTP_USERNAME = "FILL_THIS_IN"
    SMTP_PASSWORD = "FILL_THIS_IN"