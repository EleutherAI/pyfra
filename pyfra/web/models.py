from .app import db
from .app import login

from werkzeug.security import generate_password_hash, check_password_hash

from flask_login import UserMixin

from time import time
import jwt
from .app import app

class User(db.Model, UserMixin):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)
    email = db.Column(db.String(120), index=True)
    password_hash = db.Column(db.String(128))
    roles = db.Column(db.String(512))

    def __repr__(self):
        return '<User> Email: {} | Name: {}'.format(self.email, self.name)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
 
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

    def get_reset_password_token(self, expires_in=600):
        return jwt.encode(
            {'reset_password': self.id, 'exp': time() + expires_in},
            app.config['SECRET_KEY'], algorithm='HS256') #.decode('utf-8')

    @staticmethod
    def verify_reset_password_token(token):
        try:
            id = jwt.decode(token, app.config['SECRET_KEY'],
                            algorithms=['HS256'])['reset_password']
        except:
            return
        return User.query.get(id)
    
    def get_roles(self):
        return list(set(["everyone"] + self.roles.lower().split(',')))

@login.user_loader
def load_user(id):
    return User.query.get(int(id))

