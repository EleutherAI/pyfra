import csv

from models import User
from app import db

import random
import string

def randomStringwithDigitsAndSymbols(stringLength=10):
    """Generate a random string of letters, digits and special characters """
    password_characters = string.ascii_letters + string.digits + string.punctuation
    return ''.join(random.choice(password_characters) for i in range(stringLength))   

users = User.query.all()
for u in users:
    db.session.delete(u)

with open('users.csv') as f:
    reader = csv.reader(f)    
    for row in reader:
        newUser = User(email=row[0], name=row[1], roles=row[3])

        plaintextPassword = row[2]
        if plaintextPassword == "":
            plaintextPassword = randomStringwithDigitsAndSymbols()

        newUser.password = plaintextPassword
        print(newUser.email, newUser.name, plaintextPassword, newUser.password_hash)

        db.session.add(newUser)        

db.session.commit()