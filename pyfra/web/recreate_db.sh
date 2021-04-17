rm -rf migrations
rm app.db
python3 -m flask db init
python3 -m flask db migrate
python3 -m flask db upgrade
python3 load_users.py
