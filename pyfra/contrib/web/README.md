# Flask Based Polling Website Demo

Basic implementation of a polling website with authentication and administration backend.

Currently implements the following:

Webpage Templating and Endpoints (flask)
Forms (flask-wtf)
Admin Section (flask-admin)
Authentication (flask-login, pyjwt)
Backend sqlite database (flask-sqlalchemy, flask-migrate)

## To Do

Fix the admin section blowing up when you aren't logged in.  
Copy over from Votr the polling logic.  
Hide logout button when not logged in

## Installation

Clone, copy the config template and set the config items correctly. Secret key
is used to create jwt. Email config is used for sending password reset emails
to users.

```
git clone https://github.com/EleutherAI/poll_website_demo.git
cp config_template.py config.py
vi config.py
```

Create the database:

```
./recreate_db.sh
```

To run the dev server on your local machines external ip, using port 5000:
```
python server.py
```

## Usage

Visiting root will show you a login screen where you can register, reset password etc.

Admin section can be found in /admin which requires an admin user to be logged in. The default admin credentials can be found in users.csv.

## Design Details

The app is wired together inside app.py, which is imported into server.py.

config.py is imported using app.config.from_object(Config) inside app.py.

ORM model can be found in models.py

flask-migrate uses alembic on the backend, which creates database diffs allowing modification of live databases. We have a simple script to generate the initial database in recreate_db.sh, but to make changes to a live site you will need to learn a bit about flask-migrate. Our script calls load_users.py which populates the users table from the users.csv file if you want some users when the application starts (you will need an admin user if you dont want to have to manually setup the user with sqlite). We login with email (not Name).

Templates are found in the templates directory, with the email subdirectory containing the template used for sending the password reset email.

All web templates inherit from base.html

Stylesheets can be found in static/css. We use a reset sheet and a main.css currently.

static also has a bunch of junk found in the html5 boilerplate in case you want it. You can replace the favicon.ico if wanted.

jquery is included in the template if you want to use it - just write javascript in the templates you use. To serve react I'd probably build a separate react pipeline and just copy the output of build and serve it up using a flask endpoint (unless there's something better out there I haven't used yet).

I pulled this application from something else I wrote and left the print and excel buttons in case we ever want to do exporting stuff.

