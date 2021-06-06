from flask import Flask, request, render_template, render_template_string, json, jsonify, Response, flash, redirect, url_for, send_file

from werkzeug.urls import url_parse

from flask_login import current_user, login_user, login_required, logout_user

from .forms import LoginForm
from .forms import ResetPasswordRequestForm
from .forms import ResetPasswordForm

from .emailer import send_password_reset_email

from .app import app
from .app import db
from .app import admin

from .models import User

from flask_admin.contrib.sqla import ModelView

import collections
import pathlib

class PageRegistry:
    def __init__(self):
        self.registry = collections.defaultdict(list)
        self.i = 0

    def add_page(self, name, pretty_name, allowed_roles):
        for role in allowed_roles:
            self.registry[role].append((self.i, name, pretty_name))
        self.i += 1
    
    def get_pages(self, roles):
        ret = []
        for role in roles:
            for page in self.registry[role]:
                if page not in ret: ret.append(page)
        
        return [(n,p) for _,n,p in sorted(ret)]

registry = PageRegistry()


def add_user(name, email, password, roles=[]):
    newUser = User(email=email, name=name, roles=roles)

    newUser.set_password(password)

    db.session.add(newUser)        
    db.session.commit()


# Index and 404
# ===================================================================
@app.route('/')
@login_required
def index():
    roles = current_user.get_roles()
    pages = registry.get_pages(roles)
    return render_template('index.html', pages=pages)

@app.errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404

# WTForms stuff
# ===================================================================
template_path = pathlib.Path(__file__).parent.absolute() / 'templates'

registry.add_page("admin/user", "Admin Dashboard", ["admin"])
registry.add_page("change_password", "Change Password", ["everyone"])

def register_page(name, pretty_name, form_class, callback, allowed_roles, redirect_index, has_form):
    if pretty_name is None: pretty_name = name

    registry.add_page(name, pretty_name, allowed_roles)

    @login_required
    def _fn():

        is_authorized = any([
            role in current_user.get_roles()
            for role in allowed_roles
        ])

        if not is_authorized:
            flash("You are not authorized to view this page.")
            return redirect(url_for('index'))

        if has_form:
            form = form_class()()

            html = ""
            if form.validate_on_submit():
                data = {
                    field.name: field.data
                    for field in form
                    if field.name not in ["submit", "csrf_token"]
                }

                ret = callback(data)
                html = ret if ret is not None else ""
                
                if redirect_index:
                    flash("Form successfully submitted")
                    return redirect(url_for('index'))
        else:
            html = callback({})
            form = None

        return render_template_string(open(template_path / 'form_template.html').read(), body=html, form=form, title=pretty_name, has_form=has_form)
    
    app.add_url_rule(f"/{name}", name, _fn, methods=['GET', 'POST'])


# Authentication Stuff
# ===================================================================
@app.route('/change_password/')
@login_required
def change_password():
    token = current_user.get_reset_password_token().decode()
    return redirect(f'/reset_password/{token}')

@app.route('/reset_password/<token>', methods=['GET', 'POST'])
def reset_password(token):
    user = User.verify_reset_password_token(token)
    if not user:
        return redirect(url_for('index'))
    form = ResetPasswordForm()
    if form.validate_on_submit():
        user.set_password(form.password.data)
        db.session.commit()
        flash('Your password has been reset.')
        return redirect(url_for('login'))
    return render_template('reset_password.html', form=form)

@app.route('/forgot-password', methods=['GET', 'POST'])
def resetPassword():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    form = ResetPasswordRequestForm()
    if form.validate_on_submit():
        user = User.query.filter_by(email=form.email.data).first()
        if user:
            token = user.get_reset_password_token()
            send_password_reset_email(app.config, user.email, user.name, token)
        flash('Check your email for the instructions to reset your password')
        return redirect(url_for('login'))
    return render_template('forgot_password.html',
                           title='Reset Password', form=form)

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))

    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(name=form.name.data).first()
        if user is None or not user.check_password(form.password.data):
            flash('Invalid username or password')
            return redirect(url_for('login'))
        login_user(user, remember=form.remember_me.data)
        next_page = request.args.get('next')
        if not next_page or url_parse(next_page).netloc != '':
            next_page = url_for('index')
        return redirect(next_page)

    return render_template('login.html', title='Sign In', form=form)

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login')) 


# Admin Section
# ===================================================================
class UserAdminView(ModelView):
    create_modal = True
    edit_modal = True
    can_export = True

    form_columns = ('name', 'email', "roles")
    column_exclude_list = ["password_hash"]
    column_searchable_list = ['name', 'email', "roles"]
    column_filters = ['name', 'email', "roles"]

    def is_accessible(self):
        return "admin" in current_user.get_roles()

    def inaccessible_callback(self, name, **kwargs):
        flash("You need to be an admin to view this page.")
        return redirect(url_for('index'))

admin.add_view(UserAdminView(User, db.session))
