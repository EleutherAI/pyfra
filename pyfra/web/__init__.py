from .server import *
from functools import wraps, partial
import inspect
import string
import random

from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Email, EqualTo


__all__ = ['form', 'run_server']


def form(pretty_name=None, field_names={}, roles=['everyone']):
    def _fn(callback, pretty_name, field_names, roles):

        sig = inspect.signature(callback)

        class CustomForm(FlaskForm):
            pass

        for name in sig.parameters:
            type = sig.parameters[name].annotation
            is_required = (sig.parameters[name].default == inspect._empty)

            if type == int:
                field = IntegerField
            elif type == bool:
                field = BooleanField
            else:
                field = StringField

            setattr(CustomForm, name, field(
                field_names.get(name, name), 
                validators=[DataRequired()] if is_required else [],
                default = sig.parameters[name].default if not is_required else None
                ))

        CustomForm.submit = SubmitField('Submit')

        def _callback_wrapper(k):
            return callback(**k)

        register_wtf_form(callback.__name__, pretty_name, CustomForm, _callback_wrapper, roles)
    
    # used @form and not @form()
    if callable(pretty_name):
        return _fn(pretty_name, pretty_name=None, field_names=field_names, roles=roles)

    return partial(_fn, pretty_name=pretty_name, field_names=field_names, roles=roles)


def gen_pass(stringLength=16):
    """Generate a random string of letters, digits """
    password_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(password_characters) for i in range(stringLength))   


@form("Add User", roles=["admin"])
def adduser(username: str, email: str="example@example.com", roles: str=""):
    password = gen_pass()

    add_user(username, email, password, roles)

    return f"Added user {username} with randomly generated password {password}."


def run_server(debug=False):
    if User.query.count() == 0:
        # Add temporary admin user
        password = gen_pass()
        add_user("root", "example@example.com", password, "admin")
        print("=================================================")
        print("ADMIN LOGIN CREDENTIALS (WILL ONLY BE SHOWN ONCE)")
        print("Username: root")
        print("Password:", password)
        print("=================================================")
    app.run(host='0.0.0.0', debug=debug)