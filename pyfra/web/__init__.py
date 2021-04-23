from .server import *
from functools import wraps, partial
import inspect
import string
import random
import os
from html import escape
try:
    from typing_extensions import Literal
except ModuleNotFoundError:
    from typing import Literal

from flask_migrate import upgrade
from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, IntegerField, SelectField
from wtforms.validators import DataRequired, Email, EqualTo
from ansi2html import Ansi2HTMLConverter


__all__ = [
    'page', 'webserver', 
    'current_user', # so we can know who's making each request inside a @page annotated function
    'User'
]


def dict_replace_if_fn(d):
    return {
        k: v() if callable(v) else v
        for k, v in d.items()
    }


def page(pretty_name=None, display: Literal["raw", "text", "monospace"]="monospace", field_names={}, dropdowns={}, roles=['everyone']):
    def _fn(callback, pretty_name, field_names, roles, display, dropdowns):

        sig = inspect.signature(callback)

        def make_form_class(dropdowns):
            dropdowns = dict_replace_if_fn(dropdowns)
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
                    if name in dropdowns:
                        field = partial(SelectField, choices=dropdowns[name])
                    else:
                        field = StringField

                setattr(CustomForm, name, field(
                    field_names.get(name, name), 
                    validators=[DataRequired()] if is_required else [],
                    default = sig.parameters[name].default if not is_required else None
                    ))

            if len(sig.parameters) > 0:
                CustomForm.submit = SubmitField('Submit')
            return CustomForm

        form = len(sig.parameters) > 0

        def _callback_wrapper(k):
            html = callback(**k)
            if display == "raw":
                pass
            elif display == "text":
                html = escape(html)
            elif display == "monospace":
                converter = Ansi2HTMLConverter()
                html = converter.convert(html, full=False)
                html = f"<span class=\"monospace\">{html}</span>"
                html += converter.produce_headers()
            else:
                raise NotImplementedError
            
            return html

        register_page(callback.__name__, pretty_name, partial(make_form_class, dropdowns), _callback_wrapper, roles, redirect_index=False, has_form=form)
    
    # used @form and not @form()
    if callable(pretty_name):
        return _fn(pretty_name, pretty_name=None, field_names=field_names, roles=roles, display=display, dropdowns=dropdowns)

    return partial(_fn, pretty_name=pretty_name, field_names=field_names, roles=roles, display=display, dropdowns=dropdowns)


def gen_pass(stringLength=16):
    """Generate a random string of letters, digits """
    password_characters = string.ascii_letters + string.digits
    return ''.join(random.choice(password_characters) for i in range(stringLength))   


@page("Add User", roles=["admin"])
def adduser(username: str, email: str="example@example.com", roles: str=""):
    password = gen_pass()

    add_user(username, email, password, roles)

    return f"Added user {username} with randomly generated password {password}."

@page("Reset User Password", roles=["admin"])
def set_password(username: str):
    password = gen_pass()

    user = User.query.filter_by(name=username).first()

    user.set_password(password)
    db.session.commit()

    return f"Updated user {username} with randomly generated password {password}."


def webserver(debug=False):
    with app.app_context():
        migrations_path = os.path.join(os.path.dirname(__file__), "migrations")
        upgrade(migrations_path) # equivalent to running "flask db upgrade" every load

    if User.query.count() == 0:
        # Add temporary admin user
        password = gen_pass()
        add_user("root", "example@example.com", password, "admin")
        print("=================================================")
        print("ADMIN LOGIN CREDENTIALS (WILL ONLY BE SHOWN ONCE)")
        print("If you forget you'll need to manually add an")
        print("admin user to the database.")
        print("Username: root")
        print("Password:", password)
        print("=================================================")
    app.config['TEMPLATES_AUTO_RELOAD'] = True
    app.run(host='0.0.0.0', debug=debug)