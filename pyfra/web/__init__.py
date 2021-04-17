from server import *
from functools import wraps
import inspect

from flask_wtf import FlaskForm
from wtforms import StringField, BooleanField, SubmitField, IntegerField
from wtforms.validators import DataRequired, Email, EqualTo


__all__ = ['stateless_form', 'run_server']


def stateless_form(pretty_name=None, field_names={}, roles=['everyone']):
    def _fn(callback):
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
            flash("Form successfully submitted")
            return callback(**k)

        register_wtf_form(callback.__name__, pretty_name, CustomForm, _callback_wrapper, roles)
    
    return _fn

@stateless_form("Example Form", {
    'text1': "Text 1"
})
def example_form(text1: str, text2: int=123, check: bool=True):
    print("form submitted", text1, text2, check)

run_server()