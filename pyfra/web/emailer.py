from smtplib import SMTP

from email.message import EmailMessage

from flask import render_template

from threading import Thread

def create_email_message(from_address, to_address, subject, body, htmlBody):
    msg = EmailMessage()
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg.set_content(body)
    msg.set_content(htmlBody, subtype='html')
    return msg

def send_email(config, to_address, subject, body, htmlBody):
    mailserver = SMTP(config["SMTP_SERVER"], config["SMTP_PORT"])
    mailserver.ehlo()
    mailserver.starttls()
    mailserver.login(config["SMTP_USERNAME"], config["SMTP_PASSWORD"])

    message = create_email_message(config["SMTP_USERNAME"], to_address, subject, body, htmlBody)
    mailserver.send_message(message)

    mailserver.quit()

def send_password_reset_email(config, targetEmail, targetName, token):
    
    subject = "[Eleuther Flask App] Reset Your Password"
    Thread(target=send_email, 
           args=(config, targetEmail, subject,
                 render_template('email/reset_password.txt',
                                 username=targetName, token=token),
                 render_template('email/reset_password.html',
                                 username=targetName, token=token))).start()