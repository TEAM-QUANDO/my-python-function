import os
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

message = Mail(
    from_email=('amal@gmail.com', 'Amal G Jose'),
    to_emails=[('receiver01@mail.com', 'Receiver 02'), ('receiver02@mail.com', 'Receiver 02')],
    subject='Sample Email',
    html_content='My test email')

sg = SendGridAPIClient('XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX')
response = sg.send(message)
print(response.status_code, response.body, response.headers)