# Import smtplib for the actual sending function
import smtplib
# Import the email modules we'll need
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# Create message container - the correct MIME type is multipart/alternative.
msg = MIMEMultipart('alternative')


to_list = ['weiqing.yu@groundtruth.com']
sender = 'weiqing.yu@groundtruth.com' 
COMMASPACE = ', '


# Send the message via our own SMTP server, but don't include the
# envelope header.

def _send_msg(subject, text):
    plain = MIMEText(text, 'plain')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = COMMASPACE.join(to_list)
    msg.attach(plain)
    s = smtplib.SMTP('xad-com.mail.protection.outlook.com')
    s.sendmail(sender, to_list, msg.as_string())
    s.quit()

def send_msg(subject, text, done_items=None):
    NEWLINE = ''
    body = text + '\n' + 'Finished tasks:\n'
    body += '' if done_items is None else NEWLINE.join(done_items)
    _send_msg(subject, body)
