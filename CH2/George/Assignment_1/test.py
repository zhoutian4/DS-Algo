# Python code to illustrate Sending mail from  
# your Gmail account  
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import config_parser as conf
# mail_content = '''Hello,
# This is a simple mail. There is only text, no attachments are there The mail is sent using Python SMTP library.
# Thank You
# '''
#The mail addresses and password
sender_address = conf.email()["sender"]
sender_pass = conf.email()["sender_pwd"]
receiver_address = conf.email()["receiver"]
#Setup the MIME
message = MIMEMultipart()
message['From'] = sender_address
message['To'] = receiver_address
message['Subject'] = 'Algo-Trading execution has an Error'   #The subject line
#The body and the attachments for the mail
message.attach(MIMEText(mail_content, 'plain'))
#Create SMTP session for sending the mail
session = smtplib.SMTP('smtp.gmail.com', 587) #use gmail with port
session.starttls() #enable security
session.login(sender_address, sender_pass) #login with mail_id and password
text = message.as_string()
session.sendmail(sender_address, receiver_address, text)
session.quit()
