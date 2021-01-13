
# ACc4be4cddc7842bb42a121b271e83ed11
# 80dc699f4d3bf8a16c8f12f26c30f518

# Download the helper library from https://www.twilio.com/docs/python/install
import os
from twilio.rest import Client
import config_parser as conf

# Your Account Sid and Auth Token from twilio.com/console
# and set the environment variables. See http://twil.io/secure
account_sid = conf.sms()["SID"]
auth_token = conf.sms()["auth_token"]
from_number = conf.sms()["from_phone"]
to_number = conf.sms()["to_phone"]
client = Client(account_sid, auth_token)

message = client.messages \
    .create(
         body='This is test',
         from_= from_number,
         to=to_number
     )

print(message.sid)