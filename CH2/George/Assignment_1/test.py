# Download the helper library from https://www.twilio.com/docs/python/install
import os
from twilio.rest import Client


# Your Account Sid and Auth Token from twilio.com/console
# and set the environment variables. See http://twil.io/secure
account_sid = os.environ['ACc4be4cddc7842bb42a121b271e83ed11']
auth_token = os.environ['d3b747ba000aff1c68e756e1fac26ad8']
client = Client(account_sid, auth_token)

message = client.messages \
    .create(
         body='test',
         from_='+13344639051',
         to='6472857154'
     )

print(message.sid)
