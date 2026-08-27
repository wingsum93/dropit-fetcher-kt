# worker init
each worker need to obtain a token for subsequent api calling. refer
post https://api.freshop.ncrcloud.com/2/sessions/create as
application/x-www-form-urlencoded with payload:

app_key	"lindos"
locale	"false"
referrer	"https://www.dropit.bm/"
utc	current epoch milliseconds

The worker only reports healthy after this token is obtained.
