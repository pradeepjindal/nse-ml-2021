## WebSocket
from smartapi import WebSocket
FEED_TOKEN = "your feed token"

#fetch the feedtoken
feedToken = obj.getfeedToken()
FEED_TOKEN = feedToken

CLIENT_CODE ="your client Id"
token = "channel you want the information of" #"nse_cm|2885&nse_cm|1594&nse_cm|11536"

ss = WebSocket(FEED_TOKEN, CLIENT_CODE)

def on_tick(ws, tick):
    print("Ticks: {}".format(tick))

def on_connect(ws, response):
    ws.send_request(token)

def on_close(ws, code, reason):
    ws.stop()

# Assign the callbacks.
ss.on_ticks = on_tick
ss.on_connect = on_connect
ss.on_close = on_close

ss.connect()
