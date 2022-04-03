from pydantic import BaseModel
from starlette.websockets import WebSocket

class WSConnection(BaseModel):
    host: str
    msg_count: int = 0
    _ws: WebSocket = None
           
    @property
    def ws(self):
        return self._ws
    
    # @ws.setter will not work. See github.com/samuelcolvin/pydantic/issues/1577
    def set_ws(self, ws):
        self._ws = ws

    class Config:
        underscore_attrs_are_private = True
        extra = "forbid"
        
        
ws = WebSocket({"type": "websocket"}, lambda x: x, lambda x: x)   # dummy object
con = WSConnection(host="foo")
con.set_ws(ws)
assert con.ws