"""Transport layer (member B): UDP/TCP, request/response matching, TLS.

Contract with the rest of the mesh:
* Every received datagram or TCP frame is passed to
  ``Dispatcher.dispatch_bytes(data, addr)``; if it returns bytes, send them back.
* Outgoing requests are ``Message`` objects from ``meshweaver.protocol``;
  ``send_rpc(addr, msg, timeout)`` awaits the reply whose ``reply_to`` equals
  ``msg.msg_id`` (register a reply hook with ``Dispatcher.set_reply_hook``).
"""
