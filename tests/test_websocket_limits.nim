import mummy {.all.}, whisky, std/atomics, std/importutils, std/locks,
    std/net, std/os, std/tables, std/times, std/strutils

privateAccess(ServerObj)

# Real-socket tests for the per-WebSocket transport limits:
# 1) The per-WebSocket receive limit (WebSocketLimits.maxMessageLen),
#    including rejection from the frame header alone, before any payload.
# 2) The pending event/byte caps (maxPendingEvents/maxPendingBytes),
#    including an empty-ping flood breaching at the exact event cap with
#    exactly one terminal cleanup.
# 3) Outbound admission (trySend + maxOutboundEvents/maxOutboundBytes) and
#    completion callbacks, including a stalled reader driving refusal with
#    bounded memory and correct completions.

proc waitFor(startedAt: float64, timeoutSeconds = 10.0) =
  doAssert epochTime() - startedAt < timeoutSeconds, "timed out waiting"
  sleep(5)

proc serveProc(args: tuple[server: Server, port: int]) {.thread.} =
  try:
    args.server.serve(Port(args.port))
  except CatchableError:
    echo "serve failed: ", getCurrentExceptionMsg()

proc rawWebSocketConnect(port: int, path: string): net.Socket =
  ## Opens a raw TCP connection and completes the WebSocket upgrade
  ## handshake, returning the socket for hand-crafted frames.
  result = newSocket()
  result.connect("127.0.0.1", Port(port))
  result.send(
    "GET " & path & " HTTP/1.1\r\n" &
    "Host: 127.0.0.1\r\n" &
    "Connection: Upgrade\r\n" &
    "Upgrade: websocket\r\n" &
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n" &
    "Sec-WebSocket-Version: 13\r\n" &
    "\r\n"
  )
  var response: string
  while not response.endsWith("\r\n\r\n"):
    let c = result.recv(1, timeout = 5000)
    doAssert c.len == 1, "unexpected EOF during WebSocket upgrade"
    response &= c
  doAssert response.startsWith("HTTP/1.1 101")

proc frameHeaderOnly(payloadLen: uint64): string =
  ## A fin + binary + masked frame header declaring payloadLen, with no
  ## payload (and not even the mask) behind it.
  result.add 0x82.char # fin + binary
  result.add 0xFF.char # masked + 127 (64-bit extended length)
  for i in countdown(7, 0):
    result.add char((payloadLen shr (8 * i)) and 0xFF)

proc isDisconnected(ws: whisky.WebSocket, timeout = 10000): bool =
  ## True if the server has closed the connection (close frame or raw TCP
  ## close), false only if the wait timed out with the connection alive.
  try:
    let message = ws.receiveMessage(timeout)
    if message.isSome:
      return false # Received an actual message, still connected
    return false # Timed out, still connected
  except CatchableError:
    return true

block: # 1) Per-WebSocket receive limit
  proc handler(request: Request) =
    case request.path:
    of "/small":
      discard request.upgradeToWebSocket(
        WebSocketLimits(maxMessageLen: 100)
      )
    of "/big":
      discard request.upgradeToWebSocket(
        WebSocketLimits(maxMessageLen: 262158)
      )
    of "/default":
      discard request.upgradeToWebSocket()
    else:
      request.respond(404)

  proc websocketHandler(
    websocket: mummy.WebSocket,
    event: WebSocketEvent,
    message: mummy.Message
  ) =
    if event == MessageEvent and
      message.kind in {mummy.TextMessage, mummy.BinaryMessage}:
      # Echo the received size back
      websocket.send($message.data.len)

  let server = newServer(handler, websocketHandler)

  var serverThread: Thread[tuple[server: Server, port: int]]
  createThread(serverThread, serveProc, (server, 8201))
  server.waitUntilReady()

  block: # A message larger than the server default passes a raised limit
    let ws = newWebSocket("ws://127.0.0.1:8201/big")
    var big = newString(200 * 1024) # Over the 64 KiB server default
    ws.send(big, whisky.BinaryMessage)
    let reply = ws.receiveMessage(10000)
    doAssert reply.isSome and reply.get.data == $(200 * 1024)
    ws.close()

  block: # A socket without limits keeps the server default behavior
    let ws = newWebSocket("ws://127.0.0.1:8201/default")
    var big = newString(100 * 1024)
    ws.send(big, whisky.BinaryMessage)
    doAssert ws.isDisconnected()
    ws.close()

  block: # At the limit passes, one byte over closes the connection
    let ws = newWebSocket("ws://127.0.0.1:8201/small")
    var atLimit = newString(100)
    ws.send(atLimit, whisky.BinaryMessage)
    let reply = ws.receiveMessage(10000)
    doAssert reply.isSome and reply.get.data == "100"
    var overLimit = newString(101)
    ws.send(overLimit, whisky.BinaryMessage)
    doAssert ws.isDisconnected()
    ws.close()

  block: # An over-limit frame is rejected from the header, pre-payload
    let raw = rawWebSocketConnect(8201, "/small")
    raw.send(frameHeaderOnly(1_000_000))
    # The server must close the connection having seen only the 10 header
    # bytes: no mask, no payload, nothing to buffer.
    var closed = false
    try:
      closed = raw.recv(1, timeout = 10000).len == 0
    except CatchableError:
      closed = true
    doAssert closed, "server did not reject the frame from its header"
    raw.close()

  server.close()
  joinThread(serverThread)
  echo "Receive limit tests passed"

var
  gate: Atomic[bool] # While false, the WebSocket handler blocks in OpenEvent
  messageCount, errorCount, closeCount: Atomic[int]

block: # 2) Pending event/byte caps
  proc handler(request: Request) =
    case request.path:
    of "/capped":
      discard request.upgradeToWebSocket(
        WebSocketLimits(maxPendingEvents: 8)
      )
    of "/bytecapped":
      discard request.upgradeToWebSocket(
        WebSocketLimits(maxPendingBytes: 1000)
      )
    else:
      request.respond(404)

  proc websocketHandler(
    websocket: mummy.WebSocket,
    event: WebSocketEvent,
    message: mummy.Message
  ) =
    case event:
    of OpenEvent:
      while not gate.load: # Block dispatch so pending events accumulate
        sleep(1)
    of MessageEvent:
      discard messageCount.fetchAdd(1)
    of ErrorEvent:
      discard errorCount.fetchAdd(1)
    of CloseEvent:
      discard closeCount.fetchAdd(1)

  let server = newServer(handler, websocketHandler)

  var serverThread: Thread[tuple[server: Server, port: int]]
  createThread(serverThread, serveProc, (server, 8202))
  server.waitUntilReady()

  block: # Exactly at the event cap: nothing is dropped
    gate.store(false)
    let ws = newWebSocket("ws://127.0.0.1:8202/capped")
    for i in 0 ..< 8:
      ws.send("", whisky.Ping) # Empty pings ride the same queue and count
    gate.store(true)
    let startedAt = epochTime()
    while messageCount.load < 8:
      waitFor(startedAt)
    doAssert not ws.isDisconnected(500) # Still connected, no breach
    ws.close()
    while closeCount.load < 1:
      waitFor(startedAt)
    doAssert messageCount.load == 8

  block: # An empty-ping flood breaches at the exact event cap
    gate.store(false)
    let ws = newWebSocket("ws://127.0.0.1:8202/capped")
    for i in 0 ..< 9:
      ws.send("", whisky.Ping) # The 9th ping breaches the cap of 8
    # The server closes the connection while the handler is still blocked
    doAssert ws.isDisconnected()
    ws.close()
    gate.store(true)
    let startedAt = epochTime()
    while closeCount.load < 2: # Exactly one cleanup for this connection
      waitFor(startedAt)
    sleep(100) # Nothing further may arrive after the cleanup
    doAssert messageCount.load == 8 # The 8 queued pings were purged, not dispatched
    doAssert closeCount.load == 2
    doAssert errorCount.load == 2

  block: # The byte cap breaches the same way
    gate.store(false)
    let ws = newWebSocket("ws://127.0.0.1:8202/bytecapped")
    var chunk = newString(400)
    ws.send(chunk) # 400 pending bytes
    ws.send(chunk) # 800 pending bytes
    ws.send(chunk) # 1200 > 1000, breach
    doAssert ws.isDisconnected()
    ws.close()
    gate.store(true)
    let startedAt = epochTime()
    while closeCount.load < 3:
      waitFor(startedAt)
    sleep(100)
    doAssert messageCount.load == 8 # Both queued messages were purged
    doAssert closeCount.load == 3

  # After every CloseEvent has been dispatched the queues must be gone
  withLock server.websocketQueuesLock:
    doAssert server.websocketQueues.len == 0
    doAssert server.websocketClaimed.len == 0

  server.close()
  joinThread(serverThread)
  echo "Pending cap tests passed"

const
  floodMessageLen = 256 * 1024
  floodMaxOutboundEvents = 4
  floodMaxOutboundBytes = 1024 * 1024

var
  scenario: Atomic[int] # Which OpenEvent behavior the next connection gets
  sentCount, droppedCount: Atomic[int]
  floodAdmitted, floodRefused: Atomic[int]
  floodDone: Atomic[bool]
  openCloseCount: Atomic[int]

proc onCompletion(
  websocket: mummy.WebSocket,
  completion: SendCompletion
) {.gcsafe.} =
  case completion:
  of SendSent:
    discard sentCount.fetchAdd(1)
  of SendDropped:
    discard droppedCount.fetchAdd(1)

block: # 3) Outbound admission and completion callbacks
  proc handler(request: Request) =
    case request.path:
    of "/capped":
      discard request.upgradeToWebSocket(WebSocketLimits(
        maxOutboundEvents: floodMaxOutboundEvents,
        maxOutboundBytes: floodMaxOutboundBytes
      ))
    of "/uncapped":
      discard request.upgradeToWebSocket()
    else:
      request.respond(404)

  proc websocketHandler(
    websocket: mummy.WebSocket,
    event: WebSocketEvent,
    message: mummy.Message
  ) =
    case event:
    of OpenEvent:
      case scenario.load:
      of 1: # Small sends to a reading client, all must complete as sent
        for i in 0 ..< 3:
          doAssert websocket.trySend("hello", onCompletion = onCompletion)
      of 2: # Without caps, trySend always admits
        for i in 0 ..< 10:
          doAssert websocket.trySend("hello", onCompletion = onCompletion)
      of 3: # Flood a stalled reader until admission refuses
        var consecutiveRefusals = 0
        while consecutiveRefusals < 50:
          var payload = newString(floodMessageLen)
          if websocket.trySend(
            payload, mummy.BinaryMessage, onCompletion = onCompletion
          ):
            discard floodAdmitted.fetchAdd(1)
            consecutiveRefusals = 0
            # Bounded memory: the pending unsent messages can never exceed
            # the event cap (+1 for a completion that has freed its cap
            # reservation but not yet bumped its counter)
            doAssert floodAdmitted.load - sentCount.load -
              droppedCount.load <= floodMaxOutboundEvents + 1
            # A stalled reader cannot make the server admit unboundedly
            doAssert floodAdmitted.load <= 64
          else:
            discard floodRefused.fetchAdd(1)
            inc consecutiveRefusals
            sleep(10)
        floodDone.store(true)
      else:
        discard
    of CloseEvent:
      discard openCloseCount.fetchAdd(1)
    else:
      discard

  let server = newServer(handler, websocketHandler)

  var serverThread: Thread[tuple[server: Server, port: int]]
  createThread(serverThread, serveProc, (server, 8203))
  server.waitUntilReady()

  block: # Sent completions fire once per message handed to the OS
    scenario.store(1)
    let ws = newWebSocket("ws://127.0.0.1:8203/capped")
    for i in 0 ..< 3:
      let message = ws.receiveMessage(10000)
      doAssert message.isSome and message.get.data == "hello"
    let startedAt = epochTime()
    while sentCount.load < 3:
      waitFor(startedAt)
    doAssert droppedCount.load == 0
    ws.close()
    while openCloseCount.load < 1:
      waitFor(startedAt)

  block: # Without outbound caps trySend always admits
    scenario.store(2)
    let ws = newWebSocket("ws://127.0.0.1:8203/uncapped")
    for i in 0 ..< 10:
      let message = ws.receiveMessage(10000)
      doAssert message.isSome and message.get.data == "hello"
    let startedAt = epochTime()
    while sentCount.load < 13:
      waitFor(startedAt)
    doAssert droppedCount.load == 0
    ws.close()
    while openCloseCount.load < 2:
      waitFor(startedAt)

  block: # A stalled reader drives refusal, then drops complete on close
    scenario.store(3)
    let raw = rawWebSocketConnect(8203, "/capped")
    # Never read: the write path backs up, admission must start refusing
    var startedAt = epochTime()
    while not floodDone.load:
      waitFor(startedAt, 30)
    doAssert floodRefused.load >= 50 # Refusal was persistent, not a blip
    let admitted = floodAdmitted.load
    doAssert admitted >= 1
    # Disconnect with messages still queued: they must complete as dropped
    raw.close()
    startedAt = epochTime()
    while sentCount.load + droppedCount.load < 13 + admitted:
      waitFor(startedAt)
    doAssert sentCount.load + droppedCount.load == 13 + admitted
    doAssert droppedCount.load >= 1
    while openCloseCount.load < 3:
      waitFor(startedAt)

  # Every outbound cap state must be cleaned up with its socket
  withLock server.outboundLock:
    doAssert server.outboundStates.len == 0
  withLock server.websocketQueuesLock:
    doAssert server.websocketQueues.len == 0

  server.close()
  joinThread(serverThread)
  echo "Outbound admission tests passed"
