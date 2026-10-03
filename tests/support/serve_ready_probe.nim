import std/[atomics, monotimes, os, posix, strutils, times]
import mummy

# Fixture stop intent mirrors process-owned engine shutdown; no work in signals.
var stopRequested, sealed: Atomic[bool]
var ownerCreated: bool
var readyCount: int
var ownerThread: Thread[Server]

type SignalHandler = proc(number: cint) {.noconv.}
proc signalHandler(_: cint) {.noconv, gcsafe, raises: [].} =
  stopRequested.store(true, moRelaxed)
proc setSignalHandler(number: cint, handler: SignalHandler): SignalHandler
    {.importc: "signal", header: "<signal.h>".}

doAssert setSignalHandler(SIGTERM, signalHandler) != cast[SignalHandler](-1)
doAssert setSignalHandler(SIGINT, signalHandler) != cast[SignalHandler](-1)

proc owner(server: Server) {.thread.} =
  echo "READY"
  flushFile(stdout)
  let deadline = getMonoTime() + initDuration(seconds = 5)
  while not stopRequested.load(moRelaxed):
    doAssert getMonoTime() < deadline, "fixture owner stop intent did not arrive"
    sleep(1)
  server.close()
  sealed.store(true, moRelaxed)

proc ready(server: Server) {.gcsafe, raises: [ResourceExhaustedError].} =
  createThread(ownerThread, owner, server)
  ownerCreated = true
  inc readyCount

proc handler(request: Request) =
  request.respond(200, body = "actual-ready")

let args = commandLineParams()
let server = newServer(handler, workerThreads = 1)
if args[0] == "before-listen":
  echo "CONSTRUCTED"
  flushFile(stdout)
  doAssert stdin.readLine() == "serve"

if args[0] == "bind-failure":
  doAssertRaises(MummyError):
    server.serve(Port(args[1].parseInt()), "127.0.0.1", onReady = ready)
  doAssert readyCount == 0 and not ownerCreated
  echo "BIND_FAILED_WITHOUT_OWNER"
else:
  try:
    server.serve(Port(args[1].parseInt()), "127.0.0.1", onReady = ready)
  finally:
    stopRequested.store(true, moRelaxed)
    if ownerCreated:
      joinThread(ownerThread)
  doAssert readyCount == 1 and sealed.load(moRelaxed)
  echo "SEALED_AND_JOINED"
