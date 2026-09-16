defmodule KiteSubscriberTest do
  use ExUnit.Case, async: true
  use Mimic

  alias Kite.Subscriber
  alias Kite.TestHandler
  alias Kite.Cursorless

  test "connects with the configured filters and delivers a commit" do
    commit = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "did": "did:plc:abc",
        "seq": 42,
        "time": "2026-08-31T07:50:25.331672Z",
        "collection": "app.bsky.feed.post",
        "rkey": "3mu",
        "rev": "3mu",
        "operation": "create",
        "cid": "bafy",
        "record": {"text": "hello"}
      }
    }
    """

    path =
      "/xrpc/network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post&kinds=commit&cursor=41"

    expect(TestHandler, :get_cursor, fn -> 41 end)

    expect(Mint.HTTP, :connect, fn :https, "jetstream.example", 443, [protocols: [:http1]] ->
      {:ok, :conn}
    end)

    expect(Mint.WebSocket, :upgrade, fn :wss,
                                        :conn,
                                        ^path,
                                        [{"sec-websocket-protocol", "xrpc.v1.json"}] ->
      {:ok, :conn, :ref}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, commit}]}
    end)

    expect(Mint.WebSocket, :new, fn :conn, :ref, 101, [] -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^commit -> {:ok, :ws, [text: commit]} end)

    expect(TestHandler, :handle_event, fn payload, _context ->
      assert payload["seq"] == 42
      assert payload["collection"] == "app.bsky.feed.post"
      :ok
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         collections: ["app.bsky.feed.post"],
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    :sys.get_state(subscriber)
  end

  test "omits the cursor parameter when nothing has been stored" do
    path =
      "/xrpc/network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post&kinds=commit"

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)

    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, ^path, _headers ->
      {:ok, :conn, :ref}
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         collections: ["app.bsky.feed.post"],
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)

    :sys.get_state(subscriber)
  end

  test "drops events whose kind is not wanted" do
    identity = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#identity",
        "seq": 42
      }
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, identity}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^identity -> {:ok, :ws, [text: identity]} end)

    reject(TestHandler, :handle_event, 2)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    :sys.get_state(subscriber)
  end

  test "delivers an info frame when :info was configured" do
    info = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#info",
        "name": "OutdatedCursor",
        "message": "requested timestamp cursor below retention floor"
      }
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, info}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^info -> {:ok, :ws, [text: info]} end)

    expect(TestHandler, :handle_event, fn payload, _context ->
      assert payload["name"] == "OutdatedCursor"
      :ok
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:info],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    :sys.get_state(subscriber)
  end

  test "decodes websocket bytes that arrive before the upgrade is done" do
    commit = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "seq": 42
      }
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn,
       [
         {:status, :ref, 101},
         {:headers, :ref, []},
         {:data, :ref, commit},
         {:done, :ref}
       ]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^commit -> {:ok, :ws, [text: commit]} end)

    expect(TestHandler, :handle_event, fn payload, _context ->
      assert payload["seq"] == 42
      :ok
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)

    :sys.get_state(subscriber)
  end

  test "answers a ping with a pong" do
    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, "ping-frame"}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, "ping-frame" -> {:ok, :ws, [ping: "1"]} end)
    expect(Mint.WebSocket, :encode, fn :ws, {:pong, "1"} -> {:ok, :ws, :pong_data} end)
    expect(Mint.WebSocket, :stream_request_body, fn :conn, :ref, :pong_data -> {:ok, :conn} end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    :sys.get_state(subscriber)
  end

  test "persists the last seq on the flush interval" do
    test_pid = self()

    commit = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "seq": 42
      }
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, commit}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^commit -> {:ok, :ws, [text: commit]} end)

    expect(TestHandler, :put_cursor, fn seq -> send(test_pid, {:flushed, seq}) end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         flush_interval: 0,
         skip_continue: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    assert_receive {:flushed, 42}
  end

  test "persists the last seq on shutdown" do
    commit = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "seq": 42
      }
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, commit}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^commit -> {:ok, :ws, [text: commit]} end)

    expect(TestHandler, :put_cursor, fn 42 -> :ok end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    GenServer.stop(subscriber)
  end

  @tag :capture_log
  test "reconnects from the retention floor after a CursorTooOld rejection" do
    test_pid = self()

    rejection = """
    {
      "error": "CursorTooOld",
      "message": "cursor 41 below lookback floor 100"
    }
    """

    sentinel_path =
      "/xrpc/network.bsky.jetstream.subscribeEvents?kinds=commit&cursor=1000000000000000"

    expect(TestHandler, :get_cursor, fn -> 41 end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :rejected ->
      {:ok, :conn,
       [
         {:status, :ref, 400},
         {:headers, :ref, []},
         {:data, :ref, rejection},
         {:done, :ref}
       ]}
    end)

    expect(TestHandler, :put_cursor, fn seq -> assert seq == 41 end)
    expect(Mint.HTTP, :close, fn :conn -> {:ok, :conn} end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)

    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, ^sentinel_path, _headers ->
      send(test_pid, :reconnected)
      {:ok, :conn, :ref}
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true,
         base_backoff: 1}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :rejected)

    assert_receive :reconnected, 2000
  end

  @tag :capture_log
  test "raises on an InvalidRequest rejection instead of retrying" do
    Process.flag(:trap_exit, true)

    rejection = """
    {
      "error": "InvalidRequest",
      "message": "unknown kind bogus"
    }
    """

    expect(TestHandler, :get_cursor, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :rejected ->
      {:ok, :conn,
       [
         {:status, :ref, 400},
         {:headers, :ref, []},
         {:data, :ref, rejection},
         {:done, :ref}
       ]}
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :rejected)

    assert_receive {:EXIT, ^subscriber, {%ArgumentError{}, _stacktrace}}
  end

  @tag :capture_log
  test "closes the socket when the upgrade fails" do
    test_pid = self()

    expect(TestHandler, :get_cursor, 2, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :opened} end)

    expect(Mint.WebSocket, :upgrade, fn _scheme, :opened, _path, _headers ->
      {:error, :opened, %Mint.TransportError{reason: :closed}}
    end)

    expect(Mint.HTTP, :close, fn :opened -> {:ok, :opened} end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)

    expect(Mint.WebSocket, :upgrade, fn _scheme, :conn, _path, _headers ->
      send(test_pid, :reconnected)
      {:ok, :conn, :ref}
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true,
         base_backoff: 1}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)

    assert_receive :reconnected
  end

  test "reconnects once when a close frame and a transport error arrive together" do
    test_pid = self()

    expect(TestHandler, :get_cursor, 2, fn -> nil end)
    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts -> {:ok, :conn} end)
    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :new, fn _conn, _ref, _status, _headers -> {:ok, :conn, :ws} end)

    expect(Mint.WebSocket, :stream, fn :conn, :dropped ->
      {:ok, :conn,
       [
         {:data, :ref, "close-frame"},
         {:error, :ref, %Mint.TransportError{reason: :closed}}
       ]}
    end)

    expect(Mint.WebSocket, :decode, fn :ws, "close-frame" -> {:ok, :ws, [{:close, 1000, ""}]} end)
    expect(Mint.HTTP, :close, fn :conn -> {:ok, :conn} end)

    expect(Mint.HTTP, :connect, fn _scheme, _host, _port, _opts ->
      send(test_pid, :reconnected)
      {:ok, :conn}
    end)

    expect(Mint.WebSocket, :upgrade, fn _scheme, _conn, _path, _headers -> {:ok, :conn, :ref} end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: TestHandler,
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true,
         base_backoff: 1}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(TestHandler, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :dropped)

    assert_receive :reconnected
    refute_receive :reconnected, 50
  end

  test "supports cursorless handlers" do
    commit = """
    {
      "$type": "message",
      "payload": {
        "$type": "network.bsky.jetstream.subscribeEvents#commit",
        "did": "did:plc:abc",
        "seq": 42,
        "time": "2026-08-31T07:50:25.331672Z",
        "collection": "app.bsky.feed.post",
        "rkey": "3mu",
        "rev": "3mu",
        "operation": "create",
        "cid": "bafy",
        "record": {"text": "hello"}
      }
    }
    """

    path =
      "/xrpc/network.bsky.jetstream.subscribeEvents?collections=app.bsky.feed.post&kinds=commit"

    expect(Mint.HTTP, :connect, fn :https, "jetstream.example", 443, [protocols: [:http1]] ->
      {:ok, :conn}
    end)

    expect(Mint.WebSocket, :upgrade, fn :wss,
                                        :conn,
                                        ^path,
                                        [{"sec-websocket-protocol", "xrpc.v1.json"}] ->
      {:ok, :conn, :ref}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :handshake ->
      {:ok, :conn, [{:status, :ref, 101}, {:headers, :ref, []}, {:done, :ref}]}
    end)

    expect(Mint.WebSocket, :stream, fn :conn, :frame ->
      {:ok, :conn, [{:data, :ref, commit}]}
    end)

    expect(Mint.WebSocket, :new, fn :conn, :ref, 101, [] -> {:ok, :conn, :ws} end)
    expect(Mint.WebSocket, :decode, fn :ws, ^commit -> {:ok, :ws, [text: commit]} end)

    expect(Cursorless, :handle_event, fn payload, _context ->
      assert payload["collection"] == "app.bsky.feed.post"
      :ok
    end)

    subscriber =
      start_link_supervised!(
        {Subscriber,
         endpoint: "wss://jetstream.example",
         handler: Cursorless,
         collections: ["app.bsky.feed.post"],
         kinds: [:commit],
         skip_continue: true,
         skip_flush: true}
      )

    Mimic.allow(Mint.HTTP, self(), subscriber)
    Mimic.allow(Mint.WebSocket, self(), subscriber)
    Mimic.allow(Cursorless, self(), subscriber)

    send(subscriber, :connect)
    send(subscriber, :handshake)
    send(subscriber, :frame)

    :sys.get_state(subscriber)
  end
end
