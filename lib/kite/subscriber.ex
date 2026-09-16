defmodule Kite.Subscriber do
  @moduledoc """
  The internal GenServer implementation for the jetstream subscriber, use through the `Kite` root module.

  Connects to the given Jetstream V2 server with the configured `collections` and `dids` filters.

  Heartbeat ping/pong messages are properly handled. This is important because some servers may
  clean up inactive connections, and if we're subscribing with filters that mean there's a lot
  of idle time between events, we would otherwise get the connection closed on us.

  Cursors are flushed every @flush_interval rather than per write to avoid unnecessary writes.
  """

  use GenServer

  require Logger

  @type_prefix "network.bsky.jetstream.subscribeEvents#"
  @path "/xrpc/network.bsky.jetstream.subscribeEvents"
  @subprotocol "xrpc.v1.json"
  # Jetstream V2 supports both timestamps and sequential numbers as cursors. You
  # can give it a very old timestamp to get the oldest thing that's still being
  # retained. The window is an implementation detail but roughly 1-2 days of data.
  # The line below is 2001-09-09 as unix microseconds. Anything >= 1e15 is considered a timestamp.
  @oldest_retained 1_000_000_000_000_000
  @flush_interval 5_000

  def start_link(opts) do
    if Keyword.get(opts, :enabled, true) do
      GenServer.start_link(__MODULE__, opts)
    else
      :ignore
    end
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    handler = Keyword.fetch!(opts, :handler)
    Process.set_label(handler)

    %URI{host: host, port: port, scheme: scheme} = URI.parse(Keyword.fetch!(opts, :endpoint))
    {transport, upgrade} = schemes(scheme)

    kinds = Keyword.get(opts, :kinds, [])
    collections = Keyword.get(opts, :collections, [])
    dids = Keyword.get(opts, :dids, [])
    flush_interval = Keyword.get(opts, :flush_interval, @flush_interval)
    base_backoff = Keyword.get(opts, :base_backoff, 1_000)
    context = Keyword.get(opts, :context, %{})
    skip_flush = Keyword.get(opts, :skip_flush, false)
    skip_continue = Keyword.get(opts, :skip_continue, false)

    state = %{
      handler: handler,
      host: host,
      port: port,
      transport: transport,
      upgrade: upgrade,
      collections: collections,
      kinds: kinds -- [:info],
      wanted: Enum.map(kinds, &(@type_prefix <> Atom.to_string(&1))),
      dids: dids,
      flush_interval: flush_interval,
      base_backoff: base_backoff,
      cursor: nil,
      resume: nil,
      flushed: nil,
      attempt: 0,
      conn: nil,
      ref: nil,
      ws: nil,
      status: nil,
      headers: nil,
      body: [],
      context: context
    }

    if not skip_flush do
      Process.send_after(self(), :flush, state.flush_interval)
    end

    if skip_continue do
      {:ok, state}
    else
      {:ok, state, {:continue, :connect}}
    end
  end

  @impl GenServer
  def handle_continue(:connect, state) do
    case connect(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        {:noreply, retry(state)}

      {:error, conn, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        Mint.HTTP.close(conn)
        {:noreply, retry(state)}
    end
  end

  @impl GenServer
  def handle_info({:EXIT, _pid, reason}, state) when reason in [:normal, :shutdown] do
    {:noreply, state}
  end

  def handle_info({:EXIT, _pid, reason}, state) do
    {:stop, reason, state}
  end

  def handle_info(:connect, state) do
    case connect(state) do
      {:ok, state} ->
        {:noreply, state}

      {:error, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        {:noreply, retry(state)}

      {:error, conn, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        Mint.HTTP.close(conn)
        {:noreply, retry(state)}
    end
  end

  def handle_info(:flush, state) do
    Process.send_after(self(), :flush, state.flush_interval)

    flushed = flush(state.handler, state.cursor, state.flushed)
    {:noreply, %{state | flushed: flushed}}
  end

  def handle_info(_message, %{conn: nil} = state) do
    {:noreply, state}
  end

  def handle_info(message, state) do
    case Mint.WebSocket.stream(state.conn, message) do
      {:ok, conn, responses} ->
        {:noreply, Enum.reduce(responses, %{state | conn: conn}, &handle_response/2)}

      {:error, _conn, reason, _responses} ->
        Logger.warning("Kite: #{inspect(reason)}")
        {:noreply, retry(state)}

      :unknown ->
        {:noreply, state}
    end
  end

  @impl GenServer
  def terminate(_reason, state) do
    flush(state.handler, state.cursor, state.flushed)
  end

  defp connect(state) do
    cursor = state.cursor || get_cursor(state.handler)
    path = path(state.collections, state.kinds, state.dids, state.resume || cursor)

    with {:ok, conn} <-
           Mint.HTTP.connect(state.transport, state.host, state.port, protocols: [:http1]),
         {:ok, conn, ref} <-
           Mint.WebSocket.upgrade(state.upgrade, conn, path, [
             {"sec-websocket-protocol", @subprotocol}
           ]) do
      {:ok, %{state | conn: conn, ref: ref, cursor: cursor, resume: nil}}
    end
  end

  defp path(collections, kinds, dids, cursor) do
    params =
      Enum.map(collections, &{"collections", &1}) ++
        Enum.map(kinds, &{"kinds", &1}) ++
        Enum.map(dids, &{"dids", &1}) ++
        cursor_param(cursor)

    case params do
      [] -> @path
      params -> @path <> "?" <> URI.encode_query(params)
    end
  end

  defp cursor_param(nil), do: []
  defp cursor_param(cursor), do: [{"cursor", cursor}]

  defp handle_response(_message, %{conn: nil} = state) do
    state
  end

  defp handle_response({:status, ref, status}, %{ref: ref} = state) do
    %{state | status: status}
  end

  defp handle_response({:headers, ref, headers}, %{ref: ref} = state) do
    %{state | headers: headers}
  end

  defp handle_response({:data, ref, data}, %{ref: ref, ws: nil} = state) do
    %{state | body: [data | state.body]}
  end

  defp handle_response({:data, ref, data}, %{ref: ref} = state) do
    decode(state, data)
  end

  defp handle_response({:done, ref}, %{ref: ref, status: 101} = state) do
    case Mint.WebSocket.new(state.conn, ref, state.status, state.headers) do
      {:ok, conn, ws} ->
        decode(%{state | conn: conn, ws: ws, body: []}, body(state.body))

      {:error, _conn, reason} ->
        Logger.warning("Kite: upgrade failed #{inspect(reason)}")
        retry(state)
    end
  end

  defp handle_response({:done, ref}, %{ref: ref} = state) do
    rejected(state, JSON.decode(body(state.body)))
  end

  defp handle_response({:error, ref, reason}, %{ref: ref} = state) do
    Logger.warning("Kite: #{inspect(reason)}")
    retry(state)
  end

  defp body(chunks) do
    chunks
    |> Enum.reverse()
    |> IO.iodata_to_binary()
  end

  defp rejected(state, {:ok, %{"error" => "CursorTooOld"} = error}) do
    Logger.warning("Kite: cursor too old #{error["message"]}")
    retry(%{state | resume: @oldest_retained})
  end

  defp rejected(_state, {:ok, %{"error" => "InvalidRequest"} = error}) do
    raise ArgumentError, "jetstream rejected the subscription: #{error["message"]}"
  end

  defp rejected(state, decoded) do
    Logger.warning("Kite: HTTP #{state.status} #{inspect(decoded)}")
    retry(state)
  end

  defp decode(state, "") do
    state
  end

  defp decode(state, data) do
    case Mint.WebSocket.decode(state.ws, data) do
      {:ok, ws, frames} ->
        Enum.reduce(frames, %{state | ws: ws}, &handle_frame/2)

      {:error, _ws, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        retry(state)
    end
  end

  defp handle_frame({:pong, _binary}, state) do
    state
  end

  defp handle_frame(_frame, %{conn: nil} = state) do
    state
  end

  defp handle_frame({:text, text}, state) do
    dispatch(state, JSON.decode!(text), state.context)
  end

  defp handle_frame({:ping, payload}, state) do
    {:ok, ws, data} = Mint.WebSocket.encode(state.ws, {:pong, payload})

    case Mint.WebSocket.stream_request_body(state.conn, state.ref, data) do
      {:ok, conn} ->
        %{state | ws: ws, conn: conn, attempt: 0}

      {:error, _conn, reason} ->
        Logger.warning("Kite: #{inspect(reason)}")
        retry(%{state | ws: ws})
    end
  end

  defp handle_frame({:close, _code, _reason}, state) do
    retry(state)
  end

  defp handle_frame({:error, reason}, state) do
    Logger.warning("Kite: #{inspect(reason)}")
    retry(state)
  end

  defp dispatch(state, %{"$type" => "message", "payload" => payload}, context) do
    if wanted?(state.wanted, payload["$type"]) do
      state.handler.handle_event(payload, context)
    end

    %{state | cursor: payload["seq"] || state.cursor}
  end

  defp dispatch(state, %{"$type" => "error"} = frame, _context) do
    Logger.warning("Kite: #{frame["error"]} #{frame["message"]}")
    retry(state)
  end

  defp retry(state) do
    flushed = flush(state.handler, state.cursor, state.flushed)

    if state.conn do
      Mint.HTTP.close(state.conn)
    end

    Process.send_after(self(), :connect, backoff(state.attempt, state.base_backoff))

    %{
      state
      | conn: nil,
        ws: nil,
        ref: nil,
        status: nil,
        headers: nil,
        body: [],
        flushed: flushed,
        attempt: state.attempt + 1
    }
  end

  defp backoff(attempt, base) do
    :rand.uniform(min(base * Integer.pow(2, attempt), 60_000))
  end

  defp flush(_handler, cursor, cursor), do: cursor
  defp flush(_handler, nil, flushed), do: flushed

  defp flush(handler, cursor, _flushed) do
    put_cursor(handler, cursor)
    cursor
  end

  defp schemes(s) when s in ["https", "wss"], do: {:https, :wss}
  defp schemes(s) when s in ["http", "ws"], do: {:http, :ws}

  defp wanted?([], _type), do: true
  defp wanted?(wanted, type), do: type in wanted

  defp get_cursor(handler) do
    if function_exported?(handler, :get_cursor, 0) do
      handler.get_cursor()
    end
  end

  defp put_cursor(handler, cursor) do
    if function_exported?(handler, :put_cursor, 1) do
      handler.put_cursor(cursor)
    end
  end
end
