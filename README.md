# Kite

Kite is an atproto Jetstream V2 subscriber library. Use it to subscribe to a jetstream server and get a live stream of records being created, updated, and deleted on the AT protocol that powers services like Bluesky, Tangled, and Leaflet.

You can either connect without a cursor to just get live data as it comes through, or define callbacks for persisted cursors to keep track of your position and never miss an event. Jetstream also supports defining filters on collections, such as `app.bsky.feed.post` for Bluesky posts or `site.standard.document` for standard.site blog posts, and even filtering by specific account identifiers, aka dids. If your handler raises, the subscriber will restart from the last known cursor, which may include records you have already processed. In other words, event delivery is at least once.

Kite implements websocket keep alive with ping/pong heartbeats and gracefully restarts on closed connections, with backoff. If you're falling behind on processing events, Kite will apply back pressure, but if you fall too far behind the upstream will reject the connection and Kite automatically falls back to requesting the oldest data available.

Kite requires Elixir 1.18 (released 2024) and OTP 25 or higher because it uses the built-in `JSON` module, and other modern niceties like `Process.set_label` (1.17). When zstd compression lands that optional feature will require OTP 28.

Kite is used live in production at [shelf.cafe](https://shelf.cafe).

<!-- MDOC !-->

Define your compile time options and implement the available callbacks to get a Jetstream V2 compatible subscriber.

## Example

    defmodule MyApp.Posts do
      use Kite,
        endpoint: "wss://jetstream.us-east.bsky.network",
        collections: ["app.bsky.feed.post"],
        kinds: [:commit]

      def handle_event(payload, _context), do: IO.inspect(payload)
    end

Add your newly defined subscriber to your `application.ex` and you're ready to go.

    @impl true
    def start(_type, _args) do
  
      children = [
        {MyApp.Posts, []} # <- Add your new module to the list of children.
        ...
      ]

      opts = [strategy: :one_for_one, name: MyApp.Supervisor]
      Supervisor.start_link(children, opts)
    end

Or, for some more ad-hoc experimenting, try `MyApp.Posts.start_link([])` in IEx.

## Callbacks

`handle_event/2` is required. If you don't define `get_cursor/0` and `put_cursor/1` the subscriber always starts from the latest event available.

For most practical use cases you want to implement `get_cursor/0` and `put_cursor/1`, in order to gracefully handle restarts or intermittent issues.

The second argument of `handle_event` is context, which is user defined data passed when starting the subscriber. This can be used to make metadata available to the handler where you run multiple instances of the same implementation.

## Options

The supported options are:

* `endpoint` - *required* - a websocket URL for a jetstream, like "wss://jetstream.us-east.bsky.network" or "wss://jetstream2.fr.hose.cam/".
* `collections` - a list of NSID strings representing the resources you're interested in, like `"app.bsky.feed.post"` or `"site.standard.document"`.
* `kinds` - a list of atoms corresponding to the types of events you want to see, normally just `[:commit]`.
* `context` - use this to pass anything you want to the `handle_event` callback, it will be passed as the second argument.
* `enabled` - default `true`, set to `false` to prevent the subscriber from connecting to the jetstream, for example in tests.
* `flush_interval` - default 5s, the frequency with which to write the latest cursor state with `put_cursor/1`. Write more frequently to reduce the window
  of potential repeated events on a crash.

Anything set in the `use Kite` options can be overriden when starting the subscriber from your Application children list, eg:

    children = [
      {MyApp.Posts, endpoint: System.fetch_env!("JETSTREAM_URL")}
    ]

## Payloads

Jetstream outputs events in JSON, sending batches over the websocket, and Kite exposes the raw data for you. Jetstream emits 4 different "kinds" of events: `#identity`, `#account`, `#commit`, and `#sync`. For a full reference, check out the [docs](https://atproto.com/specs/sync#repository-event-stream). For now we'll focus on `#commit` which represents changes to atproto records, including create, update, and delete. Note that records can have defined schemas, aka lexicons, but the data in the jetstream is not guaranteed to be validated and correct, so you must do your own validation.

Here's an example Bluesky post (`#commit` kind), in the full handler payload format:

    %{
      "$type" => "network.bsky.jetstream.subscribeEvents#commit",
      "cid" => "bafyreigkx5h...",
      "collection" => "app.bsky.feed.post",
      "did" => "did:plc:user",
      "operation" => "create",
      "record" => %{
        "$type" => "app.bsky.feed.post",
        "createdAt" => "2026-09-16T18:13:33.933Z",
        "langs" => ["nl"],
        "reply" => %{
          "parent" => %{
            "cid" => "bafyreigkx5h...",
            "uri" => "at://did:plc:user/app.bsky.feed.post/rkey"
          },
          "root" => %{
            "cid" => "bafyreigkx5h...",
            "uri" => "at://did:plc:user/app.bsky.feed.post/rkey"
          }
        },
        "text" => "Voeg hier even vvd aan toe."
      },
      "rev" => "3mvn...",
      "rkey" => "3mvn...",
      "seq" => 25947795773,
      "time" => "2026-09-16T18:13:35.185499Z"
    }

And to demonstrate how to write a handler for it, let's say we want to just print the text of the posts.

    def handle_event(%{"operation" => "create"} = commit, _context) do
      %{"record" => record, "did" => did} = commit

      if text = record["text"] do
        IO.inspect(text)
      end
    end

    def handle_event(_commit, _context), do: :ok

<!-- MDOC !-->

## Todo

- [ ] Optional zstd compression using OTP 28's `:zstd`
- [ ] Batch processing
- [ ] Optional structs for the event metadata for commit, account, etc
