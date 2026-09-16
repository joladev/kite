defmodule Kite do
  @external_resource "README.md"
  @moduledoc "README.md"
             |> File.read!()
             |> String.split("<!-- MDOC !-->")
             |> Enum.fetch!(1)

  @callback get_cursor() :: integer() | nil
  @callback put_cursor(integer()) :: any()
  @callback handle_event(map(), term()) :: any()
  @optional_callbacks get_cursor: 0, put_cursor: 1

  defmacro __using__(opts) do
    quote do
      @behaviour Kite

      def start_link(overrides \\ []) do
        unquote(opts)
        |> Keyword.merge(overrides)
        |> Keyword.put(:handler, __MODULE__)
        |> Kite.Subscriber.start_link()
      end

      def child_spec(overrides) do
        %{id: __MODULE__, start: {__MODULE__, :start_link, [overrides]}}
      end

      defoverridable child_spec: 1
    end
  end
end
