defmodule Kite.TestHandler do
  @moduledoc false

  def get_cursor, do: nil
  def put_cursor(_seq), do: :ok
  def handle_event(_payload, _context), do: :ok
end
