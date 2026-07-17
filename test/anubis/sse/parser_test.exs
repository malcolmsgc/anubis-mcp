defmodule Anubis.SSE.ParserTest do
  use ExUnit.Case, async: true

  alias Anubis.MCP.Message
  alias Anubis.SSE.Parser

  @moduletag capture_log: true

  test "parses a simple event with only data" do
    sse = "data: hello world\n\n"
    assert [event] = Parser.run(sse)

    assert event.data == "hello world"
    # default event type
    assert event.event == "message"
    assert event.id == nil
    assert event.retry == nil
  end

  test "parses event with multiple data lines" do
    sse = "data: first line\ndata: second line\n\n"
    assert [event] = Parser.run(sse)
    assert event.data == "first line\nsecond line"
  end

  test "parses event with id and custom event type" do
    sse = "id: 42\nevent: custom\ndata: sample event\n\n"
    assert [event] = Parser.run(sse)

    assert event.id == "42"
    assert event.event == "custom"
    assert event.data == "sample event"
  end

  test "parses event with retry value" do
    sse = "retry: 3000\ndata: test retry\n\n"
    assert [event] = Parser.run(sse)

    assert event.retry == 3000
    assert event.data == "test retry"
  end

  test "ignores comment lines" do
    sse = ": this is a comment\ndata: real data\n\n"
    assert [event] = Parser.run(sse)

    assert event.data == "real data"
  end

  test "parses multiple events" do
    sse = """
    data: first event

    data: second event
    """

    events = Parser.run(sse)

    assert length(events) == 2
    assert Enum.at(events, 0).data == "first event"
    assert Enum.at(events, 1).data == "second event"
  end

  test "handles fields with no colon value" do
    sse = "data\n\n"
    assert Enum.empty?(Parser.run(sse))
  end

  describe "feed/2 incremental parsing" do
    test "emits a whole event with no remainder" do
      assert {[event], ""} = Parser.feed("", "data: hello\n\n")
      assert event.data == "hello"
    end

    test "reassembles an event split across two feeds" do
      assert {[], "data: hel"} = Parser.feed("", "data: hel")
      assert {[event], ""} = Parser.feed("data: hel", "lo\n\n")
      assert event.data == "hello"
    end

    test "reassembles when the CRLF terminator is split across chunks" do
      assert {[first, second], ""} =
               Parser.feed("data: x\r", "\n\r\ndata: y\r\n\r\n")

      assert first.data == "x"
      assert second.data == "y"
    end

    test "emits complete events and holds a trailing partial" do
      assert {[event], "data: b"} = Parser.feed("", "data: a\n\ndata: b")
      assert event.data == "a"

      assert {[event_b], ""} = Parser.feed("data: b", "\n\n")
      assert event_b.data == "b"
    end

    test "buffers everything when there is no terminator yet" do
      assert {[], "data: partial"} = Parser.feed("", "data: partial")
    end

    test "reassembles multi-line data split across chunks" do
      assert {[event], ""} = Parser.feed("data: first\ndata: sec", "ond\n\n")
      assert event.data == "first\nsecond"
    end

    test "takes only the last terminator when several events arrive together" do
      assert {[a, b], "data: c"} = Parser.feed("", "data: a\n\ndata: b\n\ndata: c")
      assert a.data == "a"
      assert b.data == "b"
    end

    test "does not crash on a bare terminator" do
      assert {[], ""} = Parser.feed("", "\n\n")
    end
  end

  describe "handles MCP message event correctly" do
    test "handles MCP endpoint event correctly" do
      sse = "event: endpoint\r\ndata: /messages/?session_id=123\r\n\r\n"

      assert [event] = Parser.run(sse)
      assert event.event == "endpoint"
      assert event.data == "/messages/?session_id=123"
    end

    test "handles MCP request correctly" do
      assert {:ok, msg} = Message.encode_request(%{"method" => "ping"}, "req-123")
      sse = "event: message\r\ndata: #{msg}\r\n\r\n"

      assert [event] = Parser.run(sse)
      assert event.event == "message"

      assert {:ok, [req]} = Message.decode(event.data)
      assert req["id"] == "req-123"
      assert req["method"] == "ping"
    end

    test "handles MCP notification correctly" do
      assert {:ok, msg} =
               Message.encode_notification(%{
                 "method" => "notifications/cancelled",
                 "params" => %{"requestId" => 123, "reason" => "user_cancelled"}
               })

      sse = "event: message\r\ndata: #{msg}\r\n\r\n"

      assert [event] = Parser.run(sse)
      assert event.event == "message"

      assert {:ok, [notif]} = Message.decode(event.data)
      assert notif["method"] == "notifications/cancelled"
      assert notif["params"]["requestId"] == 123
      assert notif["params"]["reason"] == "user_cancelled"
    end

    test "handles MCP response correctly" do
      assert {:ok, msg} = Message.encode_response(%{"result" => "pong"}, "req-123")

      sse = "event: message\r\ndata: #{msg}\r\n\r\n"

      assert [event] = Parser.run(sse)
      assert event.event == "message"

      assert {:ok, [res]} = Message.decode(event.data)
      assert res["id"] == "req-123"
      assert res["result"] == "pong"
    end

    test "handles multiple MCP events correctly" do
      sse = """
      event: endpoint
      data: /messages/?session_id=123

      event: message
      data: {"jsonrpc":"2.0","method":"ping","params":{},"id":1}

      event: message
      data: {"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":123,"reason":"user_cancelled"}}

      event: message
      data: {"jsonrpc":"2.0","result":"pong","id":1}
      """

      events = Parser.run(sse)

      assert length(events) == 4
      assert Enum.at(events, 0).event == "endpoint"
      assert Enum.at(events, 1).event == "message"
      assert Enum.at(events, 2).event == "message"
      assert Enum.at(events, 3).event == "message"
    end
  end
end
