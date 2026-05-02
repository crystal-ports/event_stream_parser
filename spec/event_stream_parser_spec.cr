# frozen_string_literal: true

require "./spec_helper"

Spectator.describe EventStreamParser::Parser do
  macro before(&block)
    before_each do
      {{block.body}}
    end
  end

  macro expect(events)
    %expression = ::Spectator::Value.new(@events.not_nil!, "@events")
    %location = ::Spectator::Location.new({{events.filename || @def.filename}}, {{events.line_number || 0}})
    ::Spectator::Expectation::Target.new(%expression, %location).to(eq({{events}}))
  end

  before do
    @event_stream_parser = EventStreamParser::Parser.new
    @events = [] of Tuple(String, String, String, Int32?)
  end

  describe "feed" do
    it "doesn't yield until empty line" do
      feed <<-CHUNK
        data: hello
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "doesn't yield with just an event field" do
      feed <<-CHUNK
        event: greeting

        #
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "doesn't yield with just an id field" do
      feed <<-CHUNK
        id: event-1

        #
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "doesn't yield with just a retry field" do
      feed <<-CHUNK
        retry: 300

        #
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "yields with a data field" do
      feed <<-CHUNK
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "", nil}
      ]
    end

    it "yields with data and event fields" do
      feed <<-CHUNK
        event: greeting
        data: hello

        #
      CHUNK

      expect [
        {"greeting", "hello", "", nil}
      ]
    end

    it "yields with data and id fields" do
      feed <<-CHUNK
        id: event-1
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "event-1", nil}
      ]
    end

    it "yields with data and retry fields" do
      feed <<-CHUNK
        retry: 300
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "", 300}
      ]
    end

    it "yields with all fields" do
      feed <<-CHUNK
        retry: 300
        id: event-1
        event: greeting
        data: hello

        #
      CHUNK

      expect [
        {"greeting", "hello", "event-1", 300}
      ]
    end

    it "ignores unknown fields" do
      feed <<-CHUNK
        foo: 1
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "", nil}
      ]
    end

    it "ignores empty lines" do
      feed <<-CHUNK

        #
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "ignores lines starting with a colon" do
      feed <<-CHUNK
        :comment

        #
      CHUNK

      expect [] of Tuple(String, String, String, Int32?)
    end

    it "joins adjacent data fields with a new line" do
      feed <<-CHUNK
        data: hello
        data: world

        #
      CHUNK

      expect [
        {"", "hello\nworld", "", nil}
      ]
    end

    it "treats CR as line delimiter" do
      feed <<-CHUNK.split("\n").join("\r")
        event: greeting
        data: hello
        data: world

        #
      CHUNK

      expect [
        {"greeting", "hello\nworld", "", nil}
      ]
    end

    it "treats CRLF as line delimiter" do
      feed <<-CHUNK.split("\n").join("\r\n")
        event: greeting
        data: hello
        data: world

        #
      CHUNK

      expect [
        {"greeting", "hello\nworld", "", nil}
      ]
    end

    it "handles fragmented CRLF" do
      feed "data: hello\r"
      feed "\nevent: greeting\r\n\r\n"

      expect [
        {"greeting", "hello", "", nil}
      ]
    end

    it "yields multiple events" do
      feed <<-CHUNK
        data: hello

        data: world

        #
      CHUNK

      expect [
        {"", "hello", "", nil},
        {"", "world", "", nil}
      ]
    end

    it "resets event type" do
      feed <<-CHUNK
        event: greeting
        data: hello

        data: world

        #
      CHUNK

      expect [
        {"greeting", "hello", "", nil},
        {"", "world", "", nil}
      ]
    end

    it "preserves last event id" do
      feed <<-CHUNK
        id: event-1
        data: hello

        data: world

        id: event-2
        data: bye

        #
      CHUNK

      expect [
        {"", "hello", "event-1", nil},
        {"", "world", "event-1", nil},
        {"", "bye", "event-2", nil}
      ]
    end

    it "preserves reconnection time" do
      feed <<-CHUNK
        data: hello

        retry: 300
        data: world

        data: bye

        #
      CHUNK

      expect [
        {"", "hello", "", nil},
        {"", "world", "", 300},
        {"", "bye", "", 300}
      ]
    end

    it "ignores non-decimal retry field value" do
      feed <<-CHUNK
        retry: a1
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "", nil}
      ]
    end

    it "ignores id field value with a null" do
      feed <<-CHUNK
        id: event-\u0000
        data: hello

        #
      CHUNK

      expect [
        {"", "hello", "", nil}
      ]
    end

    it "treats line without a colon as empty field" do
      feed <<-CHUNK
        data

        id: event-1
        data: hello

        id
        data: world

        #
      CHUNK

      expect [
        {"", "", "", nil},
        {"", "hello", "event-1", nil},
        {"", "world", "", nil}
      ]
    end

    it "treats a single space after colon as optional" do
      feed <<-CHUNK.delete('|')
        data:hello

        data: world

        data:  bye

        data:

        data: |

        data:  |

        #
      CHUNK

      expect [
        {"", "hello", "", nil},
        {"", "world", "", nil},
        {"", " bye", "", nil},
        {"", "", "", nil},
        {"", "", "", nil},
        {"", " ", "", nil}
      ]
    end

    it "yields events on subsequent calls" do
      chunks = <<-CHUNK.split("\n").map { |line| "#{line}\n" }
        event: greeting
        data: hello
        data: world

        event: farewell
        data: bye

        #
      CHUNK

      chunks.each { |chunk| feed(chunk) }

      expect [
        {"greeting", "hello\nworld", "", nil},
        {"farewell", "bye", "", nil}
      ]
    end

    describe "stream" do
      it "yields events" do
        chunks = <<-CHUNK.split("\n").map { |line| "#{line}\n" }
          event: greeting
          data: hello
          data: world

          event: farewell
          data: bye

          #
        CHUNK

        stream chunks

        expect [
          {"greeting", "hello\nworld", "", nil},
          {"farewell", "bye", "", nil}
        ]
      end

      it "yields events with non-new-line chunk boundaries" do
        chunks = <<-CHUNK.split('e').map { |line| "#{line}e" }
          event: greeting
          data: hello
          data: world

          event: farewell
          data: bye
          data: world

          #
        CHUNK

        stream chunks

        expect [
          {"greeting", "hello\nworld", "", nil},
          {"farewell", "bye\nworld", "", nil}
        ]
      end
    end
  end

  private def feed(chunk : String)
    @event_stream_parser.not_nil!.feed(normalize_chunk(chunk)) do |type, data, id, reconnection_time|
      @events.not_nil! << {type, data, id, reconnection_time}
    end
  end

  private def stream(chunks)
    stream = @event_stream_parser.not_nil!.stream do |type, data, id, reconnection_time|
      @events.not_nil! << {type, data, id, reconnection_time}
    end

    chunks.each { |chunk| stream.call(normalize_chunk(chunk)) }
  end

  private def normalize_chunk(chunk : String)
    chunk
      .gsub(/\A  /, "")
      .gsub(/\n  /, "\n")
      .gsub(/\r\n  /, "\r\n")
      .gsub(/\r  /, "\r")
  end
end
