# frozen_string_literal: true

require "./event_stream_parser/version"

module EventStreamParser
  ##
  # Implements a spec-compliant event stream parser, following:
  #
  # https://html.spec.whatwg.org/multipage/server-sent-events.html
  # Section: 9.2.6 Interpreting an event stream
  #
  # Code comments are copied from the spec.
  #
  class Parser
    @buffer : String
    @data_buffer : String
    @event_type_buffer : String
    @last_event_id_buffer : String
    @last_delimiter : String?
    @reconnection_time : Int32?

    def initialize
      ##
      # When a stream is parsed, a data buffer, an event type buffer, and a last
      # event ID buffer must be associated with it. They must be initialized to
      # the empty string.
      #
      @data_buffer = ""
      @event_type_buffer = ""
      @last_event_id_buffer = ""

      @reconnection_time = nil
      @buffer = ""
      @last_delimiter = nil
    end

    def feed(chunk : String, &proc : String, String, String, Int32? ->)
      @buffer += chunk

      ##
      # The stream must then be parsed by reading everything line by line, with a
      # U+000D CARRIAGE RETURN U+000A LINE FEED (CRLF) character pair, a single
      # U+000A LINE FEED (LF) character not preceded by a U+000D CARRIAGE RETURN
      # (CR) character, and a single U+000D CARRIAGE RETURN (CR) character not
      # followed by a U+000A LINE FEED (LF) character being the ways in which a
      # line can end.
      #
      skip_fragmented_line_feed

      while (parsed = next_line)
        line = parsed[0]
        @last_delimiter = parsed[1]
        process_line(line) { |type, data, id, reconnection_time| proc.call(type, data, id, reconnection_time) }
      end
      ##
      # Once the end of the file is reached, any pending data must be discarded.
      # (If the file ends in the middle of an event, before the final empty line,
      # the incomplete event is not dispatched.)
      #
    end

    def stream(&proc : String, String, String, Int32? ->)
      ->(chunk : String) do
        feed(chunk) { |type, data, id, reconnection_time| proc.call(type, data, id, reconnection_time) }
      end
    end

    private def process_line(line : String, &proc : String, String, String, Int32? ->)
      ##
      # Lines must be processed, in the order they are received, as follows:
      #
      case line
      ##
      # If the line is empty (a blank line)
      #
      when ""
        ##
        # Dispatch the event, as defined below.
        #
        dispatch_event(&proc)
      ##
      # If the line starts with a U+003A COLON character (:)
      #
      when /^:/
        ##
        # Ignore the line.
        #
        ignore
      ##
      # If the line contains a U+003A COLON character (:)
      #
      # Collect the characters on the line before the first U+003A COLON character
      # (:), and let field be that string.
      #
      # Collect the characters on the line after the first U+003A COLON character
      # (:), and let value be that string. If value starts with a U+0020 SPACE
      # character, remove it from value.
      #
      when /\A(?<field>[^:]+):\s?(?<value>.*)\z/
        ##
        # Process the field using the steps described below, using field as the
        # field name and value as the field value.
        #
        process_field($~["field"], $~["value"])
      ##
      # Otherwise, the string is not empty but does not contain a U+003A COLON
      # character (:)
      #
      else
        ##
        # Process the field using the steps described below, using the whole line
        # as the field name, and the empty string as the field value.
        #
        process_field(line, "")
      end
    end

    private def process_field(field : String, value : String)
      ##
      # The steps to process the field given a field name and a field value depend
      # on the field name, as given in the following list. Field names must be
      # compared literally, with no case folding performed.
      #
      case field
      ##
      # If the field name is "event"
      #
      when "event"
        ##
        # Set the event type buffer to field value.
        #
        @event_type_buffer = value
      ##
      # If the field name is "data"
      #
      when "data"
        ##
        # Append the field value to the data buffer, then append a single U+000A
        # LINE FEED (LF) character to the data buffer.
        #
        @data_buffer += value
        @data_buffer += "\n"
      ##
      # If the field name is "id"
      #
      when "id"
        ##
        # If the field value does not contain U+0000 NULL, then set the last event
        # ID buffer to the field value. Otherwise, ignore the field.
        #
        @last_event_id_buffer = value unless value.includes?("\u0000")
      ##
      # If the field name is "retry"
      #
      when "retry"
        ##
        # If the field value consists of only ASCII digits, then interpret the
        # field value as an integer in base ten, and set the event stream's
        # reconnection time to that integer. Otherwise, ignore the field.
        #
        @reconnection_time = value.to_i if value =~ /\A\d+\z/
      ##
      # Otherwise
      #
      else
        ##
        # The field is ignored.
        #
        ignore
      end
    end

    private def dispatch_event(&proc : String, String, String, Int32? ->)
      ##
      # When the user agent is required to dispatch the event, the user agent must
      # process the data buffer, the event type buffer, and the last event ID
      # buffer using steps appropriate for the user agent.
      #
      # For web browsers, the appropriate steps to dispatch the event are as
      # follows:
      #
      # 1. Set the last event ID string of the event source to the value of the
      #    last event ID buffer. The buffer does not get reset, so the last event
      #    ID string of the event source remains set to this value until the next
      #    time it is set by the server.
      #
      # Note: If an event doesn't have an "id" field, but an earlier event did set
      # the event source's last event ID string, then the event's lastEventId
      # field will be set to the value of whatever the last seen "id" field was.
      #
      id = @last_event_id_buffer
      ##
      # 2. If the data buffer is an empty string, set the data buffer and the
      #    event type buffer to the empty string and return.
      #
      if @data_buffer.empty?
        @data_buffer = ""
        @event_type_buffer = ""
        return
      end
      ##
      # 3. If the data buffer's last character is a U+000A LINE FEED (LF)
      #    character, then remove the last character from the data buffer.
      #
      @data_buffer = @data_buffer.chomp
      ##
      # 5. Initialize event's type attribute to "message", its data attribute to
      #    data, ...
      # 6. If the event type buffer has a value other than the empty string,
      #    change the type of the newly created event to equal the value of the
      #    event type buffer.
      #
      # For other user agents, the appropriate steps to dispatch the event are
      # implementation dependent, but at a minimum they must set the data and
      # event type buffers to the empty string before returning.
      #
      type = @event_type_buffer
      data = @data_buffer
      ##
      # 7. Set the data buffer and the event type buffer to the empty string.
      #
      @data_buffer = ""
      @event_type_buffer = ""

      proc.call(type, data, id, @reconnection_time)
    end

    private def skip_fragmented_line_feed
      return unless @last_delimiter == "\r"
      return unless @buffer.starts_with?('\n')

      @buffer = @buffer.byte_slice(1)
    end

    private def next_line : Tuple(String, String)?
      idx = 0

      while idx < @buffer.bytesize
        case @buffer.byte_at(idx)
        when '\n'.ord
          return extract_line(idx, 1, "\n")
        when '\r'.ord
          if idx + 1 < @buffer.bytesize && @buffer.byte_at(idx + 1) == '\n'.ord
            return extract_line(idx, 2, "\r\n")
          end

          return extract_line(idx, 1, "\r")
        end

        idx += 1
      end

      nil
    end

    private def extract_line(line_end : Int32, delimiter_size : Int32, delimiter : String) : Tuple(String, String)
      line = @buffer.byte_slice(0, line_end)
      @buffer = @buffer.byte_slice(line_end + delimiter_size)
      {line, delimiter}
    end

    def ignore; end
  end
end
