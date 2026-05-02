# event_stream_parser

[![CI](https://github.com/Shopify/event_stream_parser/actions/workflows/ci.yml/badge.svg)](https://github.com/Shopify/event_stream_parser/actions/workflows/ci.yml)

A lightweight, fully spec-compliant parser for the
[event stream](https://www.w3.org/TR/eventsource/) format.

It only deals with the parsing of events and not any of the client/transport
aspects. This is not a Server-sent Events (SSE) client.

Under the hood, it's a stateful parser that receives chunks (that are received
from an HTTP client, for example) and emits events as it parses them. But it
remembers the last event id and reconnection time and keeps emitting them as
long as they are not overwritten by new ones.

BOM stripping is left as a responsibility of the chunk provider.

## Installation

Add this shard to your application's `shard.yml`:

```yaml
dependencies:
  event_stream_parser:
    github: Shopify/event_stream_parser
```

Then run:

```sh
shards install
```

## Usage

Create a new Parser:

```cr
require "event_stream_parser"

parser = EventStreamParser::Parser.new
```

Then, feed it chunks as they come in:

```cr
do_something_that_yields_chunks do |chunk|
  parser.feed(chunk) do |type, data, id, reconnection_time|
    puts "Event type: #{type}"
    puts "Event data: #{data}"
    puts "Event id: #{id}"
    puts "Reconnection time: #{reconnection_time}"
  end
end
```

Or use the `stream` method to generate a proc that you can pass to a chunk
producer:

```cr
parser_stream = parser.stream do |type, data, id, reconnection_time|
  puts "Event type: #{type}"
  puts "Event data: #{data}"
  puts "Event id: #{id}"
  puts "Reconnection time: #{reconnection_time}"
end

do_something_that_yields_chunks(&parser_stream)
```

## Development

After checking out the repo:

1. Run `shards install` to install dependencies.
2. Run `crystal spec` to run the Spectator suite.
3. Run `crystal build src/event_stream_parser.cr` to check the shard builds.

## Contributing

Bug reports and pull requests are welcome on GitHub at
https://github.com/Shopify/event_stream_parser. This project is intended to be a
safe, welcoming space for collaboration, and contributors are expected to adhere
to the [Contributor Covenant](http://contributor-covenant.org) code of conduct. Read more about contributing [here](https://github.com/Shopify/event_stream_parser/blob/main/CONTRIBUTING.md).

## License

The shard is available as open source under the terms of the
[MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in this repository is expected to follow the
[Code of Conduct](https://github.com/Shopify/event_stream_parser/blob/main/CODE_OF_CONDUCT.md).
