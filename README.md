- Output can be either sqlite or csv (for version 0.0.1)
- Primarily just a utility for something that I'm working on
- Should not be very complex. Keep it simple, just do what it needs to do
- Does not keep the timestamps
- Potentially give the option to not write the output, just keep in memory

## Exception Cases

When you try to pull data from csv before the time something was listed, it returns just empty
data. So figure out some way to handle this. Ideally this would just start at the earliest data,
but it's a bit more difficult to figure that out with python than you would imagine.

***IMPORTANT***: Try using `exchange.markets[symbol]` to see if that information is in the market
info for that particular symbol.
