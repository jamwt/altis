Altis is a Redis clone in Haskell.

So far it's somewhat slower (2x?) and it uses substantially more memory.

Woot.

The stretch goals are to implement some experiments to improve
scalability and robustness characteristics under 24/7 load; winning
the pure per-core speed or the memory-light contest against C-redis isn't
likely to happen.
