# `tqsdk-relay`

`tqsdk-relay` is an optional market relay and cache service for `tqsdk-rust`.

It is not part of the default SDK path. Existing SDK crates continue to connect
directly to Tianqin unless users explicitly configure their market endpoint to a
relay instance.

V1 scope:

- market route only
- futures tick upstream first
- quote / tick / K-line fan-out
- in-memory cache first
- optional disk cache later in the relay crate
- no trade / query / auth proxy
