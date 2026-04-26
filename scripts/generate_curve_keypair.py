#!/usr/bin/env python
"""
scripts/generate_curve_keypair.py
──────────────────────────────────
One-shot helper that generates a ZeroMQ CURVE **client** key-pair and
prints the values ready to paste into your .env file.

Run this ONCE, then store the result in .env.
Do NOT re-run on every startup — the keys must stay stable across restarts.

Usage
-----
    python scripts/generate_curve_keypair.py

Output (example)
----------------
    ZMQ_CURVE_CLIENT_PUBLIC_KEY=rq:rM>}U?@Lns47E1%kR.o@n%FcmmsL/@{H8]yf7
    ZMQ_CURVE_CLIENT_SECRET_KEY=JTKVSB%%)wK0E.X)V>+}o?pNmC{O&4W4b!Ni{Lh6

Next steps
----------
  1. Paste the two lines above into your .env file.
  2. Ask the broker operator for the broker's public key and set:
         ZMQ_CURVE_SERVER_PUBLIC_KEY=<broker-public-key>
  3. (Optional) Share YOUR public key (ZMQ_CURVE_CLIENT_PUBLIC_KEY) with the
     broker operator if their server enforces client allowlisting.
"""

import zmq


def main() -> None:
  public_key, secret_key = zmq.curve_keypair()
  # curve_keypair() returns bytes; decode to ASCII Z85 strings
  pub_z85 = public_key.decode("ascii")
  sec_z85 = secret_key.decode("ascii")

  print("# -- ZeroMQ CURVE CLIENT keypair--")
  print(f'ZMQ_CURVE_CLIENT_PUBLIC_KEY="{pub_z85}"')
  print(f'ZMQ_CURVE_CLIENT_SECRET_KEY="{sec_z85}"')


if __name__ == "__main__":
  main()
