"""Settings shared by the simulation, the WebSocket server, the dashboards and the tests.

The addresses are odctl's services as seen from the host. Every table lives in the
`dashboard` schema, so nothing else in the database is touched.
"""

DSN = "postgresql://user:password@127.0.0.1:5432/odctl"
SCHEMA = "dashboard"
PARAMS_TABLE = f"{SCHEMA}.params"  # desflow's ingress reads live changes from here

API_HOST, API_PORT = "127.0.0.1", 8000
WS_URL = f"ws://{API_HOST}:{API_PORT}/ws"
LOOKBACK_MINUTES = 5  # the server sends the order items of this window
REFRESH_SECONDS = 5  # and sends them again this often

# The simulation's parameters. Each is in desflow's registry, so it can be changed
# while the simulation runs: see "Change the simulation while it runs" in the README.
SIM_ID = "sales"
ARRIVALS = {"visitor": 4.0}  # mean visitors a second, exponential
SERVICES = {  # mean and standard deviation in seconds, lognormal
    "page_view": (4.0, 2.0),  # time on a page before the next one
    "pick": (5.0, 2.0),  # a picker packs an order
    "transit": (60.0, 20.0),  # a shipped order reaches the customer
    "return_after": (60.0, 30.0),  # a returned order comes back after delivery
    "patience": (120.0, 60.0),  # an order's wait for a picker before it is cancelled
}
PICKERS, MAX_PICKERS = 10, 20  # warehouse pickers packing orders at once
CHANCES = {
    "returning": 0.6,  # a visitor is a registered user
    "buy": 0.3,  # a visitor buys at the end of the visit
    "return": 0.1,  # a delivered order is returned
}
