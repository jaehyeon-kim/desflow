"""Changes a parameter of the running simulation.

It adds the change to the parameter table, where desflow's PostgreSQL ingress picks
it up within two seconds and updates the registry.

Run: python -m sales.simulation.control sales.arrival.visitor.rate 8
"""

import argparse
import asyncio

from sales.stores import postgres

PARAMETERS = {
    "sales.arrival.visitor.rate": "visitors a second (default 4)",
    "sales.resources.pickers.current_cap": "warehouse pickers, up to 20 (default 10)",
    "sales.service.pick.mean": "mean seconds to pack an order (default 5)",
    "sales.service.patience.mean": "mean seconds an order waits for a picker (default 120)",
    "sales.variables.buy": "chance a visitor buys (default 0.3)",
    "sales.variables.return": "chance a delivered order is returned (default 0.1)",
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="parameters:\n"
        + "\n".join(f"  {p}: {d}" for p, d in PARAMETERS.items()),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "path", choices=list(PARAMETERS), help="the parameter to change"
    )
    parser.add_argument("value", type=float, help="its new value")
    args = parser.parse_args()
    asyncio.run(postgres.send_change(args.path, args.value))
    print(f"Sent {args.path} = {args.value}")
