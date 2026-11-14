# Gas Station Refueling

Cars take fuel from a station's tank, and a control process calls a tank truck when the tank runs low. This is SimPy's Gas Station Refueling example, written in desflow.

---

## Before You Start

SimPy's gas station has two pumps and a tank of 200 litres. A car arrives every 30 to 300 seconds with 5 to 25 litres left in its 50 litre tank. It waits for a pump, takes the fuel it needs from the station's tank, and refuels at 2 litres a second. A control process checks the tank every 10 seconds. When the tank is below 25 percent, it calls a tank truck, which arrives 300 seconds later and fills the tank. The run lasts 1000 seconds.

The example needs only desflow, which each Quick Start in [Run It](#run-it) installs, and no other service. `desflow examples get simpy-gas-station-refuel --api yaml` downloads it, as [Get an Example](../../cli/index.md#get-an-example) shows. [SimPy Examples](index.md) says how these pages read SimPy's times and numbers. SimPy's own page for the example is [on its documentation site](https://simpy.readthedocs.io/en/4.1.2/examples/gas_station_refuel.html).

---

## Steps

### Car Refuels

SimPy's `car` requests a pump, takes the fuel it needs from the tank, and refuels at 2 litres a second.

```python title="docs/snippets/simpy/gas_station_refuel.py:car-refuels"
def car(name, env, gas_station, station_tank):
    """A car arrives at the gas station for refueling.

    It requests one of the gas station's fuel pumps and tries to get the
    desired amount of fuel from it. If the station's fuel tank is
    depleted, the car has to wait for the tank truck to arrive.

    """
    car_tank_level = random.randint(*CAR_TANK_LEVEL)
    print(f'{env.now:6.1f} s: {name} arrived at gas station')
    with gas_station.request() as req:
        # Request one of the gas pumps
        yield req

        # Get the required amount of fuel
        fuel_required = CAR_TANK_SIZE - car_tank_level
        yield station_tank.get(fuel_required)

        # The "actual" refueling process takes some time
        yield env.timeout(fuel_required / REFUELING_SPEED)

        print(f'{env.now:6.1f} s: {name} refueled with {fuel_required:.1f}L')
```

=== "YAML"

    `pumps` is a resource with two units and `tank` is a container of 200 litres, full at the start. The `car` task takes a pump, gets `50 - attr.level` litres from the tank, and refuels for `(50 - attr.level) / 2` seconds. `attr.level` is the fuel the car has left.

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:pumps-and-tank"
    resources:
      pumps: {current_cap: 2}

    containers:
      tank: {current_cap: 200, level: 200}
    ```

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:car-refuels"
    car:
      resource: pumps
      get: {container: tank, amount: "50 - attr.level"}
      service_time: "(50 - attr.level) / 2"
      payload:
        event_type: refueled
        litres: {gen: expr, expr: "50 - attr.level"}
    ```

=== "Builder"

    `add_resource` and `add_container` declare the pumps and the tank. The `car` task takes `get` and `service_time` as expressions on the car's attribute.

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:pumps-and-tank"
    app.add_resource("pumps", current_cap=2)
    app.add_container("tank", current_cap=200, level=200)
    ```

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:car-refuels"
    car_payload = Template(
        {
            "event_type": "refueled",
            "litres": {"gen": "expr", "expr": "50 - attr.level"},
        },
        name="car",
    )

    @app.task(
        name="car",
        resource_id="pumps",
        get={"container": "tank", "amount": "50 - attr.level"},
        service_time="(50 - attr.level) / 2",
    )
    def car(task_id):
        """Refuels at a pump."""
        return car_payload
    ```

=== "Low-level"

    The low-level version has SimPy's `simpy.Resource` and `simpy.Container`, and SimPy's `car` function with records published for it.

    ```python title="examples/low-level/simpy/gas_station_refuel_example.py:pumps-and-tank"
    gas_station = simpy.Resource(env, 2)
    station_tank = simpy.Container(env, STATION_TANK_SIZE, init=STATION_TANK_SIZE)
    ```

    ```python title="examples/low-level/simpy/gas_station_refuel_example.py:car-refuels"
    def car(name, car_tank_level):
        number = next(numbers["car"])
        with gas_station.request() as req:
            publish(name, "car", number, {"status": "queued"})
            yield req
            fuel_required = CAR_TANK_SIZE - car_tank_level
            yield station_tank.get(fuel_required)
            publish(name, "car", number, {"status": "started"})
            yield env.timeout(fuel_required / REFUELING_SPEED)
            publish(
                name, "car", number, {"event_type": "refueled", "litres": fuel_required}
            )
    ```

### Control Calls the Truck

SimPy's `gas_station_control` checks the tank every 10 seconds and, below 25 percent, calls the truck and waits for it.

```python title="docs/snippets/simpy/gas_station_refuel.py:control-calls-truck"
def gas_station_control(env, station_tank):
    """Periodically check the level of the gas station tank and call the tank
    truck if the level falls below a threshold."""
    while True:
        if station_tank.level / station_tank.capacity * 100 < THRESHOLD:
            # We need to call the tank truck now!
            print(f'{env.now:6.1f} s: Calling tank truck')
            # Wait for the tank truck to arrive and refuel the station tank
            yield env.process(tank_truck(env, station_tank))

        yield env.timeout(10)  # Check every 10 seconds
```

=== "YAML"

    The `control` task checks `level.tank / 200 * 100 < 25` in its `next` rule. When the tank is low it goes to `call_truck`, and then to `await_truck`, which waits for the `truck` task to finish. Otherwise it goes to `pause`, which waits 10 seconds and returns to `control`.

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:control-calls-truck"
    control:
      payload: {event_type: checked}
      next:
        when:
          - {condition: "level.tank / 200 * 100 < 25", task: call_truck}
        default: pause
    call_truck:
      payload: {event_type: truck_called}
      next: await_truck
    await_truck:
      wait: {task: truck}
      payload: {event_type: truck_returned}
      next: pause
    ```

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:pause"
    pause:
      service_time: 10
      payload: {event_type: waited}
      next: control
    ```

=== "Builder"

    The same tasks, with `next` and `wait` as in the YAML.

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:control-calls-truck"
    @app.task(
        name="control",
        next={
            "when": [
                {"condition": "level.tank / 200 * 100 < 25", "task": "call_truck"}
            ],
            "default": "pause",
        },
    )
    def control(task_id):
        """Checks the level of the tank."""
        return {"event_type": "checked"}

    @app.task(name="call_truck", next="await_truck")
    def call_truck(task_id):
        """Calls the tank truck."""
        return {"event_type": "truck_called"}

    @app.task(name="await_truck", wait={"task": "truck"}, next="pause")
    def await_truck(task_id):
        """Waits for the tank truck."""
        return {"event_type": "truck_returned"}
    ```

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:pause"
    @app.task(name="pause", service_time=10, next="control")
    def pause(task_id):
        """Waits until the next check."""
        return {"event_type": "waited"}
    ```

=== "Low-level"

    `gas_station_control` is SimPy's loop. It runs `tank_truck` as a process and waits for it.

    ```python title="examples/low-level/simpy/gas_station_refuel_example.py:control-calls-truck"
    def gas_station_control(name):
        while True:
            publish(
                name, "control", next(numbers["control"]), {"event_type": "checked"}
            )
            if station_tank.level / station_tank.capacity * 100 < THRESHOLD:
                call = next(numbers["call_truck"])
                publish(name, "call_truck", call, {"event_type": "truck_called"})
                waiting = next(numbers["await_truck"])
                publish(name, "await_truck", waiting, {"status": "queued"})
                yield env.process(tank_truck(name))
                publish(name, "await_truck", waiting, {"status": "started"})
                publish(name, "await_truck", waiting, {"event_type": "truck_returned"})
            pause = next(numbers["pause"])
            publish(name, "pause", pause, {"status": "started"})
            yield env.timeout(10)
            publish(name, "pause", pause, {"event_type": "waited"})
    ```

### Truck Fills the Tank

SimPy's `tank_truck` waits 300 seconds and then fills the tank.

```python title="docs/snippets/simpy/gas_station_refuel.py:truck-fills-tank"
def tank_truck(env, station_tank):
    """Arrives at the gas station after a certain delay and refuels it."""
    yield env.timeout(TANK_TRUCK_TIME)
    amount = station_tank.capacity - station_tank.level
    station_tank.put(amount)
    print(
        f'{env.now:6.1f} s: Tank truck arrived and refuelled station with {amount:.1f}L'
    )
```

=== "YAML"

    The `truck` task waits `service_time: 300` and then puts `200 - level.tank` litres into the container, which fills it.

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:truck-fills-tank"
    truck:
      service_time: 300
      put: {container: tank, amount: "200 - level.tank"}
      payload:
        event_type: truck_arrived
        litres: {gen: expr, expr: "200 - level.tank"}
    ```

=== "Builder"

    The `truck` task takes `service_time` and `put` as the YAML does.

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:truck-fills-tank"
    truck_payload = Template(
        {
            "event_type": "truck_arrived",
            "litres": {"gen": "expr", "expr": "200 - level.tank"},
        },
        name="truck",
    )

    @app.task(
        name="truck",
        service_time=300,
        put={"container": "tank", "amount": "200 - level.tank"},
    )
    def truck(task_id):
        """Fills the tank on arrival."""
        return truck_payload
    ```

=== "Low-level"

    `tank_truck` is SimPy's function.

    ```python title="examples/low-level/simpy/gas_station_refuel_example.py:truck-fills-tank"
    def tank_truck(name):
        number = next(numbers["truck"])
        publish(name, "truck", number, {"status": "started"})
        yield env.timeout(TANK_TRUCK_TIME)
        amount = station_tank.capacity - station_tank.level
        yield station_tank.put(amount)
        publish(
            name, "truck", number, {"event_type": "truck_arrived", "litres": amount}
        )
    ```

### Cars Arrive

SimPy's `car_generator` starts a car after each gap of 30 to 300 seconds, each with its own fuel level.

```python title="docs/snippets/simpy/gas_station_refuel.py:cars-arrive"
def car_generator(env, gas_station, station_tank):
    """Generate new cars that arrive at the gas station."""
    for i in itertools.count():
        yield env.timeout(random.randint(*T_INTER))
        env.process(car(f'Car {i}', env, gas_station, station_tank))
```

=== "YAML"

    The arrival `cars` draws its `gap` from 30 to 300 seconds and gives each car an attribute `level` from 5 to 25 litres. The arrival `control` starts the control process once at time 0.

    ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml:cars-arrive"
    arrivals:
      control: {initial: 1, spawn: control}
      cars:
        gap: {gen: integer, bounds: [30, 300]}
        spawn: car
        key: {gen: sequence, format: "Car {}"}
        attributes:
          level: {gen: integer, bounds: [5, 25]}
    ```

=== "Builder"

    `add_arrival` takes `gap` and `attributes`, and `add_initial_arrival` starts the control process.

    ```python title="examples/builder/simpy/gas_station_refuel_example.py:cars-arrive"
    app.add_initial_arrival("control", 1, spawn="control")
    app.add_arrival(
        "cars",
        gap=generator({"gen": "integer", "bounds": [30, 300]}),
        key=lambda n: "Car {}".format(n - 1),
        attributes={"level": generator({"gen": "integer", "bounds": [5, 25]})},
        spawn="car",
    )
    ```

=== "Low-level"

    `car_generator` draws the gap and the fuel level from desflow's `Sampler`.

    ```python title="examples/low-level/simpy/gas_station_refuel_example.py:cars-arrive"
    def car_generator():
        for i in itertools.count():
            yield env.timeout(int(gaps.integers(30, 301)))
            env.process(car(f"Car {i}", int(levels.integers(5, 26))))

    env.process(gas_station_control("control-1"))
    env.process(car_generator())
    ```

### Run It

=== "YAML"

    `desflow run` loads the blueprint and runs it unpaced, because it sets `factor: 0`. Each record is printed to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api yaml
        cd simpy-gas-station-refuel/yaml

        # 3. Run it
        desflow run gas_station_refuel.yaml
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api yaml
        cd simpy-gas-station-refuel/yaml

        # 3. Run it
        desflow run gas_station_refuel.yaml
        ```

=== "Builder"

    The script builds the model with the builder API, runs it unpaced, and prints each record to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api builder
        cd simpy-gas-station-refuel/builder

        # 3. Run it
        uv run --no-project --with desflow gas_station_refuel_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api builder
        cd simpy-gas-station-refuel/builder

        # 3. Run it
        python gas_station_refuel_example.py
        ```

=== "Low-level"

    The script wires the model by hand on `DynamicRealtimeEnvironment`, runs it unpaced, and prints each record to the terminal.

    **Quick Start**

    `desflow examples get` downloads the example at the release you install, into a folder of its own. Pick uv or pip:

    === "uv"

        ```bash
        # 1. Install
        uv tool install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api low-level
        cd simpy-gas-station-refuel/low-level

        # 3. Run it
        uv run --no-project --with desflow gas_station_refuel_example.py
        ```

    === "pip"

        ```bash
        # 1. Install
        pip install desflow

        # 2. Download the example
        desflow examples get simpy-gas-station-refuel --api low-level
        cd simpy-gas-station-refuel/low-level

        # 3. Run it
        python gas_station_refuel_example.py
        ```

---

## Check the Result

Five cars refuel in the 1000 seconds. The control task finds the tank below 25 percent at 820 seconds and calls the truck. The truck needs 300 seconds, so it would arrive at 1120 seconds, after the run ends, and the records show no truck arrival.

In SimPy's output, below, the truck is called at 460 seconds and arrives at 760 seconds. Here the cars arrive at other times, so the tank is low later. desflow draws each random value from its own stream, named by the place where the value is used, such as `arrivals.cars.gap`. A seed of 42 therefore gives different numbers from SimPy's `random.seed(42)`. The system is the same. The figures are not.

```text title="docs/snippets/simpy/gas_station_refuel.out"
Gas Station refuelling
  87.0 s: Car 0 arrived at gas station
 105.5 s: Car 0 refueled with 37.0L
 129.0 s: Car 1 arrived at gas station
 148.0 s: Car 1 refueled with 38.0L
 284.0 s: Car 2 arrived at gas station
 305.0 s: Car 2 refueled with 42.0L
 385.0 s: Car 3 arrived at gas station
 398.5 s: Car 3 refueled with 27.0L
 459.0 s: Car 4 arrived at gas station
 460.0 s: Calling tank truck
 481.0 s: Car 4 refueled with 44.0L
 705.0 s: Car 5 arrived at gas station
 750.0 s: Car 6 arrived at gas station
 760.0 s: Tank truck arrived and refuelled station with 188.0L
 779.0 s: Car 6 refueled with 38.0L
 781.5 s: Car 5 refueled with 43.0L
 891.0 s: Car 7 arrived at gas station
 904.0 s: Car 7 refueled with 26.0L
```

The three versions in desflow print the same records, which [a test](https://github.com/jaehyeon-kim/desflow/blob/main/tests/docs/test_example_twins.py) checks by running each version unpaced. Each record has `sim_ts`, the simulation time, and a key for the entity it belongs to. The statistics and the parameters that a live run can change are in the same stream, which SimPy's file does not have.

---

## Full Files

??? example "Full files"

    === "YAML"

        The blueprint is `gas_station_refuel.yaml`.

        ```yaml title="examples/yaml/simpy/gas_station_refuel.yaml"
        # Gas Station Refueling: SimPy's gas_station_refuel.py example, in YAML.
        #
        # The YAML version of gas_station_refuel_example.py, which --api builder downloads. A
        # gas station has two pumps and a tank of 200 litres. A car arrives every 30 to 300
        # seconds with 5 to 25 litres left in its 50 litre tank. It waits for a pump, takes
        # the fuel it needs from the station's tank and refuels at 2 litres a second. A control
        # task checks the tank every 10 seconds, and when it is below 25 percent it calls a
        # tank truck, which arrives 300 seconds later and fills the tank.
        #
        # Each value is drawn from desflow's random sequence for its place in the file, so the
        # numbers differ from those SimPy prints.
        #
        # Needs no containers. Runs unpaced and ends after 1000 simulation seconds.
        # Run it from its folder with: desflow run gas_station_refuel.yaml

        simulation: {sim_id: GasStation, factor: 0, random_seed: 42}

        egress:
          - {type: Console}

        resources:
          pumps: {current_cap: 2}

        containers:
          tank: {current_cap: 200, level: 200}

        arrivals:
          control: {initial: 1, spawn: control}
          cars:
            gap: {gen: integer, bounds: [30, 300]}
            spawn: car
            key: {gen: sequence, format: "Car {}"}
            attributes:
              level: {gen: integer, bounds: [5, 25]}

        tasks:
          car:
            resource: pumps
            get: {container: tank, amount: "50 - attr.level"}
            service_time: "(50 - attr.level) / 2"
            payload:
              event_type: refueled
              litres: {gen: expr, expr: "50 - attr.level"}
          control:
            payload: {event_type: checked}
            next:
              when:
                - {condition: "level.tank / 200 * 100 < 25", task: call_truck}
              default: pause
          call_truck:
            payload: {event_type: truck_called}
            next: await_truck
          await_truck:
            wait: {task: truck}
            payload: {event_type: truck_returned}
            next: pause
          truck:
            service_time: 300
            put: {container: tank, amount: "200 - level.tank"}
            payload:
              event_type: truck_arrived
              litres: {gen: expr, expr: "200 - level.tank"}
          pause:
            service_time: 10
            payload: {event_type: waited}
            next: control

        run: {until: 1000}
        ```

    === "Builder"

        The script builds the model with the builder API.

        ```python title="examples/builder/simpy/gas_station_refuel_example.py"
        """Gas Station Refueling: SimPy's gas_station_refuel.py example, builder API.

        A gas station has two pumps and a tank of 200 litres. A car arrives every 30 to 300
        seconds with 5 to 25 litres left in its 50 litre tank. It waits for a pump, takes the
        fuel it needs from the station's tank and refuels at 2 litres a second. A control task
        checks the tank every 10 seconds, and when it is below 25 percent it calls a tank
        truck, which arrives 300 seconds later and fills the tank.

        Each value is drawn from desflow's random sequence for its place in the model, so the
        numbers differ from those SimPy prints.

        Needs no containers. Runs unpaced and ends after 1000 simulation seconds.
        """

        import logging

        from desflow import ConsoleEgress, SimulationContext
        from desflow.values import Template, generator


        def build() -> SimulationContext:
            """Builds the gas station."""
            app = SimulationContext(sim_id="GasStation", factor=0, random_seed=42)
            app.add_egress(ConsoleEgress())
            app.add_resource("pumps", current_cap=2)
            app.add_container("tank", current_cap=200, level=200)
            app.add_initial_arrival("control", 1, spawn="control")
            app.add_arrival(
                "cars",
                gap=generator({"gen": "integer", "bounds": [30, 300]}),
                key=lambda n: "Car {}".format(n - 1),
                attributes={"level": generator({"gen": "integer", "bounds": [5, 25]})},
                spawn="car",
            )
            car_payload = Template(
                {
                    "event_type": "refueled",
                    "litres": {"gen": "expr", "expr": "50 - attr.level"},
                },
                name="car",
            )

            @app.task(
                name="car",
                resource_id="pumps",
                get={"container": "tank", "amount": "50 - attr.level"},
                service_time="(50 - attr.level) / 2",
            )
            def car(task_id):
                """Refuels at a pump."""
                return car_payload


            @app.task(
                name="control",
                next={
                    "when": [
                        {"condition": "level.tank / 200 * 100 < 25", "task": "call_truck"}
                    ],
                    "default": "pause",
                },
            )
            def control(task_id):
                """Checks the level of the tank."""
                return {"event_type": "checked"}

            @app.task(name="call_truck", next="await_truck")
            def call_truck(task_id):
                """Calls the tank truck."""
                return {"event_type": "truck_called"}

            @app.task(name="await_truck", wait={"task": "truck"}, next="pause")
            def await_truck(task_id):
                """Waits for the tank truck."""
                return {"event_type": "truck_returned"}


            truck_payload = Template(
                {
                    "event_type": "truck_arrived",
                    "litres": {"gen": "expr", "expr": "200 - level.tank"},
                },
                name="truck",
            )

            @app.task(
                name="truck",
                service_time=300,
                put={"container": "tank", "amount": "200 - level.tank"},
            )
            def truck(task_id):
                """Fills the tank on arrival."""
                return truck_payload


            @app.task(name="pause", service_time=10, next="control")
            def pause(task_id):
                """Waits until the next check."""
                return {"event_type": "waited"}


            return app


        app = build()


        def run(model: SimulationContext = app) -> None:
            """Runs 1000 simulation seconds."""
            model.run(until=1000)


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

    === "Low-level"

        The script wires the model by hand on `DynamicRealtimeEnvironment`, with SimPy's own processes.

        ```python title="examples/low-level/simpy/gas_station_refuel_example.py"
        """Gas Station Refueling: SimPy's gas_station_refuel.py example, low-level API.

        The low-level version of `builder/simpy/gas_station_refuel_example.py`: SimPy's own
        processes on a `DynamicRealtimeEnvironment`. They publish with `publish_event` the
        records that the YAML and builder versions publish: each task's `queued` and
        `started`, its own events, and the number of the task's instance. A gas station has
        two pumps and a tank of 200 litres. A car arrives every 30 to 300 seconds with 5 to
        25 litres left in its 50 litre tank. It waits for a pump, takes the fuel it needs
        from the station's tank and refuels at 2 litres a second. A control process checks
        the tank every 10 seconds, and when it is below 25 percent it calls a tank truck,
        which arrives 300 seconds later and fills the tank.

        Each value is drawn from desflow's `Sampler`, from the random sequence the YAML and
        builder versions use for the same place, so the three versions publish the same
        records.

        Needs no containers. Runs unpaced and ends after 1000 simulation seconds.
        """

        import itertools
        import logging

        import simpy

        from desflow import ConsoleEgress, DynamicRealtimeEnvironment, Sampler

        STATION_TANK_SIZE = 200  # litres
        THRESHOLD = 25  # percent of the tank that calls the truck
        CAR_TANK_SIZE = 50  # litres
        REFUELING_SPEED = 2  # litres a second
        TANK_TRUCK_TIME = 300  # seconds
        UNTIL = 1000


        def build(env: DynamicRealtimeEnvironment) -> None:
            """Starts the control process and the cars on `env`."""
            sampler = Sampler(seed=42)
            gaps = sampler.generator("arrivals.cars.gap")
            levels = sampler.generator("arrivals.cars.attributes.level")
            gas_station = simpy.Resource(env, 2)
            station_tank = simpy.Container(env, STATION_TANK_SIZE, init=STATION_TANK_SIZE)
            numbers = {
                task: itertools.count()
                for task in ("car", "control", "call_truck", "await_truck", "truck", "pause")
            }

            def publish(name, task, number, value):
                env.publish_event(name, value, task=task, instance=number)

            def car(name, car_tank_level):
                number = next(numbers["car"])
                with gas_station.request() as req:
                    publish(name, "car", number, {"status": "queued"})
                    yield req
                    fuel_required = CAR_TANK_SIZE - car_tank_level
                    yield station_tank.get(fuel_required)
                    publish(name, "car", number, {"status": "started"})
                    yield env.timeout(fuel_required / REFUELING_SPEED)
                    publish(
                        name, "car", number, {"event_type": "refueled", "litres": fuel_required}
                    )


            def tank_truck(name):
                number = next(numbers["truck"])
                publish(name, "truck", number, {"status": "started"})
                yield env.timeout(TANK_TRUCK_TIME)
                amount = station_tank.capacity - station_tank.level
                yield station_tank.put(amount)
                publish(
                    name, "truck", number, {"event_type": "truck_arrived", "litres": amount}
                )


            def gas_station_control(name):
                while True:
                    publish(
                        name, "control", next(numbers["control"]), {"event_type": "checked"}
                    )
                    if station_tank.level / station_tank.capacity * 100 < THRESHOLD:
                        call = next(numbers["call_truck"])
                        publish(name, "call_truck", call, {"event_type": "truck_called"})
                        waiting = next(numbers["await_truck"])
                        publish(name, "await_truck", waiting, {"status": "queued"})
                        yield env.process(tank_truck(name))
                        publish(name, "await_truck", waiting, {"status": "started"})
                        publish(name, "await_truck", waiting, {"event_type": "truck_returned"})
                    pause = next(numbers["pause"])
                    publish(name, "pause", pause, {"status": "started"})
                    yield env.timeout(10)
                    publish(name, "pause", pause, {"event_type": "waited"})


            def car_generator():
                for i in itertools.count():
                    yield env.timeout(int(gaps.integers(30, 301)))
                    env.process(car(f"Car {i}", int(levels.integers(5, 26))))

            env.process(gas_station_control("control-1"))
            env.process(car_generator())


        def run() -> None:
            """Runs 1000 simulation seconds."""
            env = DynamicRealtimeEnvironment(factor=0)
            env.setup_egress([ConsoleEgress()])
            build(env)
            try:
                env.run(until=UNTIL)
            finally:
                env.teardown()


        if __name__ == "__main__":
            logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
            run()
        ```

    === "SimPy"

        This is SimPy 4.1.2's `gas_station_refuel.py`, with SimPy's licence notice added at the top and comment lines that mark the pieces shown above. Run with Python, it prints the output shown under Check the Result.

        ```python title="docs/snippets/simpy/gas_station_refuel.py"
        # SimPy 4.1.2, docs/examples/code/gas_station_refuel.py, under SimPy's licence (LICENSE.rst):
        #
        # The MIT License (MIT)
        #
        # Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke (also see AUTHORS.txt)
        #
        # Permission is hereby granted, free of charge, to any person obtaining a copy of
        # this software and associated documentation files (the "Software"), to deal in
        # the Software without restriction, including without limitation the rights to
        # use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
        # the Software, and to permit persons to whom the Software is furnished to do so,
        # subject to the following conditions:
        #
        # The above copyright notice and this permission notice shall be included in all
        # copies or substantial portions of the Software.
        #
        # THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
        # IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
        # FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
        # COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
        # IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
        # CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

        """
        Gas Station Refueling example

        Covers:

        - Resources: Resource
        - Resources: Container
        - Waiting for other processes

        Scenario:
          A gas station has a limited number of gas pumps that share a common
          fuel reservoir. Cars randomly arrive at the gas station, request one
          of the fuel pumps and start refueling from that reservoir.

          A gas station control process observes the gas station's fuel level
          and calls a tank truck for refueling if the station's level drops
          below a threshold.

        """

        import itertools
        import random

        import simpy

        # fmt: off
        RANDOM_SEED = 42
        STATION_TANK_SIZE = 200    # Size of the gas station tank (liters)
        THRESHOLD = 25             # Station tank minimum level (% of full)
        CAR_TANK_SIZE = 50         # Size of car fuel tanks (liters)
        CAR_TANK_LEVEL = [5, 25]   # Min/max levels of car fuel tanks (liters)
        REFUELING_SPEED = 2        # Rate of refuelling car fuel tank (liters / second)
        TANK_TRUCK_TIME = 300      # Time it takes tank truck to arrive (seconds)
        T_INTER = [30, 300]        # Interval between car arrivals [min, max] (seconds)
        SIM_TIME = 1000            # Simulation time (seconds)
        # fmt: on


        def car(name, env, gas_station, station_tank):
            """A car arrives at the gas station for refueling.

            It requests one of the gas station's fuel pumps and tries to get the
            desired amount of fuel from it. If the station's fuel tank is
            depleted, the car has to wait for the tank truck to arrive.

            """
            car_tank_level = random.randint(*CAR_TANK_LEVEL)
            print(f'{env.now:6.1f} s: {name} arrived at gas station')
            with gas_station.request() as req:
                # Request one of the gas pumps
                yield req

                # Get the required amount of fuel
                fuel_required = CAR_TANK_SIZE - car_tank_level
                yield station_tank.get(fuel_required)

                # The "actual" refueling process takes some time
                yield env.timeout(fuel_required / REFUELING_SPEED)

                print(f'{env.now:6.1f} s: {name} refueled with {fuel_required:.1f}L')


        def gas_station_control(env, station_tank):
            """Periodically check the level of the gas station tank and call the tank
            truck if the level falls below a threshold."""
            while True:
                if station_tank.level / station_tank.capacity * 100 < THRESHOLD:
                    # We need to call the tank truck now!
                    print(f'{env.now:6.1f} s: Calling tank truck')
                    # Wait for the tank truck to arrive and refuel the station tank
                    yield env.process(tank_truck(env, station_tank))

                yield env.timeout(10)  # Check every 10 seconds


        def tank_truck(env, station_tank):
            """Arrives at the gas station after a certain delay and refuels it."""
            yield env.timeout(TANK_TRUCK_TIME)
            amount = station_tank.capacity - station_tank.level
            station_tank.put(amount)
            print(
                f'{env.now:6.1f} s: Tank truck arrived and refuelled station with {amount:.1f}L'
            )


        def car_generator(env, gas_station, station_tank):
            """Generate new cars that arrive at the gas station."""
            for i in itertools.count():
                yield env.timeout(random.randint(*T_INTER))
                env.process(car(f'Car {i}', env, gas_station, station_tank))


        # Setup and start the simulation
        print('Gas Station refuelling')
        random.seed(RANDOM_SEED)

        # Create environment and start processes
        env = simpy.Environment()
        gas_station = simpy.Resource(env, 2)
        station_tank = simpy.Container(env, STATION_TANK_SIZE, init=STATION_TANK_SIZE)
        env.process(gas_station_control(env, station_tank))
        env.process(car_generator(env, gas_station, station_tank))

        # Execute!
        env.run(until=SIM_TIME)
        ```

---

## Sources

- [SimPy 4.1.2, Gas Station Refueling](https://simpy.readthedocs.io/en/4.1.2/examples/gas_station_refuel.html): the original code and its output.
- [SimPy's licence](https://gitlab.com/team-simpy/simpy/-/blob/master/LICENSE.rst): MIT, Copyright (c) 2013 Ontje Lünsdorf and Stefan Scherfke. The SimPy file above carries this notice.
- [`examples/` in the desflow repository](https://github.com/jaehyeon-kim/desflow/tree/main/examples): the three versions in desflow, under `simpy/` in each folder.
