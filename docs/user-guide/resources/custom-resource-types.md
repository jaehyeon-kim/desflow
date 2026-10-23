# Custom Resource Types

A [resource](index.md) can grant its units by a rule of your own. This page shows how to write one.

---

## How to Use It

To change how a resource grants units, subclass `ResourceBase` and name the class in `type`. `ResourceBase` is built on the hooks of SimPy's `BaseResource`, as a subclass of SimPy's `Resource` is. A subclass can change these:

- Override `_do_put(request)` to decide when a request is granted, and `_do_get(release)` to decide what a release gives back.
- Set `PutQueue` to the class of the waiting line. It can be SimPy's `SortedQueue`, written `staticmethod(functools.partial(SortedQueue, maxlen=2))`.
- Set `request` to `BoundClass(MyRequest)`, for a request class that subclasses `DynamicResourceRequest`, or set `release` the same way, for a release class that subclasses `DynamicRelease`. Both base classes are in `desflow.resources.resource`.

`DynamicResource` is `ResourceBase` with nothing changed. The constructor is `DynamicResource`'s, because the resource reads its capacity from the registry. A subclass that defines `__init__` therefore takes `*args, **kwargs` and passes them on. A `PutQueue` of your own sets its own order and length, so it takes no `queue_limit`. The tests run each of these beside the same SimPy subclass in all three APIs.

This resource keeps how long each request waited:

=== "YAML"

    ```yaml title="docs/snippets/yaml/resources_custom.yaml:declare"
    resources:
      # A subclass of ResourceBase that keeps how long each request waited
      packers: {current_cap: 1, max_cap: 4, type: !python shop_logic.LoggedPackers}
    ```

    The class is in `shop_logic.py`, beside the blueprint:

    ```python title="docs/snippets/yaml/shop_logic.py:logged-packers"
    class LoggedPackers(ResourceBase):
        """A resource that keeps how long each request waited before it was granted."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.waits = []

        def _do_put(self, request):
            super()._do_put(request)
            if request.triggered:  # the request was granted just now
                self.waits.append(self.env.now - request.time)
    ```

=== "Builder"

    ```python title="docs/snippets/pages/custom_resources.py:builder"
    app = SimulationContext(sim_id="Shop", factor=0.0)
    app.add_resource("packers", current_cap=1, max_cap=4, type=LoggedPackers)
    ```

    The class is the one the Low-level tab shows.

=== "Low-level"

    ```python title="docs/snippets/pages/custom_resources.py:class"
    class LoggedPackers(ResourceBase):
        """A resource that keeps how long each request waited before it was granted."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.waits = []

        def _do_put(self, request):
            super()._do_put(request)
            if request.triggered:  # the request was granted just now
                self.waits.append(self.env.now - request.time)
    ```

    ```python title="docs/snippets/pages/custom_resources.py:low-level"
    env = DynamicRealtimeEnvironment(factor=0.0)
    env.registry.register_sim_parameter(
        SimParameter(
            sim_id="Shop", resources={"packers": CapacityConfig(current_cap=1, max_cap=4)}
        )
    )
    packers = LoggedPackers(env, "Shop", "packers")


    def order(env):
        with packers.request() as request:
            yield request
            yield env.timeout(2.0)
    ```

---

## Stability and Limits

The limits of this page are listed with those of the whole section, in [Stability and Limits](index.md#stability-and-limits) on the Resources page.
