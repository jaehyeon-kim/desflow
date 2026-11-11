# SimPy Examples

SimPy's documentation ships seven example programs. Each page here takes one of them, shows SimPy's code piece by piece, and shows the same piece written in desflow as a YAML blueprint, with the builder API, and with the low-level API. Every desflow version is a file that `desflow examples get` downloads and a test runs.

| Example                                                                                                   | What it shows                                                         |
| --------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| [Bank Renege](https://jaehyeon.me/desflow/latest/simpy/examples/bank-renege/index.md)                     | customers who wait too long at a counter go away                      |
| [Carwash](https://jaehyeon.me/desflow/latest/simpy/examples/carwash/index.md)                             | a car keeps a washing machine while the wash runs                     |
| [Machine Shop](https://jaehyeon.me/desflow/latest/simpy/examples/machine-shop/index.md)                   | machines break down, and a repair preempts the repairman's other jobs |
| [Movie Renege](https://jaehyeon.me/desflow/latest/simpy/examples/movie-renege/index.md)                   | a film that sells out sends its queue away                            |
| [Gas Station Refueling](https://jaehyeon.me/desflow/latest/simpy/examples/gas-station-refuel/index.md)    | cars drain a tank that a tank truck refills                           |
| [Process Communication](https://jaehyeon.me/desflow/latest/simpy/examples/process-communication/index.md) | messages through a store, one to one and broadcast                    |
| [Event Latency](https://jaehyeon.me/desflow/latest/simpy/examples/latency/index.md)                       | each message reaches the receiver after a fixed delay                 |

## How to Read the Pages

Each page keeps the system of SimPy's example and changes three things that a reader would otherwise take for a difference in the model.

- **Times are seconds.** SimPy's times have no unit. desflow reads each time unit as one second. In Carwash, Machine Shop and Movie Renege, SimPy counts minutes, and each of its minutes is one second here. A run of 20 minutes in SimPy is a run of 20 seconds in desflow.
- **Numbers differ from SimPy's output.** desflow draws each random value from its own stream, named by the place where the value is used, such as `Bank.service.patience`. A seed of 42 gives different numbers from SimPy's `random.seed(42)`. Each page shows SimPy's output beside the figures of the desflow run. The two have the same structure and different numbers.
- **Machine Shop writes breakdowns as tasks.** SimPy interrupts the process that makes parts. The repair is then a request for the repairman, who also does other jobs. The page writes each machine's failures as a task that interrupts the machine's `part` task, so that the repair is an ordinary request at priority 1.

SimPy's code is shown with its licence notice. [Coming from SimPy](https://jaehyeon.me/desflow/latest/simpy/coming-from-simpy/index.md) maps each SimPy construct to its desflow form, and [SimPy Compatibility](https://jaehyeon.me/desflow/latest/simpy/compatibility/index.md) lists what is supported.
