# io_tester_visualizer

## Running

```bash
usage: run_io.py [-h] --tester TESTER --config CONFIG --output-dir OUTPUT_DIR [--storage STORAGE]

io_tester runner and visualizer

options:
  -h, --help            show this help message and exit
  --tester TESTER       Path to io_tester
  --config CONFIG       Path to configuration .yaml file
  --output-dir OUTPUT_DIR
                        Directory to save the output to
  --storage STORAGE     Directory for temporary files

```

```bash
usage: run_rpc.py [-h] --tester TESTER --config CONFIG --output-dir OUTPUT_DIR [--ip IP] [--server-cpuset SERVER_CPUSET] [--client-cpuset CLIENT_CPUSET]

rpc_tester runner and visualizer

options:
  -h, --help            show this help message and exit
  --tester TESTER       Path to rpc_tester
  --config CONFIG       Path to configuration .yaml file
  --output-dir OUTPUT_DIR
                        Directory to save the output to
  --ip IP               Ip address to connect on
  --server-cpuset SERVER_CPUSET
                        Cpuset for the server
  --client-cpuset CLIENT_CPUSET
                        Cpuset for the client
```

```bash
usage: redraw.py [-h] [--symmetric SYMMETRIC] [--asymmetric ASYMMETRIC] --output-dir OUTPUT_DIR

io_tester runner and visualizer

options:
  -h, --help            show this help message and exit
  --symmetric SYMMETRIC
                        Path to symmetric results
  --asymmetric ASYMMETRIC
                        Path to configuration .yaml file
  --output-dir OUTPUT_DIR
                        Directory to save the output to
```