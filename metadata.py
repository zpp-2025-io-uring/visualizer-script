BACKENDS_NAMES = ["epoll", "linux-aio", "io_uring", "asymmetric_io_uring"]

# https://plotly.com/python/discrete-color/
BACKEND_COLORS = {
    "epoll": "#ef553b",
    "linux-aio": "#ab63fa",
    "io_uring": "#00cc96",
    "asymmetric_io_uring": "#636efa",
}

assert set(BACKENDS_NAMES).issubset(set(BACKEND_COLORS.keys())), "All backends must have a defined color"
