BACKENDS_NAMES = ["epoll", "linux-aio", "io_uring", "asymmetric_io_uring"]

BACKEND_COLORS = {
    "io_uring": "#1f77b4",  # blue
    "epoll": "#ff7f0e",  # orange
    "linux-aio": "#2ca02c",  # green
    "asymmetric_io_uring": "#e91e63",  # bright magenta/pink
}

assert set(BACKENDS_NAMES).issubset(set(BACKEND_COLORS.keys())), "All backends must have a defined color"
