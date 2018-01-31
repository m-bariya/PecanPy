# PecanPy

Python API for the Pecan Street Dataport.

## Installation
First, install the [Git](https://git-scm.com/downloads) version control system and [clone](https://help.github.com/articles/cloning-a-repository/) this repository onto your local machine. Next, install the [Conda](https://conda.io/docs/user-guide/install/index.html) package and environment management system.

Open a terminal and run the following command...

```bash
$ conda env -f environment.yml
```

...which uses `conda` to create a clean virtual development environment with
Python 3.6 and all required dependencies for `pecanpy`. Activate the new
environment using the OS-specific instructions printed to the terminal.

After activating the `pecanpy-dev` environment, run the following command...

```bash
$ pip install -e .
```

...to install the `pecanpy` package in development mode.
