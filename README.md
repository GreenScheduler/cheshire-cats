`cheshireCATS` runs as a separate process on a SLURM cluster and masks nodes during peak carbon intensity times. Carbon intensity forecasts are obtained from the [CATS](https://github.com/GreenScheduler/cats) carbon footprint API.

The project is under initial development. The road map is:

    1. Establish remote procedure calls (RPC) to `slurmctld` that drain and subsequently turn nodes on and off.
    2. Call the python CATS API to receive the carbon intensity forecast.
    3. Develop an optimization algorithm that selects nodes for deraining and/or powering down to achieve maximal computational resource availability given a carbon footprint budget.
