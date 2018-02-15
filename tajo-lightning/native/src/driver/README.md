# Driver
## Design Considerations
* Each driver instance should be easily able to be an independant object for easier integration unit tests.
* Driver should mostly depend on only config files rather than shell environment variables.
* It should be able to throw an error instead of exiting directly.
* It should be a daemon having running/stop states, and it should be hanging while running.
* It should be a delegator to throw all kinds of errors happening from all services.
* It is a single executable program for coordinator and workers for easiler deployments.