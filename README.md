# Light status data builder
The Application serves as middleware service between queue with data about status of lights in apartment and time 
when metric was taken. 

To start project:
 - Create virtual environment
 - Install requirements with `pip install requirements.txt`
 - Run redis
 - Run graphite

# Requirements
 - Python >3.10
 - Docker
 - Redis installed locally (see instructions below)
 - Graphite installed locally (see instructions below)

#Redis
## Install Redis
[Official documentation](https://redis.io/docs/getting-started/installation/) of how to install redis on any OS.


## Start Redis
To start Redis locally you need to run command:

`redis-server`

**Important!** Do not close your terminal where you started Redis. 

## Install graphite
There is two options how to install graphite.

For the first, you need docker installed locally. Lunch docker and run command below in terminal:

`docker run -d \
 --name graphite \
 --restart=always \
 -p 80:80 \
 -p 2003-2004:2003-2004 \
 -p 2023-2024:2023-2024 \
 -p 8125:8125/udp \
 -p 8126:8126 \
 graphiteapp/graphite-statsd`

For the second option everything you need you can find in [official documentation](https://graphite.readthedocs.io/en/latest/install.html) (God bless you if chose this pass).

# Running application
There are three approaches presented in this repo:
 - First is synchronous code which you can find under `sync` folder.
 - Second is asynchronous code without generators which you can find under `async` folder.
 - Third is asynchronous code based on async generators which you can find under `async_generators` folder.

Easy, right?

But before running code you should populate your redis with data. For that each folder contains `send_lights_statuses.py`
which will populate specific year with data (point per each hour in a day in year ≈ 8760 points)

**Enjoy!**