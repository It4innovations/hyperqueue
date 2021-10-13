# Installation guide #


1. Create environment
```
virtualenv env 
source env/bin/activate
```

2. Install merlin, snakemake
```
pip3 install merlin
pip3 install snakemake
```

3. Install redis (merlin server)
```
wget http://download.redis.io/releases/redis-6.0.5.tar.gz
tar xvf redis*.tar.gz
cd redis*/
make
make test
```

4. Config merlin
```
merlin config --broker redis
```

Run redis server:
```
./src/redis-server &
```

# Run bench #
From benchmark directory run:
```
./benchmarker.py
```