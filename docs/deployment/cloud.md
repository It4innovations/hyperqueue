# Starting HQ without shared file system

On system without shared file system, all what is needed is to distribute access file (`access.json`) to clients and workers.
This file contains address and port where server is running and secret keys.
By default, client and worker search for `access.json` in `$HOME/.hq-server`.

# Generate access file in advance

In many cases you, we want to generate an access file in advance before any server is started;
moreover, we do not want to regenerate secret keys in every start of server,
because we do not want to redistribute access when server is restarted.

To solve this, an access file can be generated in advance by command "generate-access", e.g.:

```commandline
$ hq server generate-access myaccess.json --client-port=6789 --worker-port=1234
```

This generates `myaccess.json` that contains generates keys and host information.

The server can be later started with this configuration as follows:

```commandline
$ hq server start --access-file=myaccess.json
```

Note: That server still generates and manages "own" `access.json` in the server directory path.
For connecting clients and workers you can use both, `myaccess.json` or newly generated `access.json`, they are same.

Example of starting a worker from `myaccess.json`

```commandline
$ mv myaccess.json /mydirectory/access.json
$ hq --server-dir=/mydirectory worker start
```

# Splitting access for client and workers

Access file contains two secret keys and two points to connect, for clients and for workers.
This information can be divided into two separate files,
containing only information needed only by clients or only by workers.

```commandline
$ hq server generate-access full.json --client-file=client.json --worker-file=worker.json --client-port=6789 --worker-port=1234
```

This command creates three files: `full.json`, `client.json`, `worker.json`.

For starting a client you can use `client.json` as `access.json` while it does not contain information for workers.

For starting a worker you can use `worker.json` as `access.json` while it does not contain information for clients.

For starting server (`hq server start --access-file=...`) you have to use `full.json` as it contains all necessary information.


# Setting different server hostname for workers and clients

You can use the following command to configure different hostnames under which the server is visible to workers and clients.

```commandline
hq server generate-access full.json --worker-host=<WORKER_HOST> --client-host=<CLIENT_HOST> ...
```