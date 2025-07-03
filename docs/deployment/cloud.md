# Starting HQ without a shared filesystem

By default, HyperQueue assumes the existence of a shared filesystem, which it uses to exchange metadata required to connect servers and workers and to run various HQ commands.

On systems without a shared filesystem, you will have to distribute an *access file* (`access.json`) to clients and workers.
This file contains the address and port where the server is running, and also secret keys required for encrypted communication.

## Sharing the access file

After you start a server, you can find its `access.json` file in the `$HOME/.hq-server/hq-current` directory. You can then copy it to a different filesystem using a method of your choosing, and configure clients and workers to use that file.

By default, clients and workers search for the `access.json` file in the `$HOME/.hq-server` directory, but you can override that using the `--server-dir` argument, which is available for all `hq` CLI commands. If you moved the `access.json` file into a directory called `/home/foo/hq-access` on the worker's node, you should start the worker like this:

```bash
$ hq --server-dir=/home/foo/hq-access worker start
```

!!! tip

    You can also configure the server directory using an [environment variable](./server.md#server-directory).

## Generate an access file in advance

In some cases you might want to generate the access file in advance, before the server is started, and let the server, clients and workers use that access file. This can be useful so that you don't have to redistribute the access file to client/worker nodes everytime the server restarts, which could be cumbersome.

To achieve this, an access file can be generated in advance using the [`hq server generate-access`](cli:hq.server.generate-access) command:

```bash
$ hq server generate-access myaccess.json --client-port=6789 --worker-port=1234
```

This generates a `myaccess.json` file that contains generates keys and host information.

The server can be later started with this configuration as follows:

```bash
$ hq server start --access-file=myaccess.json
```

Clients and workers should load the pre-generated access file in the same way as was described [above](#sharing-the-access-file). However, you will have to rename the generated file to `access.json`, because clients and workers look it up by its exact name in the provided server directory.

!!! note

    The server will still generate and manages its "own" `access.json` in the server directory path, even if you provide your own access file. These files are the same, so you can use either when connectiong clients and workers.


## Splitting access for client and workers

The default access file contains two secret keys and two TCP/IP addresses, one for clients and one for workers. This metadata can be divided into two separate files, containing only information needed only by clients or only by workers.

```bash
$ hq server generate-access full.json --client-file=client.json --worker-file=worker.json --client-port=6789 --worker-port=1234
```

This command creates three files: `full.json`, `client.json`, `worker.json`.

For starting a client you can use `client.json` as `access.json` while it does not contain information for workers.

For starting a worker you can use `worker.json` as `access.json` while it does not contain information for clients.

For starting server (`hq server start --access-file=...`) you have to use `full.json` as it contains all necessary information.

## Setting different server hostname for workers and clients

You can use the following command to configure different hostnames under which the server is visible to workers and clients.

```bash
hq server generate-access full.json --worker-host=<WORKER_HOST> --client-host=<CLIENT_HOST> ...
```
