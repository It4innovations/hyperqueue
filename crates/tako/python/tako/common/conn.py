import asyncio
import logging
import os
import struct
from asyncio import IncompleteReadError, StreamReader, StreamWriter

import msgpack

logger = logging.getLogger(__name__)


async def connect_to_unix_socket(socket_path):
    # Protection against long filenames, socket names are limited
    backup = os.getcwd()
    try:
        os.chdir(os.path.dirname(socket_path))
        reader, writer = await asyncio.open_unix_connection(socket_path)
    finally:
        os.chdir(backup)

    return SocketWrapper(reader, writer)


class SocketWrapper:
    header = struct.Struct("<I")
    header_size = 4

    def __init__(self, reader: StreamReader, writer: StreamWriter):
        self.reader = reader
        self.writer = writer
        self._buffer = bytes()

    async def send_message(self, message):
        return await self.write_raw_message(msgpack.dumps(message))

    async def receive_message(self):
        data = await self.read_raw_message()
        # print("MESSAGE", data)
        # logger.info("MESSAGE %s", data)
        return msgpack.loads(data)

    async def write_raw_message(self, message):
        header = SocketWrapper.header.pack(len(message))
        writer = self.writer
        if len(message) <= 1_024:
            writer.write(header + message)
        else:
            writer.write(header)
            writer.write(message)
        return await writer.drain()

    async def read_raw_message(self):
        try:
            header = await self.reader.readexactly(SocketWrapper.header_size)
        except IncompleteReadError as e:
            if not e.partial:
                raise Exception("Connection closed")
            else:
                raise e
        msg_size = SocketWrapper.header.unpack(header)[0]
        return await self.reader.readexactly(msg_size)
