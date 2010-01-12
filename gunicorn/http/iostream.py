# -*- coding: utf-8 -
#
# 2009 (c) Benoit Chesneau <benoitc@e-engura.com> 
# 2009 (c) Paul J. Davis <paul.joseph.davis@gmail.com>
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

import errno
import socket
import logging

class IOStream(object):

    def __init__(self, socket, max_buffer_size=104857600, recv_chunk_size=4096):
        self.socket = socket
        self.max_buffer_size = max_buffer_size
        self.recv_chunk_size = recv_chunk_size
        self._recv_buffer = ""
        self._send_buffer = ""

    def recv(self):
        try:
            chunk = self.socket.recv(self.recv_chunk_size)
        except socket.error, e:
            if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                return ""
            elif e[0] in (errno.ECONNRESET, errno.ENOTCONN, errno.ESHUTDOWN):
                logging.warning("connection lost, %s", e)
                self.close()
                return ""
            else:
                self.close()
                return
        if not chunk:
            self.close()
            return

        self._recv_buffer += chunk
        if len(self._recv_buffer) > self.max_buffer_size:
            logging.error("Reached max buffer size")
            self.close()
            return
        else:
            return self._recv_buffer

    def send(self, data):
        if not self.socket:
            raise IOError("Closed")

        self._send_buffer += data
        while self._send_buffer:
            try:
                num_bytes = self.socket.send(self._send_buffer)
                self._send_buffer = self._send_buffer[num_bytes:]
                return num_bytes
            except socket.error, e:
                if e[0] in (errno.EWOULDBLOCK, errno.EAGAIN):
                    break
                else:
                    self.close()
                    return

    def sending(self):
        return len(self._send_buffer) > 0

    def read_until(self, delimiter):
        if not self.socket:
            raise IOError("Closed")

        while True:
            try:
                chunk = self.recv()
            except socket.error, e:
                return
            self._recv_buffer += str(chunk)

            buflen = len(self._recv_buffer)
            dellen = len(delimiter)
            i = self._recv_buffer.find(delimiter)
            if i != -1:
                if i > 0:
                    data = self._recv_buffer[:i]
                self._recv_buffer = self._recv_buffer[i + dellen:]
                return data

    def close(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def closed(self):
        return self.socket is None
