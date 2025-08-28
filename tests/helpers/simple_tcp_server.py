from cbor_rpc.tcp.tcp import TcpServer, TcpPipe

class SimpleTcpServer(TcpServer):
    """
    A simple TCP server implementation for testing purposes that accepts all connections.
    """
    async def accept(self, pipe: TcpPipe) -> bool:
        """
        Accepts all incoming TCP connections.
        """
        return True
