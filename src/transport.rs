use std::net;
use std::io;
use std::time;

use tokio_core::reactor::Handle;
use tokio_core::net::TcpStream;
use cdrs::transport::CDRSTransport;

pub struct TransportTcp(TcpStream);

impl TransportTcp {
    // TODO: change to fn new<A: net::ToSocketAddrs>(addr: A) when _TransportTcp::new
    // to be changed in order to accept net::ToSocketAddrs as arg
    pub fn new(addr: &str, h: &Handle) -> io::Result<TransportTcp> {
        net::TcpStream::connect(addr)
            .and_then(|t| TcpStream::from_stream(t, h))
            .map(|transport| TransportTcp(transport))
    }
}

impl io::Read for TransportTcp {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl io::Write for TransportTcp {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl CDRSTransport for TransportTcp {
    fn try_clone(&self) -> io::Result<TransportTcp> {
        // let addr = try!(self.tcp.peer_addr());
        // TcpStream::connect(addr).map(|socket| TransportTcp { tcp: socket })
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }

    fn close(&mut self, _close: net::Shutdown) -> io::Result<()> {
        // self.tcp.shutdown(close)
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }

    fn set_timeout(&mut self, _dur: Option<time::Duration>) -> io::Result<()> {
        // self.tcp
        //     .set_read_timeout(dur)
        //     .and_then(|_| self.tcp.set_write_timeout(dur))
        Err(io::Error::new(io::ErrorKind::Other, "not implemented"))
    }
}
