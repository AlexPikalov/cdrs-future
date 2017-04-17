use std::io;
use std::net;
use std::collections::HashMap;
use futures::future;
use futures::future::Future;

use cdrs::IntoBytes;
use cdrs::types::CBytesShort;
use cdrs::frame::{Frame, Opcode, Flag};
use cdrs::query::{Query, QueryParams, QueryBatch};
use cdrs::frame::frame_response::ResponseBody;
use cdrs::frame::events::SimpleServerEvent;
use cdrs::authenticators::Authenticator;
use cdrs::compression::Compression;
use cdrs::frame::parser::parse_frame;
use cdrs::error;
use cdrs::events::{Listener, EventStream, new_listener};
use cdrs::transport::CDRSTransport;

pub type CassandraOptions = HashMap<String, Vec<String>>;
pub type CDRSFuture<T> = future::BoxFuture<T, error::Error>;

#[derive(Eq,PartialEq,Ord,PartialOrd)]
pub struct CDRS<T: Authenticator, X> {
    compressor: Compression,
    authenticator: T,
    transport: X,
}

impl<'a, T: Authenticator + 'a, X: CDRSTransport + 'a> CDRS<T, X> {
    pub fn new(transport: X, authenticator: T) -> CDRS<T, X>
        where T: Send
    {
        CDRS {
            compressor: Compression::None,
            authenticator: authenticator,
            transport: transport,
        }
    }

    pub fn get_options(&'static mut self) -> CDRSFuture<CassandraOptions>
        where T: Send
    {
        let options_frame = Frame::new_req_options().into_cbytes();

        future::result(self.transport.write(options_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| {
                          parse_frame(&mut self.transport, &self.compressor)
                              .and_then(resolve_supported_ops)
                      })
            .boxed()
    }

    pub fn start(mut self, compressor: Compression) -> CDRSFuture<Session<T, X>>
        where T: Send + 'static,
              X: 'static
    {
        self.compressor = compressor;
        let startup_frame = Frame::new_req_startup(compressor.as_str()).into_cbytes();

        future::result(self.transport.write(startup_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| {
                let start_response = try!(parse_frame(&mut self.transport, &compressor));

                if start_response.opcode == Opcode::Ready {
                    return Ok(Session::start(self));
                }

                if start_response.opcode == Opcode::Authenticate {
                    let body = start_response.get_body()?;
                    let authenticator =
                        body.get_authenticator()
                            .expect("Cassandra Server did communicate that it needed password
                        authentication but the  auth schema was missing in the body response");

                    // This creates a new scope; avoiding a clone
                    // and we check whether
                    // 1. any authenticators has been passed in by client and if not send error back
                    // 2. authenticator is provided by the client and `auth_scheme` presented by
                    //      the server and client are same if not send error back
                    // 3. if it falls through it means the preliminary conditions are true

                    let auth_check = self.authenticator
                        .get_cassandra_name()
                        .ok_or(error::Error::General("No authenticator was provided".to_string()))
                        .map(|auth| {
                            if authenticator != auth {
                                let io_err =
                                    io::Error::new(io::ErrorKind::NotFound,
                                                   format!("Unsupported type of authenticator. {:?} got,
                                     but {} is supported.",
                                                           authenticator,
                                                           authenticator));
                                return Err(error::Error::Io(io_err));
                            }
                            Ok(())
                        });

                    if let Err(err) = auth_check {
                        return Err(err);
                    }

                    let auth_token_bytes = self.authenticator.get_auth_token().into_cbytes();
                    try!(self.transport
                             .write(Frame::new_req_auth_response(auth_token_bytes)
                                        .into_cbytes()
                                        .as_slice()));
                    try!(parse_frame(&mut self.transport, &compressor));

                    return Ok(Session::start(self));
                }

                unimplemented!();
            }).boxed()
    }

    fn drop_connection(&mut self) -> error::Result<()> {
        self.transport
            .close(net::Shutdown::Both)
            .map_err(|err| error::Error::Io(err))
    }
}

pub struct Session<T: Authenticator, X> {
    started: bool,
    cdrs: CDRS<T, X>,
    compressor: Compression,
}

impl<T: Authenticator + 'static, X: CDRSTransport + 'static> Session<T, X> {
    /// Creates new session basing on CDRS instance.
    pub fn start(cdrs: CDRS<T, X>) -> Session<T, X> {
        let compressor = cdrs.compressor.clone();
        Session {
            cdrs: cdrs,
            started: true,
            compressor: compressor,
        }
    }

    /// The method overrides a compression method of current session
    pub fn compressor(&mut self, compressor: Compression) -> &mut Self {
        self.compressor = compressor;
        self
    }

    /// Manually ends current session.
    /// Apart of that session will be ended automatically when the instance is dropped.
    pub fn end(&mut self) {
        if self.started {
            self.started = false;
            match self.cdrs.drop_connection() {
                Ok(_) => (),
                Err(err) => {
                    println!("Error occured during dropping CDRS {:?}", err);
                }
            }
        }
    }

    /// The method makes a request to DB Server to prepare provided query.
    pub fn prepare(&'static mut self,
                   query: String,
                   with_tracing: bool,
                   with_warnings: bool)
                   -> CDRSFuture<Frame>
        where T: Send
    {
        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }

        let options_frame = Frame::new_req_prepare(query, flags).into_cbytes();

        future::result(self.cdrs.transport.write(options_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| parse_frame(&mut self.cdrs.transport, &self.compressor))
            .boxed()
    }

    /// The method makes a request to DB Server to execute a query with provided id
    /// using provided query parameters. `id` is an ID of a query which Server
    /// returns back to a driver as a response to `prepare` request.
    pub fn execute(&'static mut self,
                   id: &CBytesShort,
                   query_parameters: QueryParams,
                   with_tracing: bool,
                   with_warnings: bool)
                   -> CDRSFuture<Frame>
        where T: Send
    {

        let mut flags = vec![];
        if with_tracing {
            flags.push(Flag::Tracing);
        }
        if with_warnings {
            flags.push(Flag::Warning);
        }
        let options_frame = Frame::new_req_execute(id, query_parameters, flags).into_cbytes();

        future::result(self.cdrs.transport.write(options_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| parse_frame(&mut self.cdrs.transport, &self.compressor))
            .boxed()
    }

    /// The method makes a request to DB Server to execute a query provided in `query` argument.
    /// you can build the query with QueryBuilder
    /// ```
    /// use cdrs::query::QueryBuilder;
    /// use cdrs::compression::Compression;
    /// use cdrs::consistency::Consistency;
    ///
    ///   let select_query = QueryBuilder::new("select * from emp").finalize();
    /// ```
    pub fn query(&'static mut self,
                 query: Query,
                 with_tracing: bool,
                 with_warnings: bool)
                 -> CDRSFuture<Frame>
        where T: Send
    {
        let mut flags = vec![];

        if with_tracing {
            flags.push(Flag::Tracing);
        }

        if with_warnings {
            flags.push(Flag::Warning);
        }

        let query_frame = Frame::new_req_query(query.query,
                                               query.consistency,
                                               query.values,
                                               query.with_names,
                                               query.page_size,
                                               query.paging_state,
                                               query.serial_consistency,
                                               query.timestamp,
                                               flags)
                .into_cbytes();

        future::result(self.cdrs.transport.write(query_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| parse_frame(&mut self.cdrs.transport, &self.compressor))
            .boxed()
    }

    pub fn batch(&'static mut self,
                 batch_query: QueryBatch,
                 with_tracing: bool,
                 with_warnings: bool)
                 -> CDRSFuture<Frame>
        where T: Send
    {
        let mut flags = vec![];

        if with_tracing {
            flags.push(Flag::Tracing);
        }

        if with_warnings {
            flags.push(Flag::Warning);
        }

        let query_frame = Frame::new_req_batch(batch_query, flags).into_cbytes();

        future::result(self.cdrs.transport.write(query_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| parse_frame(&mut self.cdrs.transport, &self.compressor))
            .boxed()
    }

    /// It consumes CDRS
    pub fn listen_for<'b>(mut self,
                          events: Vec<SimpleServerEvent>)
                          -> CDRSFuture<(Listener<X>, EventStream)>
        where T: Send
    {
        let query_frame = Frame::new_req_register(events).into_cbytes();

        future::result(self.cdrs.transport.write(query_frame.as_slice()))
            .map_err(Into::into)
            .and_then(move |_| {
                          parse_frame(&mut self.cdrs.transport, &self.compressor)
                              .and_then(move |_| Ok(new_listener(self.cdrs.transport)))
                      })
            .boxed()

    }
}

fn resolve_supported_ops(frame: Frame) -> Result<CassandraOptions, error::Error> {
    match frame.get_body() {
        Ok(ResponseBody::Supported(ref supported_body)) => Ok(supported_body.data.clone()),
        _ => Err("Unexpected type of frame. Supported frame is supported".into()),
    }
}
