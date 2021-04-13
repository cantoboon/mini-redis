use tokio::{io::{AsyncReadExt, AsyncWriteExt, BufWriter}, net::{TcpListener, TcpStream}};
use std::{collections::HashMap, io::Cursor, usize};
use mini_redis::{Frame, Result};
use bytes::{Buf, Bytes, BytesMut};
use std::sync::{Arc, Mutex};

type Db = Arc<Mutex<HashMap<String, Bytes>>>;

#[tokio::main]
async fn main() {
    let port = 6379;
    let listener = TcpListener::bind(format!("{}:{}", "127.0.0.1", port)).await.unwrap();

    println!("Listening on {}", port);

    let db: Db = Arc::new(Mutex::new(HashMap::new()));

    loop {
        let (socket, _) = listener.accept().await.unwrap();

        println!("Accepted connection from: {:?}", socket.peer_addr().unwrap());

        let db = db.clone();

        tokio::spawn(async move {
            process(socket, db).await;    
        });
    }
}

async fn process(socket: TcpStream, db: Db) {
    use mini_redis::Command::{self, Get, Set};

    let mut connection = Connection::new(socket);

    while let Some(frame) = connection.read_frame().await.unwrap() {
        let response = match Command::from_frame(frame).unwrap() {
            Set(cmd) => {
                let mut db = db.lock().unwrap();
                db.insert(cmd.key().to_string(), cmd.value().clone());
                println!("Setting {:?} to {:?}", cmd.key().to_string(), cmd.value().clone());
                Frame::Simple("OK".to_string())
            }
            Get(cmd) => {
                let db = db.lock().unwrap();
                if let Some (value) = db.get(cmd.key()) {
                    println!("Get received for {:?}. Returning {:?}", cmd.key().to_string(), value.clone());
                    Frame::Bulk(value.clone().into())
                } else {
                    Frame::Null
                }
            }
            cmd => panic!("unimplemented: {:?}", cmd)
        };

        connection.write_frame(&response).await.unwrap();
    }
}

struct Connection {
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(stream: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(stream),
            buffer: BytesMut::with_capacity(4096),
        }
    }
    
    pub async fn read_frame(&mut self) -> Result<Option<Frame>> {
        loop {
            // Try to parse a frame. If successful, we return it immediately.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // If we can't return a frame, we don't have enough data yet.
            // Here we attempt to read more data.
            //
            // If we read 0 bytes, we need to see the buffer is empty.
            // If it is empty, we simply return an empty Option. If it isn't empty,
            // we know the connection has been reset? Is this a standard TcpStream
            // pattern?
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Result<Option<Frame>> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match Frame::check(&mut buf) {
            Ok(_) => {
                let len = buf.position() as usize;

                // Must reset the position to 0 before calling parse
                buf.set_position(0);

                let frame = Frame::parse(&mut buf)?;

                // Move past the frame in the buffer. 
                self.buffer.advance(len);

                // Return the frame
                Ok(Some(frame))
            },
            // We don't have enough data for a frame just yet. We have to wait for more to be read
            Err(mini_redis::frame::Error::Incomplete) => Ok(None),
            // Errors should be returned from this function
            Err(e) => Err(e.into()),
        }
    }

    pub async fn write_frame(&mut self, frame: &Frame) -> Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Array(_val) => unimplemented!()
        }

        self.stream.flush().await?;

        Ok(())
    }

    /// Write a decimal frame to the stream
    async fn write_decimal(&mut self, val: u64) -> std::io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 12];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}