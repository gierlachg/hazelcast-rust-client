use std::{
    convert::TryInto,
    error::Error,
    future::Future,
    pin::Pin,
    sync::atomic::{AtomicUsize, Ordering},
    task::{Context, Poll},
};
use std::collections::HashMap;

use bytes::{Buf, Bytes, BytesMut};
use futures::SinkExt;
use log::error;
use tokio::{
    net::{tcp::{ReadHalf, WriteHalf}, TcpStream},
    prelude::*,
    stream::{Stream, StreamExt},
    sync::{mpsc, oneshot},
    task,
};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

use crate::{
    message::Message,
    remote::{
        LENGTH_FIELD_ADJUSTMENT, LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET, MessageCodec, PROTOCOL_SEQUENCE,
    },
};

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;
type Responder = oneshot::Sender<Message>;
type Frame = (Bytes, u64, Responder);

enum Event {
    Egress(Frame),
    Ingress(BytesMut),
}

pub(in crate::remote) struct Channel {
    sequence: AtomicUsize,
    egress: mpsc::UnboundedSender<Frame>,
}

impl Channel {
    pub(in crate::remote) async fn connect(address: &str) -> Result<Self> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(&PROTOCOL_SEQUENCE).await?;

        let (sender, receiver) = mpsc::unbounded_channel();
        spawn(async move {
            let (reader, writer) = stream.split();
            let mut writer = Writer::new(writer);
            let mut events = Broker::new(receiver, reader);

            let mut correlations = HashMap::with_capacity(1024);
            while let Some(event) = events.next().await {
                match event {
                    Ok(Event::Egress((frame, correlation_id, responder))) => {
                        writer.write(frame).await?;
                        correlations.insert(correlation_id, responder);
                    }
                    Ok(Event::Ingress(mut frame)) => {
                        let (message, correlation_id) = MessageCodec::decode(frame.to_bytes());
                        match correlations
                            .remove(&correlation_id)
                            .expect("missing correlation!")
                            .send(message)
                            {
                                _ => {} // TODO:
                            }
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(())
        });

        Ok(Channel { sequence: AtomicUsize::new(1), egress: sender })
    }

    pub(in crate::remote) async fn send(&self, message: Message) -> Result<Message> {
        let correlation_id: u64 = self.sequence.fetch_add(1, Ordering::SeqCst).try_into().expect("unable to convert!");
        let frame = MessageCodec::encode(&message, correlation_id);
        let (sender, receiver) = oneshot::channel();
        self.egress.send((frame, correlation_id, sender))?;
        Ok(receiver.await?)
    }
}

struct Writer<'a> {
    writer: FramedWrite<WriteHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Writer<'a> {
    fn new(writer: WriteHalf<'a>) -> Self {
        let writer = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
            .little_endian()
            .new_write(writer);

        Writer { writer }
    }

    async fn write(&mut self, frame: Bytes) -> Result<()> {
        Ok(self.writer.send(frame).await?)
    }
}

struct Broker<'a> {
    egress: mpsc::UnboundedReceiver<Frame>,
    ingress: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Broker<'a> {
    fn new(
        messages: mpsc::UnboundedReceiver<Frame>,
        reader: ReadHalf<'a>,
    ) -> Self {
        let reader = LengthDelimitedCodec::builder()
            .length_field_offset(LENGTH_FIELD_OFFSET)
            .length_field_length(LENGTH_FIELD_LENGTH)
            .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
            .little_endian()
            .new_read(reader);

        Broker { egress: messages, ingress: reader }
    }
}

impl Stream for Broker<'_> {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(Some((frame, correlation_id, responder))) = Pin::new(&mut self.egress).poll_next(cx) {
            return Poll::Ready(Some(Ok(Event::Egress((frame, correlation_id, responder)))));
        }
        // TODO: handle end of stream...

        let result: Option<_> = futures::ready!(Pin::new(&mut self.ingress).poll_next(cx));
        Poll::Ready(match result {
            Some(Ok(frame)) => Some(Ok(Event::Ingress(frame))),
            Some(Err(e)) => Some(Err(e.into())),
            None => None,
        })
    }
}

fn spawn<F>(future: F) -> task::JoinHandle<()>
    where
        F: Future<Output=Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = future.await {
            error!("{}", e)
        }
    })
}
