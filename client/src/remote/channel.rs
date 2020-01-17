use std::{
    error::Error,
    future::Future,
    pin::Pin,
    task::{Context, Poll},
};

use bytes::{Buf, BytesMut};
use futures::SinkExt;
use log::error;
use tokio::{
    net::{tcp::ReadHalf, TcpStream},
    prelude::*,
    stream::{Stream, StreamExt},
    sync::{mpsc, oneshot},
    task,
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::{
    message::Message,
    remote::{
        Correlator, LENGTH_FIELD_ADJUSTMENT, LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET, MessageCodec, PROTOCOL_SEQUENCE,
    },
};

type Responder = oneshot::Sender<Message>;
type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

enum Event {
    Egress(Message, Responder),
    Ingress(BytesMut),
}

pub(in crate::remote) struct Channel {
    egress: mpsc::UnboundedSender<(Message, Responder)>,
}

impl Channel {
    pub(in crate::remote) async fn connect(address: &str) -> Result<Self> {
        let mut stream = TcpStream::connect(address).await?;
        stream.write_all(&PROTOCOL_SEQUENCE).await?;

        let (egress, ingress): (
            mpsc::UnboundedSender<(Message, Responder)>,
            mpsc::UnboundedReceiver<(Message, Responder)>,
        ) = mpsc::unbounded_channel();

        spawn(async move {
            let (reader, writer) = stream.split();
            let reader = LengthDelimitedCodec::builder()
                .length_field_offset(LENGTH_FIELD_OFFSET)
                .length_field_length(LENGTH_FIELD_LENGTH)
                .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
                .little_endian()
                .new_read(reader);
            let mut writer = LengthDelimitedCodec::builder()
                .length_field_offset(LENGTH_FIELD_OFFSET)
                .length_field_length(LENGTH_FIELD_LENGTH)
                .length_adjustment(LENGTH_FIELD_ADJUSTMENT)
                .little_endian()
                .new_write(writer);

            let mut correlator = Correlator::new();
            let mut events = Broker::new(ingress, reader);

            while let Some(event) = events.next().await {
                match event {
                    Ok(Event::Egress(message, responder)) => {
                        let correlation_id = correlator.set(responder);
                        let frame = MessageCodec::encode(&message, correlation_id);
                        writer.send(frame).await?
                    }
                    Ok(Event::Ingress(mut frame)) => {
                        let (message, correlation_id) = MessageCodec::decode(frame.to_bytes());
                        match correlator
                            .get(&correlation_id)
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

        Ok(Channel { egress })
    }

    pub(in crate::remote) async fn send(&self, egress: Message) -> Result<Message> {
        let (sender, receiver) = oneshot::channel();
        self.egress.send((egress, sender))?;
        let ingress = receiver.await?;
        Ok(ingress)
    }
}

struct Broker<'a> {
    egress: mpsc::UnboundedReceiver<(Message, Responder)>,
    ingress: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
}

impl<'a> Broker<'a> {
    fn new(
        egress: mpsc::UnboundedReceiver<(Message, Responder)>,
        ingress: FramedRead<ReadHalf<'a>, LengthDelimitedCodec>,
    ) -> Self {
        Broker { egress, ingress }
    }
}

impl Stream for Broker<'_> {
    type Item = Result<Event>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if let Poll::Ready(Some((message, responder))) = Pin::new(&mut self.egress).poll_next(cx) {
            return Poll::Ready(Some(Ok(Event::Egress(message, responder))));
        }

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
