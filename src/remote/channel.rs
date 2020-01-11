use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use futures::SinkExt;
use tokio::{
    net::{tcp::ReadHalf, TcpStream},
    prelude::*,
    stream::{Stream, StreamExt},
    sync::{mpsc, oneshot},
    task,
};
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::message::Message;
use crate::remote::{
    Correlator, FrameCodec, LENGTH_FIELD_ADJUSTMENT, LENGTH_FIELD_LENGTH, LENGTH_FIELD_OFFSET,
    PROTOCOL_SEQUENCE,
};
use crate::Result;

type Responder = oneshot::Sender<Message>;

enum Event {
    Egress(Message, Responder),
    Ingress(BytesMut),
}

pub(crate) struct Channel {
    egress: mpsc::UnboundedSender<(Message, Responder)>,
}

impl Channel {
    pub(crate) async fn connect(address: &str) -> Result<Self> {
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
                        let mut frame = BytesMut::new();
                        let correlation_id = correlator.set(responder);
                        FrameCodec::encode(message, correlation_id, &mut frame);
                        writer.send(frame.to_bytes()).await?;
                    }
                    Ok(Event::Ingress(mut frame)) => {
                        let frame_length = frame.len();
                        let (message, correlation_id) =
                            FrameCodec::decode(&mut frame, frame_length);
                        match correlator
                            .get(&correlation_id)
                            .expect("missing correlation!")
                            .send(message)
                        {
                            _ => {} // TODO:
                        }
                    }
                    Err(error) => {
                        return Err(error);
                    }
                }
            }
            Ok(())
        });

        Ok(Channel { egress })
    }

    pub(crate) async fn send(&mut self, egress: Message) -> Result<Message> {
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
            Some(Err(error)) => Some(Err(error.into())),
            None => None,
        })
    }
}

fn spawn<F>(future: F) -> task::JoinHandle<()>
where
    F: Future<Output = Result<()>> + Send + 'static,
{
    tokio::spawn(async move {
        if let Err(e) = future.await {
            eprintln!("Unexpected error occurred - {}", e)
        }
    })
}
