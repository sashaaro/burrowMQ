use amq_protocol::frame::{AMQPFrame, gen_frame};
use rand::Rng;

pub(crate) fn gen_random_name() -> String {
    let mut rng = rand::rng();
    let mut queue_name = String::with_capacity(10);
    for _ in 0..10 {
        queue_name.push(rng.random_range('a'..='z'));
    }
    queue_name
}

pub(crate) fn make_buffer_from_frame(frame: &AMQPFrame) -> anyhow::Result<Vec<u8>> {
    let buffer = Vec::with_capacity(1024); // TODO reuse response memory per session. stop allocate every time
    let write_context = gen_frame(frame)(buffer.into())?;

    Ok(write_context.write)
}
