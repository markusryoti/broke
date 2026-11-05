use std::{
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
};

use tokio::{
    io::{self, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tracing::{error, info};

const NUM_PARTITIONS: usize = 2;

enum Command {
    Push,
    Pull,
    Join,
}

impl Command {
    fn from(raw: &str) -> Result<Command, EventError> {
        let cmd = match raw.trim().to_uppercase().as_str() {
            "PUSH" => Command::Push,
            "PULL" => Command::Pull,
            "JOIN" => Command::Join,
            _ => return Err(EventError::ParseError),
        };

        Ok(cmd)
    }
}

#[derive(Debug)]
struct ConsumerParams {
    topic: String,
    partition: usize,
    offset: usize,
}

impl ConsumerParams {
    fn parse<'a>(mut lines: impl Iterator<Item = &'a str>) -> Result<Self, EventError> {
        let topic: String = lines.next().ok_or(EventError::ParseError)?.into();

        let partition = lines
            .next()
            .ok_or(EventError::ParseError)?
            .parse::<usize>()
            .map_err(|_| EventError::ParseError)?;

        let offset = lines
            .next()
            .ok_or(EventError::ParseError)?
            .parse::<usize>()
            .map_err(|_| EventError::ParseError)?;

        Ok(ConsumerParams {
            topic,
            partition,
            offset,
        })
    }
}

#[derive(Debug)]
struct JoinParams {
    consumer_group: String,
    topic: String,
}

impl JoinParams {
    fn parse<'a>(mut lines: impl Iterator<Item = &'a str>) -> Result<Self, EventError> {
        let consumer_group = lines.next().ok_or(EventError::ParseError)?;
        let topic = lines.next().ok_or(EventError::ParseError)?;

        Ok(JoinParams {
            consumer_group: consumer_group.into(),
            topic: topic.into(),
        })
    }
}

#[derive(Debug)]
struct Event {
    name: String,
    topic: String,
    partition_key: String,
    data: String,
}

impl Event {
    fn log_line(&self) -> String {
        let line = format!(
            "{};{};{};{}\n",
            self.name, self.topic, self.partition_key, self.data
        );

        line
    }
}

impl Event {
    fn parse<'a>(mut lines: impl Iterator<Item = &'a str>) -> Result<Self, EventError> {
        let name = lines.next().ok_or(EventError::ParseError)?;
        let topic = lines.next().ok_or(EventError::ParseError)?;
        let partition_key = lines.next().ok_or(EventError::ParseError)?;
        let data = lines.next().ok_or(EventError::ParseError)?;

        Ok(Event {
            name: name.into(),
            topic: topic.into(),
            partition_key: partition_key.into(),
            data: data.into(),
        })
    }
}

#[derive(Debug)]
enum EventError {
    ParseError,
    HashError,
}

impl std::error::Error for EventError {}

impl std::fmt::Display for EventError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventError::ParseError => write!(f, "ParseError"),
            EventError::HashError => write!(f, "HashError"),
        }
    }
}

fn get_partition(key: &str) -> Result<usize, Box<dyn std::error::Error>> {
    let sum = key
        .bytes()
        .map(|b| b as usize)
        .reduce(|acc, b| acc + b)
        .ok_or(EventError::HashError)?;

    Ok(sum % NUM_PARTITIONS)
}

fn ensure_dir(dir: &str) -> io::Result<()> {
    if !fs::exists(dir)? {
        fs::create_dir_all(dir)?;
    }

    Ok(())
}

fn ensure_file(file: &str) -> io::Result<()> {
    if !fs::exists(file)? {
        File::create(file)?;
    }

    Ok(())
}

async fn handle_call(mut socket: TcpStream) -> Result<(), Box<dyn std::error::Error>> {
    info!("Handling request");

    let mut result_bytes: Vec<u8> = vec![];
    io::copy(&mut socket, &mut result_bytes).await?;
    let raw_event = String::from_utf8(result_bytes)?;

    info!(len_raw = raw_event.len(), "Read raw event");

    let mut lines = raw_event.lines();
    let cmd = lines.next().ok_or(EventError::ParseError)?;
    let cmd = Command::from(cmd)?;

    match cmd {
        Command::Push => handle_producer(lines).await,
        Command::Pull => handle_consumer(socket, lines).await,
        Command::Join => handle_join(socket, lines).await,
    }
}

async fn handle_producer(
    lines: impl Iterator<Item = &str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let event = Event::parse(lines)?;
    info!("{:?}", event);

    let partition = get_partition(&event.partition_key)?;
    let log_line = event.log_line();

    let dir = format!("data/topics/{}", event.topic);
    ensure_dir(&dir)?;

    let file_name = format!("{}/{}", dir, partition);
    ensure_file(&file_name)?;

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(file_name)?;

    file.write_all(log_line.as_bytes())?;

    info!("request handled");

    Ok(())
}

async fn handle_consumer(
    mut socket: TcpStream,
    lines: impl Iterator<Item = &str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let params = ConsumerParams::parse(lines)?;

    info!("{:?}", params);

    let dir = format!("data/topics/{}", params.topic);
    let file = format!("{}/{}", dir, params.partition);

    let file = File::open(file)?;
    let lines = BufReader::new(file).lines();
    let mut lines: Vec<_> = lines
        .enumerate()
        .filter(|p| p.0 >= params.offset)
        .map(|p| p.1.unwrap_or("".into()))
        .filter(|l| !l.is_empty())
        .collect();

    lines.push("\n".to_string());

    info!(num_lines = lines.len(), "Sending data for consumer");

    for l in lines {
        let _ = socket.write(l.as_bytes()).await?;
    }

    Ok(())
}

async fn handle_join(
    socket: TcpStream,
    lines: impl Iterator<Item = &str>,
) -> Result<(), Box<dyn std::error::Error>> {
    let params = JoinParams::parse(lines)?;

    info!("{:?}", params);

    Ok(())
}

#[tokio::main]
async fn main() -> io::Result<()> {
    tracing_subscriber::fmt::init();

    info!("Starting server");

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    loop {
        let (socket, _) = listener.accept().await?;

        tokio::spawn(async move {
            let _ = handle_call(socket).await.inspect_err(|e| error!("{}", e));
        });
    }
}
