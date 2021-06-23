use crate::client::commands::log::{CatOpts, Channel, ShowOpts};
use crate::transfer::stream::ChannelId;
use crate::{JobTaskCount, JobTaskId, Map};
use byteorder::ReadBytesExt;
use colored::{Color, Colorize};
use smallvec::SmallVec;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, ErrorKind, Read, Seek, SeekFrom, Write};
use std::path::Path;
use tako::InstanceId;

pub const HQ_LOG_HEADER: &[u8] = b"HQ:log";
pub const HQ_LOG_VERSION: u32 = 0;

pub const BLOCK_STREAM_START: u8 = 0;
pub const BLOCK_STREAM_CHUNK: u8 = 1;
pub const BLOCK_STREAM_END: u8 = 2;

pub struct ChunkInfo {
    position: u64,
    size: u32, // Currently chunk is actually limited to 128kB
}

pub struct InstanceInfo {
    instance_id: InstanceId,
    channels: [Vec<ChunkInfo>; 2],
    finished: bool,
}

pub struct TaskInfo {
    instances: SmallVec<[InstanceInfo; 1]>,
}

impl TaskInfo {
    pub fn last_instance(&self) -> &InstanceInfo {
        self.instances.last().unwrap()
    }
    pub fn superseded(&self) -> impl Iterator<Item = &InstanceInfo> {
        self.instances[0..self.instances.len() - 1].iter()
    }

    pub fn instance_mut(&mut self, instance_id: InstanceId) -> Option<&mut InstanceInfo> {
        self.instances
            .iter_mut()
            .find(|info| info.instance_id == instance_id)
    }

    pub fn instance(&self, instance_id: InstanceId) -> Option<&InstanceInfo> {
        self.instances
            .iter()
            .find(|info| info.instance_id == instance_id)
    }

    pub fn new_instance(&mut self, instance_id: InstanceId) -> anyhow::Result<()> {
        match self
            .instances
            .binary_search_by(|info| info.instance_id.cmp(&instance_id))
        {
            Ok(_) => {
                anyhow::bail!("Instance already exists")
            }
            Err(pos) => self.instances.insert(
                pos,
                InstanceInfo {
                    instance_id,
                    channels: [Vec::new(), Vec::new()],
                    finished: false,
                },
            ),
        }
        Ok(())
    }
}

pub struct LogFile {
    file: BufReader<File>,
    index: BTreeMap<JobTaskId, TaskInfo>,
    start_pos: u64,
}

pub struct Summary {
    pub n_tasks: JobTaskCount,
    pub n_streams: u64,
    pub n_opened: u64,
    pub stdout_size: u64,
    pub stderr_size: u64,
    pub n_superseded: u64,
    pub superseded_stdout_size: u64,
    pub superseded_stderr_size: u64,
}

enum Block {
    StreamStart {
        task_id: JobTaskId,
        instance_id: InstanceId,
    },
    StreamChunk {
        task_id: JobTaskId,
        instance_id: InstanceId,
        channel_id: ChannelId,
        size: u32,
    },
    StreamEnd {
        task_id: JobTaskId,
        instance_id: InstanceId,
    },
}

impl LogFile {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        let mut file = BufReader::new(File::open(path)?);
        LogFile::check_header(&mut file)?;
        let start_pos = file.stream_position()?;
        let index = LogFile::make_index(&mut file)?;
        Ok(LogFile {
            file,
            index,
            start_pos,
        })
    }

    fn check_header(file: &mut BufReader<File>) -> anyhow::Result<()> {
        let mut header = [0u8; 6];
        file.read_exact(&mut header)?;
        if &header != &HQ_LOG_HEADER {
            anyhow::bail!("Invalid file format");
        }
        let version = file.read_u32::<byteorder::BigEndian>()?;
        if version != HQ_LOG_VERSION {
            anyhow::bail!("Invalid version log file version: {}", version);
        }
        let _ = file.read_u64::<byteorder::BigEndian>()?; // Reserved bytes
        let _ = file.read_u64::<byteorder::BigEndian>()?; // Reserved bytes
        Ok(())
    }

    pub fn summary(&self) -> Summary {
        let n_opened = self
            .index
            .values()
            .map(|infos| if infos.last_instance().finished { 0 } else { 1 })
            .sum::<u64>();

        let n_streams = self
            .index
            .values()
            .map(|infos| infos.instances.len() as u64)
            .sum::<u64>();

        let mut stdout_size = 0u64;
        let mut stderr_size = 0u64;
        let mut superseded_stdout_size = 0u64;
        let mut superseded_stderr_size = 0u64;

        for task_info in self.index.values() {
            let info = task_info.last_instance();
            stdout_size += info.channels[0].iter().map(|c| c.size as u64).sum::<u64>();
            stderr_size += info.channels[1].iter().map(|c| c.size as u64).sum::<u64>();

            for info in task_info.superseded() {
                superseded_stdout_size +=
                    info.channels[0].iter().map(|c| c.size as u64).sum::<u64>();
                superseded_stderr_size +=
                    info.channels[1].iter().map(|c| c.size as u64).sum::<u64>();
            }
        }

        Summary {
            n_tasks: self.index.len() as JobTaskCount,
            n_streams,
            n_opened,
            n_superseded: n_streams - self.index.len() as u64,
            stdout_size,
            stderr_size,
            superseded_stderr_size,
            superseded_stdout_size,
        }
    }

    pub fn cat(&mut self, opts: &CatOpts) -> anyhow::Result<()> {
        self.file.seek(SeekFrom::Start(self.start_pos))?;
        let selected_channel_id = match opts.channel {
            Channel::Stdout => 0,
            Channel::Stderr => 1,
        };
        let mut buffer = Vec::new();
        let mut last_pos: i64 = self.start_pos as i64;
        let stdout = std::io::stdout();
        let mut stdout_buf = BufWriter::new(stdout.lock());

        for (_, task_info) in &self.index {
            let instance = task_info.last_instance();
            if !instance.finished {
                continue;
            }
            for chunk in &instance.channels[selected_channel_id] {
                // We are using seek_relative which makes things slightly
                // more complicated, but it does drop caches if it is not necessary
                // in comparison to self.file.seek
                let diff = chunk.position as i64 - last_pos;
                self.file.seek_relative(diff)?;
                buffer.resize(chunk.size as usize, 0u8);
                self.file.read_exact(&mut buffer)?;
                stdout_buf.write_all(&buffer)?;
                last_pos = chunk.position as i64 + chunk.size as i64;
            }
        }
        Ok(())
    }

    fn read_block(file: &mut BufReader<File>) -> anyhow::Result<Option<Block>> {
        match file.read_u8() {
            Ok(BLOCK_STREAM_START) => {
                let task_id = file.read_u32::<byteorder::BigEndian>()?;
                let instance_id = file.read_u32::<byteorder::BigEndian>()?;
                Ok(Some(Block::StreamStart {
                    task_id,
                    instance_id,
                }))
            }
            Ok(BLOCK_STREAM_CHUNK) => {
                // Job task stream data
                let task_id = file.read_u32::<byteorder::BigEndian>()?;
                let instance_id = file.read_u32::<byteorder::BigEndian>()?;
                let channel_id = file.read_u32::<byteorder::BigEndian>()?;
                let size = file.read_u32::<byteorder::BigEndian>()?;
                Ok(Some(Block::StreamChunk {
                    task_id,
                    instance_id,
                    channel_id,
                    size,
                }))
            }
            Ok(BLOCK_STREAM_END) => {
                let task_id = file.read_u32::<byteorder::BigEndian>()?;
                let instance_id = file.read_u32::<byteorder::BigEndian>()?;
                Ok(Some(Block::StreamEnd {
                    task_id,
                    instance_id,
                }))
            }
            Err(e) => match e.kind() {
                ErrorKind::UnexpectedEof => Ok(None),
                _ => Err(e.into()),
            },
            Ok(r) => anyhow::bail!("Invalid  entry type: {}", r),
        }
    }

    pub fn show(&mut self, opts: &ShowOpts) -> anyhow::Result<()> {
        let id_width = if let Some(max_id) = self.index.keys().max() {
            max_id.to_string().len()
        } else {
            return Ok(());
        };
        self.file.seek(SeekFrom::Start(self.start_pos))?;
        let mut buffer = Vec::new();

        let colors = [
            Color::Red,
            Color::Green,
            Color::Yellow,
            Color::Blue,
            Color::Magenta,
            Color::Cyan,
        ];

        let selected_channel_id = opts.channel.as_ref().map(|c| match c {
            Channel::Stdout => 0,
            Channel::Stderr => 1,
        });

        let stdout = std::io::stdout();
        let mut stdout_buf = BufWriter::new(stdout.lock());

        let active_instances: Map<_, _> = self
            .index
            .iter()
            .map(|(job_id, info)| (*job_id, info.last_instance().instance_id))
            .collect();

        loop {
            match Self::read_block(&mut self.file)? {
                Some(Block::StreamStart { .. }) => {}
                Some(Block::StreamChunk {
                    task_id,
                    instance_id,
                    channel_id,
                    size,
                }) => {
                    if selected_channel_id
                        .map(|id| channel_id == id)
                        .unwrap_or(true)
                        && *active_instances.get(&task_id).unwrap() == instance_id
                    {
                        buffer.resize(size as usize, 0u8);
                        self.file.read_exact(&mut buffer)?;
                        if buffer.last() != Some(&b'\n') {
                            buffer.push(b'\n');
                        }
                        let color = colors[task_id as usize % colors.len()];
                        let header =
                            format!("{:0width$}:{}>", task_id, channel_id, width = id_width,);
                        write!(
                            stdout_buf,
                            "{} {}",
                            header.on_color(color),
                            String::from_utf8_lossy(&buffer)
                        )?;
                    } else {
                        self.file.seek_relative(size as i64)?;
                    }
                }
                Some(Block::StreamEnd {
                    task_id,
                    instance_id,
                }) => {
                    if *active_instances.get(&task_id).unwrap() == instance_id {
                        let color = colors[task_id as usize % colors.len()];
                        writeln!(
                            stdout_buf,
                            "{}",
                            format!("{:0width$}: > stream closed", task_id, width = id_width)
                                .on_color(color)
                        )?;
                    }
                }
                None => break,
            }
        }
        Ok(())
    }

    fn make_index(file: &mut BufReader<File>) -> anyhow::Result<BTreeMap<JobTaskId, TaskInfo>> {
        //let position = file.stream_position()?;
        // index: Map<JobTaskId, Vec<(u64, usize)>>,
        let mut index = BTreeMap::new();
        loop {
            match Self::read_block(file)? {
                Some(Block::StreamStart {
                    task_id,
                    instance_id,
                }) => {
                    log::debug!("Task {} started in stream", task_id);
                    let task_info = index.entry(task_id).or_insert_with(|| TaskInfo {
                        instances: Default::default(),
                    });
                    task_info.new_instance(instance_id)?;
                }
                Some(Block::StreamChunk {
                    task_id,
                    instance_id,
                    channel_id,
                    size,
                }) => {
                    if channel_id >= 2 {
                        anyhow::bail!("Invalid channel id");
                    }
                    if let Some(instance) = index
                        .get_mut(&task_id)
                        .and_then(|task_info| task_info.instance_mut(instance_id))
                    {
                        instance.channels[channel_id as usize].push(ChunkInfo {
                            position: file.stream_position()?,
                            size,
                        });
                    } else {
                        anyhow::bail!("Data chunk for invalid task");
                    };
                    file.seek_relative(size as i64)?;
                }
                Some(Block::StreamEnd {
                    task_id,
                    instance_id,
                }) => {
                    log::debug!("Task {} finished in stream", task_id);
                    if let Some(instance) = index
                        .get_mut(&task_id)
                        .and_then(|info| info.instance_mut(instance_id))
                    {
                        instance.finished = true;
                    } else {
                        anyhow::bail!("Termination of an invalid task");
                    };
                }
                None => break,
            };
        }
        Ok(index)
    }
}
