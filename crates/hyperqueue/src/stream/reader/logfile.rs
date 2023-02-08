use crate::client::commands::log::{CatOpts, Channel, ExportOpts, ShowOpts};
use crate::common::arraydef::IntArray;
use crate::transfer::stream::ChannelId;
use crate::{JobTaskCount, JobTaskId, Map, Set};
use byteorder::ReadBytesExt;
use colored::{Color, Colorize};
use serde::{Deserialize, Serialize};
use serde_json::json;
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

impl InstanceInfo {
    fn channel_size(&self, channel_id: ChannelId) -> u64 {
        self.channels[channel_id as usize]
            .iter()
            .map(|x| x.size as u64)
            .sum()
    }
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
    current_pos: u64,
}

#[derive(Serialize, Deserialize)]
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

#[allow(clippy::enum_variant_names)]
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
        file.seek(SeekFrom::Start(start_pos))?;
        Ok(LogFile {
            file,
            index,
            start_pos,
            current_pos: start_pos,
        })
    }

    fn check_header(file: &mut BufReader<File>) -> anyhow::Result<()> {
        let mut header = [0u8; 6];
        file.read_exact(&mut header)?;
        if header != HQ_LOG_HEADER {
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
            .map(|infos| u64::from(!infos.last_instance().finished))
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
            stdout_size += info.channel_size(0);
            stderr_size += info.channel_size(1);

            for info in task_info.superseded() {
                superseded_stdout_size += info.channel_size(0);
                superseded_stderr_size += info.channel_size(1);
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

    fn _gather_infos<'a>(
        index: &'a BTreeMap<JobTaskId, TaskInfo>,
        tasks: &Option<IntArray>,
    ) -> anyhow::Result<Vec<(JobTaskId, &'a InstanceInfo)>> {
        Ok(match tasks {
            Some(ref array) => {
                let mut infos = Vec::new();
                for task_id in array.iter() {
                    if let Some(task_info) = index.get(&JobTaskId::new(task_id)) {
                        infos.push((JobTaskId::new(task_id), task_info.last_instance()));
                    } else {
                        anyhow::bail!("Task {} not found", task_id);
                    }
                }
                infos
            }
            None => index
                .iter()
                .map(|(&task_id, task_info)| (task_id, task_info.last_instance()))
                .collect(),
        })
    }

    fn read_buffer(
        file: &mut BufReader<File>,
        current_pos: &mut u64,
        position: u64,
        buffer: &mut [u8],
    ) -> anyhow::Result<()> {
        // We are using seek_relative which makes things slightly
        // more complicated, but it does not drop caches if it is not necessary
        // in comparison to file.seek
        let diff = position as i64 - *current_pos as i64;
        file.seek_relative(diff)?;
        file.read_exact(buffer)?;
        *current_pos = position + buffer.len() as u64;
        Ok(())
    }

    pub fn export(&mut self, opts: &ExportOpts) -> anyhow::Result<()> {
        let task_infos = Self::_gather_infos(&self.index, &opts.task)?;
        let mut result = Vec::new();
        let mut buffer = Vec::new();
        for (task_id, instance) in &task_infos {
            buffer.resize(instance.channel_size(0) as usize, 0u8);
            let mut buf_pos = 0;
            for chunk in &instance.channels[0] {
                let size = chunk.size as usize;
                Self::read_buffer(
                    &mut self.file,
                    &mut self.current_pos,
                    chunk.position,
                    &mut buffer[buf_pos..buf_pos + size],
                )?;
                buf_pos += size;
            }
            debug_assert!(buf_pos == buffer.len());
            result.push(json!({
                "id": task_id,
                "finished": instance.finished,
                "stdout": String::from_utf8_lossy(&buffer),
            }));
        }
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Array(result))
                .expect("Could not format JSON")
        );
        Ok(())
    }

    pub fn cat(&mut self, opts: &CatOpts) -> anyhow::Result<()> {
        let selected_channel_id = match opts.channel {
            Channel::Stdout => 0,
            Channel::Stderr => 1,
        };
        let mut buffer = Vec::new();
        let stdout = std::io::stdout();
        let mut stdout_buf = BufWriter::new(stdout.lock());

        let mut print_instance = |instance: &InstanceInfo| -> anyhow::Result<()> {
            for chunk in &instance.channels[selected_channel_id] {
                buffer.resize(chunk.size as usize, 0u8);
                Self::read_buffer(
                    &mut self.file,
                    &mut self.current_pos,
                    chunk.position,
                    &mut buffer,
                )?;
                stdout_buf.write_all(&buffer)?;
            }
            Ok(())
        };

        let task_infos = Self::_gather_infos(&self.index, &opts.task)?;

        if !opts.allow_unfinished {
            for (task_id, instance) in &task_infos {
                if !instance.finished {
                    anyhow::bail!("Stream for task {} is not finished", task_id);
                }
            }
        }

        for (_, instance) in &task_infos {
            print_instance(instance)?;
        }

        Ok(())
    }

    fn read_block(file: &mut BufReader<File>) -> anyhow::Result<Option<Block>> {
        match file.read_u8() {
            Ok(BLOCK_STREAM_START) => {
                let task_id: JobTaskId = file.read_u32::<byteorder::BigEndian>()?.into();
                let instance_id: InstanceId = file.read_u32::<byteorder::BigEndian>()?.into();
                Ok(Some(Block::StreamStart {
                    task_id,
                    instance_id,
                }))
            }
            Ok(BLOCK_STREAM_CHUNK) => {
                // Job task stream data
                let task_id: JobTaskId = file.read_u32::<byteorder::BigEndian>()?.into();
                let instance_id: InstanceId = file.read_u32::<byteorder::BigEndian>()?.into();
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
                let task_id: JobTaskId = file.read_u32::<byteorder::BigEndian>()?.into();
                let instance_id: InstanceId = file.read_u32::<byteorder::BigEndian>()?.into();
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

        let mut has_content = Set::new();

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
                        let color = colors[task_id.as_num() as usize % colors.len()];
                        let header = format!("{task_id:0id_width$}:{channel_id}>",);
                        write!(
                            stdout_buf,
                            "{} {}",
                            header.on_color(color),
                            String::from_utf8_lossy(&buffer)
                        )?;
                        has_content.insert(task_id);
                    } else {
                        self.file.seek_relative(size as i64)?;
                    }
                }
                Some(Block::StreamEnd {
                    task_id,
                    instance_id,
                }) => {
                    if *active_instances.get(&task_id).unwrap() == instance_id {
                        if !opts.show_empty && !has_content.contains(&task_id) {
                            continue;
                        }
                        let color = colors[task_id.as_num() as usize % colors.len()];
                        writeln!(
                            stdout_buf,
                            "{}",
                            format!("{task_id:0id_width$}: > stream closed").on_color(color)
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
