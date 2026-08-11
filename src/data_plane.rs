use bytes::Bytes;

pub const DATA_PROTOCOL_VERSION: u8 = 1;
pub const DATA_FRAME_HEADER_BYTES: usize = 16;
pub const MAX_DATA_PAYLOAD_BYTES: usize = 64 * 1024;
pub const MAX_DATA_FRAME_BYTES: usize = DATA_FRAME_HEADER_BYTES + MAX_DATA_PAYLOAD_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataFrameKind {
    Data = 1,
    End = 2,
    Error = 3,
    WindowUpdate = 4,
    Resize = 5,
    Signal = 6,
}

impl TryFrom<u8> for DataFrameKind {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, String> {
        match value {
            1 => Ok(Self::Data),
            2 => Ok(Self::End),
            3 => Ok(Self::Error),
            4 => Ok(Self::WindowUpdate),
            5 => Ok(Self::Resize),
            6 => Ok(Self::Signal),
            _ => Err(format!("unknown data frame type {value}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataChannel {
    Stdout = 1,
    Stderr = 2,
    Stdin = 3,
    Console = 4,
    System = 5,
}

impl TryFrom<u8> for DataChannel {
    type Error = String;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Stdout),
            2 => Ok(Self::Stderr),
            3 => Ok(Self::Stdin),
            4 => Ok(Self::Console),
            5 => Ok(Self::System),
            _ => Err(format!("unknown data frame channel {value}")),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataFrame {
    pub kind: DataFrameKind,
    pub channel: DataChannel,
    pub flags: u8,
    pub stream_id: u32,
    pub sequence: u64,
    pub payload: Bytes,
}

impl DataFrame {
    pub fn data(
        stream_id: u32,
        sequence: u64,
        channel: DataChannel,
        payload: impl Into<Bytes>,
    ) -> Self {
        Self {
            kind: DataFrameKind::Data,
            channel,
            flags: 0,
            stream_id,
            sequence,
            payload: payload.into(),
        }
    }

    pub fn end(stream_id: u32, sequence: u64) -> Self {
        Self {
            kind: DataFrameKind::End,
            channel: DataChannel::System,
            flags: 0,
            stream_id,
            sequence,
            payload: Bytes::new(),
        }
    }

    pub fn error(stream_id: u32, sequence: u64, message: impl Into<String>) -> Self {
        Self {
            kind: DataFrameKind::Error,
            channel: DataChannel::System,
            flags: 0,
            stream_id,
            sequence,
            payload: Bytes::from(message.into()),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, String> {
        if self.payload.len() > MAX_DATA_PAYLOAD_BYTES {
            return Err(format!(
                "data frame payload is {} bytes; maximum is {MAX_DATA_PAYLOAD_BYTES}",
                self.payload.len()
            ));
        }
        let mut encoded = Vec::with_capacity(DATA_FRAME_HEADER_BYTES + self.payload.len());
        encoded.push(DATA_PROTOCOL_VERSION);
        encoded.push(self.kind as u8);
        encoded.push(self.channel as u8);
        encoded.push(self.flags);
        encoded.extend_from_slice(&self.stream_id.to_be_bytes());
        encoded.extend_from_slice(&self.sequence.to_be_bytes());
        encoded.extend_from_slice(&self.payload);
        Ok(encoded)
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, String> {
        if encoded.len() < DATA_FRAME_HEADER_BYTES {
            return Err("data frame is shorter than its 16-byte header".into());
        }
        if encoded.len() > MAX_DATA_FRAME_BYTES {
            return Err(format!(
                "data frame is {} bytes; maximum is {MAX_DATA_FRAME_BYTES}",
                encoded.len()
            ));
        }
        if encoded[0] != DATA_PROTOCOL_VERSION {
            return Err(format!(
                "unsupported data protocol version {}; expected {DATA_PROTOCOL_VERSION}",
                encoded[0]
            ));
        }
        Ok(Self {
            kind: DataFrameKind::try_from(encoded[1])?,
            channel: DataChannel::try_from(encoded[2])?,
            flags: encoded[3],
            stream_id: u32::from_be_bytes(encoded[4..8].try_into().expect("four bytes")),
            sequence: u64::from_be_bytes(encoded[8..16].try_into().expect("eight bytes")),
            payload: Bytes::copy_from_slice(&encoded[DATA_FRAME_HEADER_BYTES..]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_preserves_arbitrary_binary_payload() {
        let payload = Bytes::from_static(&[0, 255, b'\n', 0, 128]);
        let frame = DataFrame::data(42, 7, DataChannel::Stdout, payload.clone());
        let decoded = DataFrame::decode(&frame.encode().unwrap()).unwrap();

        assert_eq!(decoded, frame);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn rejects_oversized_and_unknown_frames() {
        let oversized = DataFrame::data(
            1,
            0,
            DataChannel::Stdout,
            vec![0; MAX_DATA_PAYLOAD_BYTES + 1],
        );
        assert!(oversized.encode().is_err());

        let mut encoded = DataFrame::end(1, 0).encode().unwrap();
        encoded[1] = 255;
        assert!(DataFrame::decode(&encoded).is_err());
    }
}
