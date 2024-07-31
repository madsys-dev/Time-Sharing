pub const CXL_MESSAGE_READ:u16 = 0;
pub const CXL_MESSAGE_UPDATE:u16 = 1;


pub struct Message {
    m_type: u16,
    status: u16,
    value: u32,
    key: u64,
}

impl Message {
    pub fn new(m_type: u16, key: u64) -> Message {
        Message {
            m_type,
            status: 0,
            value: 0,
            key,
        }
    }
    pub fn set_value(&mut self, value: u32) {
        self.status = 1;
        self.value = value;
    }
    pub fn finish(&self) -> u16 {
        self.status
    }
}