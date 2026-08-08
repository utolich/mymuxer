// pub struct Golomb;
//
// impl Golomb {
//     pub fn decode(data: &[u8], offset: usize, signed: bool) -> (usize, i64) {
//         let mut zero_bit_length: i64 = -1;
//         let mut b = 0u32;
//         while b == 0 {
//             zero_bit_length += 1;
//             b = Self::read_bits(data, offset + zero_bit_length as usize + 1, 1);
//         }
//         let read = Self::read_bits(
//             data,
//             offset + zero_bit_length as usize + 1,
//             zero_bit_length as usize,
//         );
//         let mut num = (1i64 << zero_bit_length) - 1 + read as i64;
//         if signed {
//             if num % 2 == 0 {
//                 num = -((num + 1) / 2);
//             } else {
//                 num = (num + 1) / 2;
//             }
//         }
//         (offset + zero_bit_length as usize * 2 + 1, num)
//     }
//
//     pub fn read_bits(data: &[u8], offset: usize, n: usize) -> u32 {
//         if n == 0 {
//             return 0;
//         }
//         let bytes = (offset + n + 7) / 8;
//         if let Some(slice) = data.get(..bytes) {
//             let shift_r_count = bytes * 8 - (offset + n);
//             let mut result: u64 = 0;
//             for (i, b) in slice.iter().rev().enumerate() {
//                 result |= (*b as u64) << (i * 8);
//             }
//             let mask = if n == 64 { u64::MAX } else { (1u64 << n) - 1 };
//             ((result >> shift_r_count) & mask) as u32
//         } else {
//             // Error
//             0
//         }
//     }
// }

pub struct BitReader<'a> {
    data: &'a [u8],
    pos: usize,     // позиція в бітах
    cache: u64,     // кеш 64 біти
    cache_bits: u8, // скільки біт у кеші
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            pos: 0,
            cache: 0,
            cache_bits: 0,
        }
    }

    #[inline(always)]
    fn refill(&mut self) {
        while self.cache_bits <= 56 {
            let byte_pos = self.pos >> 3;
            if let Some(&b) = self.data.get(byte_pos) {
                self.cache <<= 8;
                self.cache |= b as u64;
                self.cache_bits += 8;
                self.pos += 8;
            } else {
                break;
            }
        }
    }

    #[inline(always)]
    pub fn read_bits(&mut self, n: u8) -> u32 {
        if self.cache_bits < n {
            self.refill();
        }

        let shift = self.cache_bits - n;
        let val = (self.cache >> shift) & ((1u64 << n) - 1);

        self.cache_bits -= n;
        self.cache &= (1u64 << self.cache_bits) - 1;

        val as u32
    }

    #[inline(always)]
    pub fn read_bit(&mut self) -> u8 {
        self.read_bits(1) as u8
    }

    // ue(v)
    #[inline(always)]
    pub fn read_ue(&mut self) -> u32 {
        let mut leading_zero_bits = 0;

        while self.read_bit() == 0 {
            leading_zero_bits += 1;
        }

        if leading_zero_bits == 0 {
            return 0;
        }

        let suffix = self.read_bits(leading_zero_bits as u8);
        ((1 << leading_zero_bits) - 1) + suffix
    }

    // se(v)
    #[inline(always)]
    pub fn read_se(&mut self) -> i32 {
        let code_num = self.read_ue() as i32;

        if (code_num & 1) == 0 {
            -(code_num / 2)
        } else {
            (code_num + 1) / 2
        }
    }
}
