/// Byte-level helpers shared across the moonjupyter packages.

/// Convert an Int (0..255) to a Byte.
pub fn u8(i : Int) -> Byte {
  i.to_byte()
}

/// Concatenate a list of byte strings.
pub fn concat(parts : Array[Bytes]) -> Bytes {
  let out : Array[Byte] = []
  for p in parts {
    for i = 0; i < p.length(); i = i + 1 {
      out.push(p[i])
    }
  }
  Bytes::from_array(out)
}

/// Copy a byte range [start, end) into a new Bytes.
/// If `end` exceeds the byte length the slice is truncated to EOF.
pub fn slice(b : Bytes, start : Int, end : Int) -> Bytes {
  let out : Array[Byte] = []
  let hi = if end < b.length() {
    end
  } else {
    b.length()
  }
  for i = start; i < hi; i = i + 1 {
    out.push(b[i])
  }
  Bytes::from_array(out)
}

/// Equality of two byte strings (length then element-wise short-circuit).
pub fn bytes_equal(a : Bytes, b : Bytes) -> Bool {
  if a.length() != b.length() {
    return false
  }
  for i = 0; i < a.length(); i = i + 1 {
    if a[i].to_int() != b[i].to_int() {
      return false
    }
  }
  true
}

/// Build a single-character string from a Unicode code point (BMP, no surrogate handling).
pub fn char_str(cp : Int) -> String {
  let c = Int::unsafe_to_char(cp)
  "\{c}"
}

/// UTF-8 encode a string.
pub fn utf8_encode(s : String) -> Bytes {
  let out : Array[Byte] = []
  let n = s.length()
  let mut i = 0
  while i < n {
    let u = s.code_unit_at(i).to_int()
    if u < 0x80 {
      out.push(u8(u))
      i = i + 1
    } else if u < 0x800 {
      out.push(u8(0xC0 | (u >> 6)))
      out.push(u8(0x80 | (u & 0x3F)))
      i = i + 1
    } else if u >= 0xD800 && u < 0xDC00 && i + 1 < n {
      let lo = s.code_unit_at(i + 1).to_int()
      if lo >= 0xDC00 && lo < 0xE000 {
        let cp = 0x10000 + ((u - 0xD800) << 10) + (lo - 0xDC00)
        out.push(u8(0xF0 | (cp >> 18)))
        out.push(u8(0x80 | ((cp >> 12) & 0x3F)))
        out.push(u8(0x80 | ((cp >> 6) & 0x3F)))
        out.push(u8(0x80 | (cp & 0x3F)))
        i = i + 2
      } else {
        // stray high surrogate without a trailing low surrogate -> U+FFFD
        out.push(u8(0xEF))
        out.push(u8(0xBF))
        out.push(u8(0xBD))
        i = i + 1
      }
    } else {
      out.push(u8(0xE0 | (u >> 12)))
      out.push(u8(0x80 | ((u >> 6) & 0x3F)))
      out.push(u8(0x80 | (u & 0x3F)))
      i = i + 1
    }
  }
  Bytes::from_array(out)
}

/// Decode UTF-8 bytes into a string. Invalid sequences become U+FFFD.
pub fn utf8_decode(b : Bytes) -> String {
  let mut s = ""
  let n = b.length()
  let mut i = 0
  while i < n {
    let c = b[i].to_int()
    let mut cp = 0xFFFD
    if c < 0x80 {
      cp = c
      i = i + 1
    } else if c >= 0xC0 && c < 0xE0 && i + 1 < n {
      cp = ((c & 0x1F) << 6) | (b[i + 1].to_int() & 0x3F)
      i = i + 2
    } else if c >= 0xE0 && c < 0xF0 && i + 2 < n {
      cp = ((c & 0x0F) << 12) | ((b[i + 1].to_int() & 0x3F) << 6) | (b[i + 2].to_int() & 0x3F)
      i = i + 3
    } else if c >= 0xF0 && i + 3 < n {
      cp = ((c & 0x07) << 18)
        | ((b[i + 1].to_int() & 0x3F) << 12)
        | ((b[i + 2].to_int() & 0x3F) << 6)
        | (b[i + 3].to_int() & 0x3F)
      i = i + 4
    } else {
      i = i + 1
    }
    if (cp >= 0xD800 && cp < 0xE000) || cp > 0x10FFFF {
      s = s + char_str(0xFFFD)
    } else {
      s = s + char_str(cp)
    }
  }
  s
}

/// Lowercase hex encoding.
pub fn hex_encode(b : Bytes) -> String {
  let digits = "0123456789abcdef"
  let mut s = ""
  for i = 0; i < b.length(); i = i + 1 {
    let v = b[i].to_int()
    let hi = digits.code_unit_at(v >> 4).to_int()
    let lo = digits.code_unit_at(v & 15).to_int()
    s = s + "\{Int::unsafe_to_char(hi)}\{Int::unsafe_to_char(lo)}"
  }
  s
}

test "utf8 roundtrip" {
  let s = "héllo wörld 你好 🥮"
  assert_eq!(utf8_decode(utf8_encode(s)), s)
}

test "hex" {
  assert_eq!(hex_encode(utf8_encode("ABC")), "414243")
}

test "concat and slice" {
  let a = utf8_encode("hello ")
  let b = utf8_encode("world")
  let joined = concat([a, b])
  assert_eq!(utf8_decode(joined), "hello world")
  assert_eq!(utf8_decode(slice(joined, 6, 11)), "world")
  assert_true!(bytes_equal(a, slice(joined, 0, 6)))
}
