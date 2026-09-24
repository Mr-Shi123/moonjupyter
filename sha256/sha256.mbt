/// Pure MoonBit SHA-256 (FIPS 180-4) and HMAC-SHA256 (RFC 2104).

/// First 32 bits of the fractional parts of the cube roots of the first 64 primes.
let k_table : FixedArray[UInt] = FixedArray::from_array([
  0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U,
  0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U,
  0xd807aa98U, 0x12835b01U, 0x243185beU, 0x550c7dc3U,
  0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U,
  0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
  0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU,
  0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U,
  0xc6e00bf3U, 0xd5a79147U, 0x06ca6351U, 0x14292967U,
  0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U,
  0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
  0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U,
  0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U,
  0x19a4c116U, 0x1e376c08U, 0x2748774cU, 0x34b0bcb5U,
  0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U,
  0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
  0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
])

fn rotr(x : UInt, n : Int) -> UInt {
  if n == 0 {
    x
  } else {
    (x >> n) | (x << (32 - n))
  }
}

pub fn sha256(data : Bytes) -> Bytes {
  let h : FixedArray[UInt] = FixedArray::from_array([
    /// First 32 bits of the fractional parts of the square roots of the first 8 primes.
    0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU,
    0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U,
  ])
  let ml = data.length()
  let padded : Array[Byte] = []
  for i = 0; i < ml; i = i + 1 {
    padded.push(data[i])
  }
  padded.push(@bx.u8(0x80))
  while (padded.length() + 8) % 64 != 0 {
    padded.push(@bx.u8(0))
  }
  let bits = ml * 8
  for shift = 24; shift >= 0; shift = shift - 8 {
    padded.push(@bx.u8((bits >> shift) & 0xFF))
  }
  for block = 0; block < padded.length(); block = block + 64 {
    let w : FixedArray[UInt] = FixedArray::make(64, 0U)
    for i = 0; i < 16; i = i + 1 {
      let b0 = padded[block + i * 4].to_int()
      let b1 = padded[block + i * 4 + 1].to_int()
      let b2 = padded[block + i * 4 + 2].to_int()
      let b3 = padded[block + i * 4 + 3].to_int()
      w[i] = ((b0 << 24) | (b1 << 16) | (b2 << 8) | b3).reinterpret_as_uint()
    }
    for i = 16; i < 64; i = i + 1 {
      let s0 = rotr(w[i - 15], 7) ^ rotr(w[i - 15], 18) ^ (w[i - 15] >> 3)
      let s1 = rotr(w[i - 2], 17) ^ rotr(w[i - 2], 19) ^ (w[i - 2] >> 10)
      w[i] = w[i - 16] + s0 + w[i - 7] + s1
    }
    let mut a = h[0]
    let mut b = h[1]
    let mut c = h[2]
    let mut d = h[3]
    let mut e = h[4]
    let mut f = h[5]
    let mut g = h[6]
    let mut hh = h[7]
    for i = 0; i < 64; i = i + 1 {
      let s1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25)
      let ch = (e & f) ^ (g & (e ^ 0xFFFFFFFFU))
      let t1 = hh + s1 + ch + k_table[i] + w[i]
      let s0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22)
      let maj = (a & b) ^ (a & c) ^ (b & c)
      let t2 = s0 + maj
      hh = g
      g = f
      f = e
      e = d + t1
      d = c
      c = b
      b = a
      a = t1 + t2
    }
    h[0] = h[0] + a
    h[1] = h[1] + b
    h[2] = h[2] + c
    h[3] = h[3] + d
    h[4] = h[4] + e
    h[5] = h[5] + f
    h[6] = h[6] + g
    h[7] = h[7] + hh
  }
  let out : Array[Byte] = []
  for i = 0; i < 8; i = i + 1 {
    let v = h[i]
    out.push(@bx.u8((v >> 24).reinterpret_as_int() & 0xFF))
    out.push(@bx.u8((v >> 16).reinterpret_as_int() & 0xFF))
    out.push(@bx.u8((v >> 8).reinterpret_as_int() & 0xFF))
    out.push(@bx.u8(v.reinterpret_as_int() & 0xFF))
  }
  Bytes::from_array(out)
}

pub fn hmac_sha256(key : Bytes, msg : Bytes) -> Bytes {
  let k : Array[Byte] = []
  if key.length() > 64 {
    let d = sha256(key)
    for i = 0; i < d.length(); i = i + 1 {
      k.push(d[i])
    }
  } else {
    for i = 0; i < key.length(); i = i + 1 {
      k.push(key[i])
    }
  }
  while k.length() < 64 {
    k.push(@bx.u8(0))
  }
  let ipad : Array[Byte] = []
  let opad : Array[Byte] = []
  for i = 0; i < 64; i = i + 1 {
    let kb = k[i].to_int()
    ipad.push(@bx.u8(kb ^ 0x36))
    opad.push(@bx.u8(kb ^ 0x5C))
  }
  let inner : Array[Byte] = []
  for i = 0; i < 64; i = i + 1 {
    inner.push(ipad[i])
  }
  for i = 0; i < msg.length(); i = i + 1 {
    inner.push(msg[i])
  }
  let inner_hash = sha256(Bytes::from_array(inner))
  let outer : Array[Byte] = []
  for i = 0; i < 64; i = i + 1 {
    outer.push(opad[i])
  }
  for i = 0; i < inner_hash.length(); i = i + 1 {
    outer.push(inner_hash[i])
  }
  sha256(Bytes::from_array(outer))
}

test "sha256 vectors" {
  assert_eq(
    @bx.hex_encode(sha256(b"")),
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  )
  assert_eq(
    @bx.hex_encode(sha256(b"abc")),
    "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
  )
}

test "hmac rfc4231 case 2" {
  let sig = hmac_sha256(@bx.utf8_encode("Jefe"), @bx.utf8_encode("what do ya want for nothing?"))
  assert_eq(
    @bx.hex_encode(sig),
    "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
  )
}
