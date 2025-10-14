pub enum PublicKey<'a> {
  Compressed {
    bytes: &'a [u8; 33]
  },
  Uncompressed{
    bytes: &'a [u8; 65]
  },
}

impl<'a> TryFrom<&'a [u8; 33]> for PublicKey<'a> {
  type Error = anyhow::Error;
  fn try_from(bytes: &'a [u8; 33]) -> Result<Self, Self::Error> {
    if !matches!(bytes[0], 0x02 | 0x03) {
      return Err(anyhow::anyhow!("Invalid compressed public key prefix: {:#x}", bytes[0]));
    }
    Ok(PublicKey::Compressed{ bytes })
  }
}

impl<'a> TryFrom<&'a [u8; 65]> for PublicKey<'a> {
  type Error = anyhow::Error;
  fn try_from(bytes: &'a [u8; 65]) -> Result<Self, Self::Error> {
    if bytes[0] != 0x04 {
      return Err(anyhow::anyhow!("Invalid uncompressed public key prefix: {:#x}", bytes[0]));
    }
    Ok(PublicKey::Uncompressed{ bytes })
  }
}

impl<'a> TryFrom<&'a [u8]> for PublicKey<'a> {
  type Error = anyhow::Error;

  fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
    match bytes.len() {
      33 => Ok(PublicKey::Compressed{ bytes: bytes.try_into().unwrap() }),
      65 => Ok(PublicKey::Uncompressed{ bytes: bytes.try_into().unwrap() }),
      _ => Err(anyhow::anyhow!("Invalid public key length: {}", bytes.len())),
    }
  }
}

pub struct PublicKeyHash<'a> {
  pub bytes: &'a [u8; 20],
}

impl<'a> TryFrom<&'a [u8]> for PublicKeyHash<'a> {
  type Error = anyhow::Error;

  fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
    if bytes.len() != 20 {
      return Err(anyhow::anyhow!("Invalid public key hash length: {}", bytes.len()));
    }
    Ok(PublicKeyHash { bytes: bytes.try_into().unwrap() })
  }
}

pub enum StandardScript<'a> {
  P2PK { pubkey: PublicKey<'a> },
  P2PKH { pubkey_hash: PublicKeyHash<'a> },
}

impl<'a> TryFrom<&'a [u8]> for StandardScript<'a> {
  type Error = anyhow::Error;

  fn try_from(script: &'a [u8]) -> Result<Self, Self::Error> {
    let assembly = Assembly::try_from(script)?;
    StandardScript::try_from(&assembly)
  }
}

impl<'a> TryFrom<&Assembly<'a>> for StandardScript<'a> {
  type Error = anyhow::Error;

  fn try_from(assembly: &Assembly<'a>) -> Result<Self, Self::Error> {
    match assembly.operators[..] {

      [
        Operator::PUSHBYTES { bytes: pubkey },
        Operator::CHECKSIG,
      ] => Ok(
        StandardScript::P2PK { pubkey: PublicKey::try_from(pubkey)? }
      ),

      [
        Operator::DUP,
        Operator::HASH160,
        Operator::PUSHBYTES { bytes: pubkey_hash },
        Operator::EQUALVERIFY,
        Operator::CHECKSIG,
      ] => Ok(
        StandardScript::P2PKH { pubkey_hash: PublicKeyHash::try_from(pubkey_hash)? }
      ),

      _ => Err(anyhow::anyhow!("Unsupported script format")),
    }
  }
}

#[derive(Clone, Copy)]
pub enum Operator<'a> {
  DUP,
  HASH160,
  EQUALVERIFY,
  CHECKSIG,
  PUSHBYTES {
    bytes: &'a [u8],
  },
}

impl Operator<'_> {
  pub fn size(self) -> usize {
    match self {
      Operator::PUSHBYTES { bytes } => 1 + bytes.len(),
      _ => 1,
    }
  }

  pub fn write_to(self, buf: &mut Vec<u8>) {
    match self {
      Operator::DUP => buf.push(0x76),
      Operator::HASH160 => buf.push(0xa9),
      Operator::EQUALVERIFY => buf.push(0x88),
      Operator::CHECKSIG => buf.push(0xac),
      Operator::PUSHBYTES { bytes } => {
        let length = bytes.len();
        if length == 0 || length > 0x4b {
          panic!("Invalid push bytes length: {}", length);
        }
        buf.push(length as u8);
        buf.extend_from_slice(bytes);
      }
    }
  }
}

impl<'a> TryFrom<&'a [u8]> for Operator<'a> {
  type Error = anyhow::Error;

  fn try_from(script: &'a [u8]) -> Result<Self, Self::Error> {
    if script.is_empty() {
      return Err(anyhow::anyhow!("Empty script"));
    }
    let opcode = script[0];
    match opcode {
      0x76 => Ok(Operator::DUP),
      0xa9 => Ok(Operator::HASH160),
      0x88 => Ok(Operator::EQUALVERIFY),
      0xac => Ok(Operator::CHECKSIG),
      n @ 0x01..=0x4b => {
        let length = n as usize;
        if 1 + length > script.len() {
          return Err(anyhow::anyhow!("Push bytes length exceeds script length"));
        }
        let bytes = &script[1..1 + length];
        Ok(Operator::PUSHBYTES { bytes })
      },
      _ => Err(anyhow::anyhow!("Unsupported opcode: {:#x}", opcode)),
    }
  }
}

pub struct Assembly<'a> {
  pub operators: Vec<Operator<'a>>,
}

impl<'a> Assembly<'a> {
  pub fn size(&self) -> usize {
    self.operators.iter().copied().map(Operator::size).sum()
  }
}

impl<'a> TryFrom<&'a [u8]> for Assembly<'a> {
  type Error = anyhow::Error;

  fn try_from(mut script: &'a [u8]) -> Result<Assembly<'a>, Self::Error> {
    let mut operators = Vec::new();
    while !script.is_empty() {
      let op = Operator::try_from(script)?;
      script = &script[op.size()..];
      operators.push(op);
    }
    Ok(Assembly { operators })
  }
}

impl<'a> TryInto<Vec<u8>> for Assembly<'a> {
  type Error = anyhow::Error;
  fn try_into(self) -> Result<Vec<u8>, Self::Error> {
    let mut buf = Vec::with_capacity(self.size());
    for op in &self.operators {
      op.write_to(&mut buf);
    }
    Ok(buf)
  }
}
