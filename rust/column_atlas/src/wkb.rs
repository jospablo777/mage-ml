//! Well-known binary to well-known text, for the geometry columns of GeoParquet files
//! (GeoDataFrame outputs). Reads ISO WKB and the Z, M and SRID flags of EWKB: points,
//! lines, polygons, their multi forms and collections. Output stops at a length limit,
//! so a polygon with millions of vertices costs no more than a short one.

use std::fmt::Write;

const MAX_DEPTH: usize = 32;

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
    little: bool,
}

impl Reader<'_> {
    fn take<const N: usize>(&mut self) -> Option<[u8; N]> {
        let slice = self.bytes.get(self.at..self.at + N)?;
        self.at += N;
        slice.try_into().ok()
    }

    fn byte(&mut self) -> Option<u8> {
        self.take::<1>().map(|bytes| bytes[0])
    }

    fn u32(&mut self) -> Option<u32> {
        let bytes = self.take::<4>()?;
        Some(if self.little {
            u32::from_le_bytes(bytes)
        } else {
            u32::from_be_bytes(bytes)
        })
    }

    fn f64(&mut self) -> Option<f64> {
        let bytes = self.take::<8>()?;
        Some(if self.little {
            f64::from_le_bytes(bytes)
        } else {
            f64::from_be_bytes(bytes)
        })
    }
}

struct Writer {
    text: String,
    limit: usize,
}

impl Writer {
    fn full(&self) -> bool {
        self.text.len() > self.limit
    }

    fn push(&mut self, part: &str) {
        if !self.full() {
            self.text.push_str(part);
        }
    }

    fn number(&mut self, value: f64) {
        if !self.full() {
            let _ = write!(self.text, "{value}");
        }
    }
}

#[derive(Clone, Copy)]
struct Header {
    kind: u32,
    dimensions: usize,
    suffix: &'static str,
}

fn read_header(reader: &mut Reader<'_>) -> Option<Header> {
    let order = reader.byte()?;
    if order > 1 {
        return None;
    }
    reader.little = order == 1;
    let code = reader.u32()?;
    let mut z = code & 0x8000_0000 != 0;
    let mut m = code & 0x4000_0000 != 0;
    if code & 0x2000_0000 != 0 {
        reader.u32()?; // EWKB SRID
    }
    let base = code & 0x0FFF_FFFF;
    let kind = base % 1000;
    match base / 1000 {
        0 => {}
        1 => z = true,
        2 => m = true,
        3 => {
            z = true;
            m = true;
        }
        _ => return None,
    }
    let suffix = match (z, m) {
        (true, true) => " ZM",
        (true, false) => " Z",
        (false, true) => " M",
        (false, false) => "",
    };
    Some(Header {
        kind,
        dimensions: 2 + z as usize + m as usize,
        suffix,
    })
}

fn coordinates(reader: &mut Reader<'_>, writer: &mut Writer, dimensions: usize) -> Option<()> {
    for index in 0..dimensions {
        if index > 0 {
            writer.push(" ");
        }
        writer.number(reader.f64()?);
    }
    Some(())
}

fn points(reader: &mut Reader<'_>, writer: &mut Writer, dimensions: usize) -> Option<()> {
    let count = reader.u32()? as usize;
    if count == 0 {
        writer.push("EMPTY");
        return Some(());
    }
    writer.push("(");
    for index in 0..count {
        if writer.full() {
            return Some(());
        }
        if index > 0 {
            writer.push(", ");
        }
        coordinates(reader, writer, dimensions)?;
    }
    writer.push(")");
    Some(())
}

fn rings(reader: &mut Reader<'_>, writer: &mut Writer, dimensions: usize) -> Option<()> {
    let count = reader.u32()? as usize;
    if count == 0 {
        writer.push("EMPTY");
        return Some(());
    }
    writer.push("(");
    for index in 0..count {
        if writer.full() {
            return Some(());
        }
        if index > 0 {
            writer.push(", ");
        }
        points(reader, writer, dimensions)?;
    }
    writer.push(")");
    Some(())
}

/// The body of a geometry whose header was read: what follows its type name.
fn body(reader: &mut Reader<'_>, writer: &mut Writer, header: Header, depth: usize) -> Option<()> {
    if depth > MAX_DEPTH {
        return None;
    }
    match header.kind {
        1 => {
            let start = reader.at;
            let empty = (0..header.dimensions).all(|offset| {
                let mut probe = Reader {
                    bytes: reader.bytes,
                    at: start + offset * 8,
                    little: reader.little,
                };
                probe.f64().is_some_and(f64::is_nan)
            });
            if empty {
                reader.at = start + header.dimensions * 8;
                writer.push("EMPTY");
            } else {
                writer.push("(");
                coordinates(reader, writer, header.dimensions)?;
                writer.push(")");
            }
        }
        2 => points(reader, writer, header.dimensions)?,
        3 => rings(reader, writer, header.dimensions)?,
        4..=7 => {
            let count = reader.u32()? as usize;
            if count == 0 {
                writer.push("EMPTY");
                return Some(());
            }
            writer.push("(");
            for index in 0..count {
                if writer.full() {
                    return Some(());
                }
                if index > 0 {
                    writer.push(", ");
                }
                let inner = read_header(reader)?;
                if header.kind == 7 {
                    writer.push(name(inner.kind)?);
                    writer.push(inner.suffix);
                    writer.push(" ");
                }
                body(reader, writer, inner, depth + 1)?;
            }
            writer.push(")");
        }
        _ => return None,
    }
    Some(())
}

fn name(kind: u32) -> Option<&'static str> {
    Some(match kind {
        1 => "POINT",
        2 => "LINESTRING",
        3 => "POLYGON",
        4 => "MULTIPOINT",
        5 => "MULTILINESTRING",
        6 => "MULTIPOLYGON",
        7 => "GEOMETRYCOLLECTION",
        _ => return None,
    })
}

/// The WKT of a WKB geometry, cut after about `limit` bytes; None when the bytes are not
/// WKB.
pub fn to_wkt(bytes: &[u8], limit: usize) -> Option<String> {
    let mut reader = Reader {
        bytes,
        at: 0,
        little: true,
    };
    let header = read_header(&mut reader)?;
    let mut writer = Writer {
        text: String::new(),
        limit,
    };
    writer.push(name(header.kind)?);
    writer.push(header.suffix);
    writer.push(" ");
    body(&mut reader, &mut writer, header, 0)?;
    Some(writer.text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(x: f64, y: f64) -> Vec<u8> {
        let mut bytes = vec![1];
        bytes.extend(1u32.to_le_bytes());
        bytes.extend(x.to_le_bytes());
        bytes.extend(y.to_le_bytes());
        bytes
    }

    #[test]
    fn points_lines_and_polygons() {
        assert_eq!(
            to_wkt(&point(-84.09, 9.93), 256).unwrap(),
            "POINT (-84.09 9.93)"
        );

        // Big-endian line with two points.
        let mut line = vec![0];
        line.extend(2u32.to_be_bytes());
        line.extend(2u32.to_be_bytes());
        for value in [0.0f64, 1.0, 2.5, -3.0] {
            line.extend(value.to_be_bytes());
        }
        assert_eq!(to_wkt(&line, 256).unwrap(), "LINESTRING (0 1, 2.5 -3)");

        let mut polygon = vec![1];
        polygon.extend(3u32.to_le_bytes());
        polygon.extend(1u32.to_le_bytes());
        polygon.extend(4u32.to_le_bytes());
        for value in [0.0f64, 0.0, 1.0, 0.0, 1.0, 1.0, 0.0, 0.0] {
            polygon.extend(value.to_le_bytes());
        }
        assert_eq!(
            to_wkt(&polygon, 256).unwrap(),
            "POLYGON ((0 0, 1 0, 1 1, 0 0))"
        );
    }

    #[test]
    fn multi_collections_dimensions_and_empty() {
        let mut multi = vec![1];
        multi.extend(4u32.to_le_bytes());
        multi.extend(2u32.to_le_bytes());
        multi.extend(point(1.0, 2.0));
        multi.extend(point(3.0, 4.0));
        assert_eq!(to_wkt(&multi, 256).unwrap(), "MULTIPOINT ((1 2), (3 4))");

        let mut collection = vec![1];
        collection.extend(7u32.to_le_bytes());
        collection.extend(1u32.to_le_bytes());
        collection.extend(point(5.0, 6.0));
        assert_eq!(
            to_wkt(&collection, 256).unwrap(),
            "GEOMETRYCOLLECTION (POINT (5 6))"
        );

        // ISO Z point (1001) and EWKB Z point with an SRID.
        let mut z = vec![1];
        z.extend(1001u32.to_le_bytes());
        for value in [1.0f64, 2.0, 3.0] {
            z.extend(value.to_le_bytes());
        }
        assert_eq!(to_wkt(&z, 256).unwrap(), "POINT Z (1 2 3)");
        let mut ewkb = vec![1];
        ewkb.extend((1u32 | 0x8000_0000 | 0x2000_0000).to_le_bytes());
        ewkb.extend(4326u32.to_le_bytes());
        for value in [1.0f64, 2.0, 3.0] {
            ewkb.extend(value.to_le_bytes());
        }
        assert_eq!(to_wkt(&ewkb, 256).unwrap(), "POINT Z (1 2 3)");

        assert_eq!(
            to_wkt(&point(f64::NAN, f64::NAN), 256).unwrap(),
            "POINT EMPTY"
        );
        let mut empty_line = vec![1];
        empty_line.extend(2u32.to_le_bytes());
        empty_line.extend(0u32.to_le_bytes());
        assert_eq!(to_wkt(&empty_line, 256).unwrap(), "LINESTRING EMPTY");
    }

    #[test]
    fn long_geometries_stop_early_and_bad_bytes_fail() {
        let count = 1_000_000u32;
        let mut line = vec![1];
        line.extend(2u32.to_le_bytes());
        line.extend(count.to_le_bytes());
        for index in 0..count * 2 {
            line.extend((index as f64).to_le_bytes());
        }
        let text = to_wkt(&line, 256).unwrap();
        assert!(text.len() < 300, "{}", text.len());

        assert_eq!(to_wkt(&[], 256), None);
        assert_eq!(to_wkt(&[7, 1, 0, 0, 0], 256), None);
        assert_eq!(to_wkt(&point(1.0, 2.0)[..10], 256), None);
        let mut unknown = vec![1];
        unknown.extend(17u32.to_le_bytes());
        assert_eq!(to_wkt(&unknown, 256), None);
    }
}
