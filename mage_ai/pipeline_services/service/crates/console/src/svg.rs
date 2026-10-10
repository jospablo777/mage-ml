use std::fmt::Write;

use ratatui::{
    buffer::Buffer,
    style::{Color, Modifier},
};

pub fn render(buffer: &Buffer) -> String {
    let cell_width = 9_u32;
    let cell_height = 18_u32;
    let padding = 20_u32;
    let width = u32::from(buffer.area.width) * cell_width + padding * 2;
    let height = u32::from(buffer.area.height) * cell_height + padding * 2;
    let mut output = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\" role=\"img\" aria-labelledby=\"title description\">\n<title id=\"title\">MageML pipeline console</title>\n<desc id=\"description\">Rendered from the terminal test backend. Colors, selection and chart cells match the dashboard buffer.</desc>\n<rect width=\"{width}\" height=\"{height}\" rx=\"12\" fill=\"#18181c\"/>\n<g font-family=\"DejaVu Sans Mono, Menlo, monospace\" font-size=\"15\">\n"
    );
    let mut backgrounds = String::new();
    let mut glyphs = String::new();
    for row in 0..buffer.area.height {
        for column in 0..buffer.area.width {
            let cell = &buffer[(buffer.area.x + column, buffer.area.y + row)];
            let x = padding + u32::from(column) * cell_width;
            let y = padding + u32::from(row) * cell_height;
            let mut foreground = color(cell.fg, "#f9fafc");
            let mut background = color(cell.bg, "#18181c");
            if cell.modifier.contains(Modifier::REVERSED) {
                std::mem::swap(&mut foreground, &mut background);
            }
            if background != "#18181c" {
                writeln!(backgrounds, "<rect x=\"{x}\" y=\"{y}\" width=\"{cell_width}\" height=\"{cell_height}\" fill=\"{background}\"/>").unwrap();
            }
            if cell.symbol().trim().is_empty() || cell.modifier.contains(Modifier::HIDDEN) {
                continue;
            }
            let bold = if cell.modifier.contains(Modifier::BOLD) {
                "bold"
            } else {
                "normal"
            };
            let italic = if cell.modifier.contains(Modifier::ITALIC) {
                "italic"
            } else {
                "normal"
            };
            let opacity = if cell.modifier.contains(Modifier::DIM) {
                "0.65"
            } else {
                "1"
            };
            if let Some(shape) = cell_shape(cell.symbol(), x, y, cell_width, cell_height) {
                writeln!(glyphs, "<g fill=\"{foreground}\" color=\"{foreground}\" opacity=\"{opacity}\">{shape}</g>").unwrap();
                continue;
            }
            let decoration = match (
                cell.modifier.contains(Modifier::UNDERLINED),
                cell.modifier.contains(Modifier::CROSSED_OUT),
            ) {
                (true, true) => "underline line-through",
                (true, false) => "underline",
                (false, true) => "line-through",
                _ => "none",
            };
            let baseline = y + cell_height - 4;
            writeln!(glyphs,
                "<text x=\"{x}\" y=\"{baseline}\" fill=\"{foreground}\" font-weight=\"{bold}\" font-style=\"{italic}\" opacity=\"{opacity}\" text-decoration=\"{decoration}\">{}</text>",
                escape_xml(cell.symbol())
            ).unwrap();
        }
    }
    output.push_str(&backgrounds);
    output.push_str(&glyphs);
    output.push_str("</g>\n</svg>\n");
    output
}

fn cell_shape(symbol: &str, x: u32, y: u32, width: u32, height: u32) -> Option<String> {
    let mut characters = symbol.chars();
    let character = characters.next()?;
    if characters.next().is_some() {
        return None;
    }
    let x = f64::from(x);
    let y = f64::from(y);
    let width = f64::from(width);
    let height = f64::from(height);
    let right = x + width;
    let bottom = y + height;
    let middle_x = x + width / 2.0;
    let middle_y = y + height / 2.0;
    let rectangle = |left: f64, top: f64, width: f64, height: f64| {
        format!("<rect x=\"{left}\" y=\"{top}\" width=\"{width}\" height=\"{height}\"/>")
    };
    match character {
        '▁'..='█' => {
            let filled_height = f64::from(u32::from(character) - 0x2580) * height / 8.0;
            return Some(rectangle(x, bottom - filled_height, width, filled_height));
        }
        '▉'..='▏' => {
            let filled_width = f64::from(0x2590 - u32::from(character)) * width / 8.0;
            return Some(rectangle(x, y, filled_width, height));
        }
        '▀' => return Some(rectangle(x, y, width, height / 2.0)),
        '▐' => return Some(rectangle(middle_x, y, width / 2.0, height)),
        '▔' => return Some(rectangle(x, y, width, height / 8.0)),
        '▕' => return Some(rectangle(right - width / 8.0, y, width / 8.0, height)),
        '\u{2800}'..='\u{28ff}' => {
            let dots = u32::from(character) - 0x2800;
            let mut output = String::new();
            for (bit, column, row) in [
                (0, 0, 0),
                (1, 0, 1),
                (2, 0, 2),
                (3, 1, 0),
                (4, 1, 1),
                (5, 1, 2),
                (6, 0, 3),
                (7, 1, 3),
            ] {
                if dots & (1 << bit) != 0 {
                    let center_x = x + width * (0.25 + f64::from(column) / 2.0);
                    let center_y = y + height * (0.125 + f64::from(row) / 4.0);
                    write!(
                        output,
                        "<circle cx=\"{center_x}\" cy=\"{center_y}\" r=\"1\"/>"
                    )
                    .unwrap();
                }
            }
            return Some(output);
        }
        _ => {}
    }
    let path = match character {
        '─' => format!("M{x} {middle_y}H{right}"),
        '│' => format!("M{middle_x} {y}V{bottom}"),
        '┌' => format!("M{middle_x} {bottom}V{middle_y}H{right}"),
        '┐' => format!("M{x} {middle_y}H{middle_x}V{bottom}"),
        '└' => format!("M{middle_x} {y}V{middle_y}H{right}"),
        '┘' => format!("M{x} {middle_y}H{middle_x}V{y}"),
        '├' => format!("M{middle_x} {y}V{bottom}M{middle_x} {middle_y}H{right}"),
        '┤' => format!("M{middle_x} {y}V{bottom}M{x} {middle_y}H{middle_x}"),
        '┬' => format!("M{x} {middle_y}H{right}M{middle_x} {middle_y}V{bottom}"),
        '┴' => format!("M{x} {middle_y}H{right}M{middle_x} {y}V{middle_y}"),
        '┼' => format!("M{x} {middle_y}H{right}M{middle_x} {y}V{bottom}"),
        '╭' => format!(
            "M{middle_x} {bottom}V{}Q{middle_x} {middle_y} {} {middle_y}H{right}",
            middle_y + 3.0,
            middle_x + 3.0
        ),
        '╮' => format!(
            "M{x} {middle_y}H{}Q{middle_x} {middle_y} {middle_x} {}V{bottom}",
            middle_x - 3.0,
            middle_y + 3.0
        ),
        '╰' => format!(
            "M{middle_x} {y}V{}Q{middle_x} {middle_y} {} {middle_y}H{right}",
            middle_y - 3.0,
            middle_x + 3.0
        ),
        '╯' => format!(
            "M{x} {middle_y}H{}Q{middle_x} {middle_y} {middle_x} {}V{y}",
            middle_x - 3.0,
            middle_y - 3.0
        ),
        _ => return None,
    };
    Some(format!(
        "<path d=\"{path}\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"1\"/>"
    ))
}

fn escape_xml(value: &str) -> String {
    let mut escaped = String::new();
    for character in value.chars() {
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&apos;"),
            '\t'
            | '\n'
            | '\r'
            | '\u{20}'..='\u{d7ff}'
            | '\u{e000}'..='\u{fffd}'
            | '\u{10000}'..='\u{10ffff}' => escaped.push(character),
            _ => {}
        }
    }
    escaped
}

fn color(value: Color, fallback: &str) -> String {
    const ANSI: [&str; 16] = [
        "#000000", "#cd0000", "#00cd00", "#cdcd00", "#0000ee", "#cd00cd", "#00cdcd", "#e5e5e5",
        "#7f7f7f", "#ff0000", "#00ff00", "#ffff00", "#5c5cff", "#ff00ff", "#00ffff", "#ffffff",
    ];
    let index = match value {
        Color::Reset => return fallback.to_owned(),
        Color::Black => 0,
        Color::Red => 1,
        Color::Green => 2,
        Color::Yellow => 3,
        Color::Blue => 4,
        Color::Magenta => 5,
        Color::Cyan => 6,
        Color::Gray => 7,
        Color::DarkGray => 8,
        Color::LightRed => 9,
        Color::LightGreen => 10,
        Color::LightYellow => 11,
        Color::LightBlue => 12,
        Color::LightMagenta => 13,
        Color::LightCyan => 14,
        Color::White => 15,
        Color::Rgb(red, green, blue) => return format!("#{red:02x}{green:02x}{blue:02x}"),
        Color::Indexed(index) => index,
    };
    if index < 16 {
        return ANSI[usize::from(index)].to_owned();
    }
    if index >= 232 {
        let gray = 8 + (index - 232) * 10;
        return format!("#{gray:02x}{gray:02x}{gray:02x}");
    }
    let index = index - 16;
    let level = |component: u8| {
        if component == 0 {
            0
        } else {
            55 + component * 40
        }
    };
    let red = level(index / 36);
    let green = level((index % 36) / 6);
    let blue = level(index % 6);
    format!("#{red:02x}{green:02x}{blue:02x}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::layout::Rect;

    #[test]
    fn remote_text_cannot_create_svg_elements() {
        assert_eq!(
            escape_xml("<script>\"&'\u{ffff}\x1b"),
            "&lt;script&gt;&quot;&amp;&apos;"
        );
        let mut buffer = Buffer::empty(Rect::new(0, 0, 1, 1));
        buffer[(0, 0)].set_symbol("<script>");
        let rendered = render(&buffer);
        assert!(!rendered.contains("<script>"));
        assert!(rendered.contains("&lt;script&gt;"));
    }

    #[test]
    fn reversed_cells_preserve_foreground_and_background() {
        let mut buffer = Buffer::empty(Rect::new(0, 0, 1, 1));
        buffer[(0, 0)]
            .set_symbol("A")
            .set_fg(Color::Rgb(1, 2, 3))
            .set_bg(Color::Rgb(4, 5, 6));
        buffer[(0, 0)].modifier = Modifier::REVERSED | Modifier::BOLD;
        let rendered = render(&buffer);
        assert!(rendered.contains("height=\"18\" fill=\"#010203\""));
        assert!(rendered.contains("fill=\"#040506\" font-weight=\"bold\""));
    }

    #[test]
    fn indexed_palette_covers_cube_and_gray() {
        assert_eq!(color(Color::Indexed(16), ""), "#000000");
        assert_eq!(color(Color::Indexed(231), ""), "#ffffff");
        assert_eq!(color(Color::Indexed(255), ""), "#eeeeee");
    }

    #[test]
    fn chart_cells_preserve_braille_dot_positions_and_block_coverage() {
        let dots = cell_shape("⡁", 0, 0, 8, 16).unwrap();
        assert!(dots.contains("cx=\"2\" cy=\"2\""));
        assert!(dots.contains("cx=\"2\" cy=\"14\""));
        assert_eq!(dots.matches("<circle").count(), 2);
        assert_eq!(
            cell_shape("▄", 0, 0, 8, 16).unwrap(),
            "<rect x=\"0\" y=\"8\" width=\"8\" height=\"8\"/>"
        );
        assert_eq!(
            cell_shape("▏", 0, 0, 8, 16).unwrap(),
            "<rect x=\"0\" y=\"0\" width=\"1\" height=\"16\"/>"
        );
    }

    #[test]
    fn borders_reach_adjacent_cells_without_font_spacing() {
        let horizontal = cell_shape("─", 20, 20, 9, 18).unwrap();
        assert!(horizontal.contains("M20 29H29"));
        let vertical = cell_shape("│", 20, 20, 9, 18).unwrap();
        assert!(vertical.contains("M24.5 20V38"));
        assert!(
            cell_shape("╭", 20, 20, 9, 18)
                .unwrap()
                .contains("M24.5 38V32Q24.5 29 27.5 29H29")
        );
        assert!(cell_shape("A", 20, 20, 9, 18).is_none());
    }
}
