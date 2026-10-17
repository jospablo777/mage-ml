"""
Block outputs that are images, shown as images in the editor (upstream #6073): matplotlib
figures, PIL images and PNG or JPEG bytes. Other values give None. Libraries are used only
when the value comes from them, so nothing is imported for other outputs.
"""
import base64
import io
from typing import Any, Optional

PNG_SIGNATURE = b'\x89PNG\r\n\x1a\n'
JPEG_SIGNATURE = b'\xff\xd8\xff'


def image_png_base64(value: Any) -> Optional[str]:
    """The image as base64 PNG (JPEG bytes stay JPEG, which browsers show too), or None."""
    if isinstance(value, (bytes, bytearray, memoryview)):
        data = bytes(value)
        if data.startswith(PNG_SIGNATURE) or data.startswith(JPEG_SIGNATURE):
            return base64.b64encode(data).decode('ascii')
        return None
    module = type(value).__module__ or ''
    buffer = io.BytesIO()
    if module.startswith('matplotlib') and hasattr(value, 'savefig'):
        value.savefig(buffer, format='png', bbox_inches='tight')
    elif module.startswith('PIL') and hasattr(value, 'save') and hasattr(value, 'mode'):
        value.save(buffer, format='PNG')
    else:
        return None
    return base64.b64encode(buffer.getvalue()).decode('ascii')
