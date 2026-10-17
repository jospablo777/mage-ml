import base64
import io

from mage_ai.shared.images import image_png_base64
from mage_ai.tests.base_test import TestCase


class ImageOutputTest(TestCase):
    def test_figures_and_images_become_png(self):
        """Images a block returned showed as text, not images (upstream #6073)."""
        import matplotlib

        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        figure, axes = plt.subplots()
        axes.plot([1, 2, 3])
        png = base64.b64decode(image_png_base64(figure))
        plt.close(figure)
        self.assertTrue(png.startswith(b'\x89PNG'))

        from PIL import Image

        png = base64.b64decode(image_png_base64(Image.new('RGB', (4, 4), 'red')))
        self.assertEqual(Image.open(io.BytesIO(png)).size, (4, 4))

    def test_image_bytes_pass_and_other_values_do_not(self):
        data = b'\x89PNG\r\n\x1a\n' + b'rest'
        self.assertEqual(base64.b64decode(image_png_base64(data)), data)
        self.assertIsNone(image_png_base64(b'plain bytes'))
        self.assertIsNone(image_png_base64({'a': 1}))
        self.assertIsNone(image_png_base64('text'))
