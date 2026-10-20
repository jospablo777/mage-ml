"""
The Chroma client (mage-ml[chroma]) on an embedded, persistent Chroma: documents with their
own vectors are exported and found by a query vector, without downloading a model.
"""
import tempfile
import unittest

import pandas as pd

try:
    import chromadb  # noqa: F401
except ImportError:
    chromadb = None


@unittest.skipIf(chromadb is None, 'the chroma extra is not installed')
class ChromaTest(unittest.TestCase):
    def test_documents_with_their_vectors_are_found_by_a_query_vector(self):
        from mage_ai.io.chroma import Chroma

        with tempfile.TemporaryDirectory() as path:
            client = Chroma(collection='songs', path=path, verbose=False)
            client.export(pd.DataFrame({
                'id': [1, 2, 3],
                'text': ['calm piano', 'loud guitar', 'soft piano'],
                'meta': [{'genre': 'classical'}, {'genre': 'rock'}, {'genre': 'classical'}],
                'vector': [[1.0, 0.0], [0.0, 1.0], [0.9, 0.1]],
            }), document_column='text', id_column='id', metadata_column='meta',
                embedding_column='vector')

            found = client.load(n_results=2, query_embeddings=[[1.0, 0.0]])

        self.assertEqual(found['ids'].tolist(), ['1', '3'])
        self.assertEqual(found['documents'].tolist(), ['calm piano', 'soft piano'])
        self.assertEqual(found['metadatas'][0], {'genre': 'classical'})
