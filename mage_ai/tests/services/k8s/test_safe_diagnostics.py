"""
Kubernetes errors and DEBUG logs leave out response headers, bodies and values: a Job's or a
workload's body holds its environment, credentials included.
"""
import io
import json
import logging
import unittest

from kubernetes.client.exceptions import ApiException

from mage_ai.services.k8s.safe_diagnostics import RESPONSE_LOGGER, protect
from mage_ai.shared.logger import set_logging_format

SECRET = 'hunter2-not-a-real-password'


def rejected_job() -> ApiException:
    error = ApiException(status=422, reason='Unprocessable Entity')
    error.headers = {'Authorization': f'Bearer {SECRET}'}
    error.body = json.dumps(dict(
        kind='Status',
        reason='Invalid',
        message=f'Job "train" is invalid: env[0].value: Invalid value: "{SECRET}"',
        details=dict(
            kind='Job',
            name='train',
            causes=[dict(reason='FieldValueInvalid', field='spec.template.env[0].value')],
        ),
    ))
    return error


class SafeDiagnosticsTest(unittest.TestCase):
    def setUp(self):
        root = logging.getLogger()
        self.addCleanup(root.setLevel, root.level)
        self.addCleanup(setattr, root, 'handlers', list(root.handlers))
        protect()

    def test_an_api_error_names_status_reason_and_fields_but_no_values(self):
        error = rejected_job()

        text = str(error)

        self.assertNotIn(SECRET, text)
        self.assertNotIn('Authorization', text)
        self.assertIn('422', text)
        self.assertIn('Invalid', text)
        self.assertIn('Job train', text)
        self.assertIn('FieldValueInvalid spec.template.env[0].value', text)
        # Code that reads the error keeps everything.
        self.assertEqual(error.status, 422)
        self.assertIn(SECRET, error.body)

    def test_a_logged_traceback_holds_no_secret(self):
        stream = io.StringIO()
        logger = logging.getLogger('mage_ai.tests.k8s')
        handler = logging.StreamHandler(stream)
        logger.addHandler(handler)
        self.addCleanup(logger.removeHandler, handler)
        try:
            raise rejected_job()
        except ApiException:
            logger.exception('Job failed')

        self.assertIn('Job failed', stream.getvalue())
        self.assertNotIn(SECRET, stream.getvalue())

    def test_debug_verbosity_does_not_log_response_bodies(self):
        set_logging_format(level='DEBUG')
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logging.getLogger().addHandler(handler)
        self.addCleanup(logging.getLogger().removeHandler, handler)

        logging.getLogger(RESPONSE_LOGGER).debug('response body: %s', SECRET)
        logging.getLogger('mage_ai.tests.k8s').debug('other debug lines still show')

        self.assertNotIn(SECRET, stream.getvalue())
        self.assertIn('other debug lines still show', stream.getvalue())

    def test_a_body_that_is_not_json_is_left_out(self):
        error = ApiException(status=500, reason='Internal Server Error')
        error.body = f'<html>{SECRET}</html>'

        self.assertEqual(str(error), 'Kubernetes API error (500): Internal Server Error')
