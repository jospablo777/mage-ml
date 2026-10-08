import unittest

import bcrypt

from mage_ai.authentication.passwords import (
    BCRYPT_MAX_PASSWORD_BYTES,
    create_bcrypt_hash,
    verify_password,
)

# Cost 4 is the bcrypt minimum. generate_salt() uses 14, which takes about a second per hash.
FAST_SALT = bcrypt.gensalt(4)


class PasswordsTest(unittest.TestCase):
    def test_short_password_round_trip(self):
        password_hash = create_bcrypt_hash('correct horse', FAST_SALT)

        self.assertTrue(verify_password('correct horse', password_hash))
        self.assertFalse(verify_password('wrong horse', password_hash))

    def test_password_longer_than_72_bytes_verifies_against_its_own_hash(self):
        password = 'ñ' * 60
        self.assertEqual(len(password.encode()), 120)

        password_hash = create_bcrypt_hash(password, FAST_SALT)

        self.assertTrue(verify_password(password, password_hash))

    def test_password_longer_than_72_bytes_verifies_against_a_bcrypt_4_hash(self):
        # bcrypt 4 hashed the first 72 bytes of longer input without raising.
        password = 'ñ' * 60
        legacy_hash = bcrypt.hashpw(
            password.encode()[:BCRYPT_MAX_PASSWORD_BYTES],
            FAST_SALT,
        ).decode()

        self.assertTrue(verify_password(password, legacy_hash))

    def test_only_the_first_72_bytes_are_compared(self):
        prefix = 'a' * BCRYPT_MAX_PASSWORD_BYTES
        password_hash = create_bcrypt_hash(prefix + 'first', FAST_SALT)

        self.assertTrue(verify_password(prefix + 'second', password_hash))
        self.assertFalse(verify_password('b' + prefix[1:] + 'first', password_hash))


if __name__ == '__main__':
    unittest.main()
