import unittest
from in_n_out_clients import email_client as ec

class TestEmailClient(unittest.TestCase):

    def test_smtp_settings(self):
        for provider, settings in ec.SMTP_SETTINGS.items():
            assert 'server_address' in settings, f'Provider `{provider}` has no server_address'
            assert 'port' in settings, f'Provider `{provider}` has no port' 
            assert settings['port'], f'Provider `{provider}` has empty port definition'
if __name__ == '__main__':
    unittest.main()