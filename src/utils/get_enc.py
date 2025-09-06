import os
import base64
from cryptography.fernet import Fernet
from pathlib import Path

class EncEnv:
    def __init__(self):
        self.env_vars = {}
        self.root_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
        self.enc_path = os.path.join(self.root_dir, '.env.enc')
        self.key_path = os.path.join(self.root_dir, '.env.key')

    def fileExists(self):
        """Check if both .env.enc and .env.key files exist"""
        return os.path.isfile(self.enc_path) and os.path.isfile(self.key_path)

    def fetch(self):
        """Decrypt and read the .env.enc file, returning a dictionary of all variables"""
        if not self.fileExists():
            return {}
        
        try:
            # Read the encryption key
            with open(self.key_path, 'rb') as key_file:
                key = key_file.read()
            
            # Initialize the Fernet cipher with the key
            cipher = Fernet(key)
            
            # Read and decrypt the encrypted env file
            with open(self.enc_path, 'rb') as enc_file:
                encrypted_data = enc_file.read()
                decrypted_data = cipher.decrypt(encrypted_data).decode('utf-8')
            
            # Parse the decrypted content into a dictionary
            for line in decrypted_data.splitlines():
                line = line.strip()
                # Skip empty lines and comments
                if not line or line.startswith('#'):
                    continue
                
                # Split by the first equals sign
                if '=' in line:
                    key, value = line.split('=', 1)
                    self.env_vars[key.strip()] = value.strip()
            
            return self.env_vars
        except Exception as e:
            print(f"Error decrypting environment file: {e}")
            return {}
    
    def get(self, key, default=None):
        """Get a specific environment variable by key with optional default value"""
        # HARDCODED FLAG: Use original .env file directly for easier testing
        use_original_env = True
        
        # Clear any previously loaded env vars to force reload
        self.env_vars = {}
        
        if use_original_env:
            # Use dotenv to load from the original .env file
            from dotenv import dotenv_values
            env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), '.env')
            # print(f"Loading environment from original .env file at: {env_path}")
            self.env_vars = dotenv_values(env_path)
            # print(f"Loaded {len(self.env_vars)} environment variables from .env")
        else:
            # Use the encrypted version
            # print("Using encrypted environment file")
            self.env_vars = self.fetch()
        
        # Debug: Print the requested key and value (masking sensitive values)
        value = self.env_vars.get(key, default)
        masked_value = "*****" if key.lower() in ["api_key", "password", "secret"] else value
        # print(f"Env variable {key} = {masked_value}")
        
        return value