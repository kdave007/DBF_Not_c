import os
import base64
from cryptography.fernet import Fernet
from pathlib import Path

def generate_key_file(key_path):
    """Generate a new encryption key and save it to a file"""
    key = Fernet.generate_key()
    with open(key_path, 'wb') as key_file:
        key_file.write(key)
    return key

def encrypt_env_file(env_path, enc_path, key_path=None):
    """
    Encrypt an .env file and save it as .env.enc
    
    Args:
        env_path: Path to the source .env file
        enc_path: Path where to save the encrypted .env.enc file
        key_path: Path where to save/read the key file. If None, a new key will be generated.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Read the original .env file
        with open(env_path, 'rb') as env_file:
            env_data = env_file.read()
        
        # Get or generate the key
        if key_path and os.path.exists(key_path):
            with open(key_path, 'rb') as key_file:
                key = key_file.read()
        else:
            key = generate_key_file(key_path)
        
        # Encrypt the data
        cipher = Fernet(key)
        encrypted_data = cipher.encrypt(env_data)
        
        # Save the encrypted data
        with open(enc_path, 'wb') as enc_file:
            enc_file.write(encrypted_data)
            
        return True
    except Exception as e:
        print(f"Error encrypting environment file: {e}")
        return False

if __name__ == "__main__":
    # Example usage
    root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(root_dir, '.env')
    enc_path = os.path.join(root_dir, '.env.enc')
    key_path = os.path.join(root_dir, '.env.key')
    
    if os.path.exists(env_path):
        success = encrypt_env_file(env_path, enc_path, key_path)
        if success:
            print(f"Environment file encrypted successfully.")
            print(f"Key saved to: {key_path}")
            print(f"Encrypted file saved to: {enc_path}")
        else:
            print("Failed to encrypt environment file.")
    else:
        print(f"Environment file not found at: {env_path}")
