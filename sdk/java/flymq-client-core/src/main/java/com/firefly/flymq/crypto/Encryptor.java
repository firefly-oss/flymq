/*
 * Copyright (c) 2026 Firefly Software Solutions Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.firefly.flymq.crypto;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.HexFormat;

/**
 * AES-256-GCM Encryptor for FlyMQ message encryption.
 * 
 * <p>Provides authenticated encryption ensuring both confidentiality and integrity.
 * Compatible with the Go FlyMQ server implementation.</p>
 * 
 * <p>Data format: nonce (12 bytes) || ciphertext || auth tag (16 bytes)</p>
 * 
 * <h2>Example Usage:</h2>
 * <pre>{@code
 * Encryptor encryptor = Encryptor.fromHexKey("0".repeat(64)); // 32-byte key in hex
 * byte[] encrypted = encryptor.encrypt("Hello, FlyMQ!".getBytes());
 * byte[] decrypted = encryptor.decrypt(encrypted);
 * }</pre>
 * 
 * @author Firefly Software Solutions Inc.
 * @since 1.0.0
 */
public class Encryptor {
    
    /** AES-256 requires 32-byte (256-bit) key */
    public static final int KEY_SIZE = 32;
    
    /** GCM standard nonce size */
    public static final int NONCE_SIZE = 12;
    
    /** GCM authentication tag size in bits */
    public static final int TAG_SIZE_BITS = 128;
    
    /** GCM authentication tag size in bytes */
    public static final int TAG_SIZE = 16;
    
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    
    private final SecretKey secretKey;
    private final SecureRandom secureRandom;
    
    /**
     * Create an encryptor with raw key bytes.
     * 
     * @param key 32-byte (256-bit) encryption key
     * @throws CryptoException if key is not 32 bytes
     */
    public Encryptor(byte[] key) {
        if (key == null || key.length != KEY_SIZE) {
            throw new CryptoException(
                "Key must be " + KEY_SIZE + " bytes (256 bits), got " + 
                (key == null ? "null" : key.length)
            );
        }
        this.secretKey = new SecretKeySpec(key, "AES");
        this.secureRandom = new SecureRandom();
    }
    
    /**
     * Create an encryptor from a hex-encoded key.
     * 
     * @param hexKey 64-character hex string (32 bytes)
     * @return configured Encryptor instance
     * @throws CryptoException if hexKey is invalid
     */
    public static Encryptor fromHexKey(String hexKey) {
        if (hexKey == null || hexKey.length() != KEY_SIZE * 2) {
            throw new CryptoException(
                "Hex key must be " + (KEY_SIZE * 2) + " characters, got " +
                (hexKey == null ? "null" : hexKey.length())
            );
        }
        try {
            byte[] key = HexFormat.of().parseHex(hexKey);
            return new Encryptor(key);
        } catch (IllegalArgumentException e) {
            throw new CryptoException("Invalid hex key: " + e.getMessage(), e);
        }
    }
    
    /**
     * Encrypt data using AES-256-GCM.
     * 
     * @param plaintext data to encrypt
     * @return nonce || ciphertext || auth_tag
     * @throws CryptoException if encryption fails
     */
    public byte[] encrypt(byte[] plaintext) {
        try {
            // Generate random nonce
            byte[] nonce = new byte[NONCE_SIZE];
            secureRandom.nextBytes(nonce);
            
            // Initialize cipher
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_SIZE_BITS, nonce);
            cipher.init(Cipher.ENCRYPT_MODE, secretKey, gcmSpec);
            
            // Encrypt
            byte[] ciphertext = cipher.doFinal(plaintext);
            
            // Combine nonce + ciphertext (tag is appended by GCM)
            byte[] result = new byte[nonce.length + ciphertext.length];
            System.arraycopy(nonce, 0, result, 0, nonce.length);
            System.arraycopy(ciphertext, 0, result, nonce.length, ciphertext.length);
            
            return result;
        } catch (Exception e) {
            throw new CryptoException("Encryption failed: " + e.getMessage(), e);
        }
    }
    
    /**
     * Decrypt data that was encrypted with encrypt().
     * 
     * @param ciphertext data from encrypt() (nonce || ciphertext || tag)
     * @return decrypted plaintext
     * @throws CryptoException if decryption fails or data is tampered
     */
    public byte[] decrypt(byte[] ciphertext) {
        int minLength = NONCE_SIZE + TAG_SIZE;
        if (ciphertext == null || ciphertext.length < minLength) {
            throw new CryptoException(
                "Ciphertext too short (minimum " + minLength + " bytes)"
            );
        }
        
        try {
            // Extract nonce
            byte[] nonce = new byte[NONCE_SIZE];
            System.arraycopy(ciphertext, 0, nonce, 0, NONCE_SIZE);
            
            // Extract encrypted data (ciphertext + tag)
            byte[] encryptedData = new byte[ciphertext.length - NONCE_SIZE];
            System.arraycopy(ciphertext, NONCE_SIZE, encryptedData, 0, encryptedData.length);
            
            // Initialize cipher
            Cipher cipher = Cipher.getInstance(ALGORITHM);
            GCMParameterSpec gcmSpec = new GCMParameterSpec(TAG_SIZE_BITS, nonce);
            cipher.init(Cipher.DECRYPT_MODE, secretKey, gcmSpec);
            
            // Decrypt
            return cipher.doFinal(encryptedData);
        } catch (Exception e) {
            throw new CryptoException(
                "Decryption failed - data may be corrupted or tampered: " + e.getMessage(), 
                e
            );
        }
    }
    
    /**
     * Generate a cryptographically secure 256-bit key.
     * 
     * @return 64-character hex string representing the key
     */
    public static String generateKey() {
        byte[] key = new byte[KEY_SIZE];
        new SecureRandom().nextBytes(key);
        return HexFormat.of().formatHex(key);
    }
    
    /**
     * Validate a hex-encoded encryption key.
     * 
     * @param hexKey key to validate
     * @return true if valid, false otherwise
     */
    public static boolean validateKey(String hexKey) {
        if (hexKey == null || hexKey.length() != KEY_SIZE * 2) {
            return false;
        }
        try {
            HexFormat.of().parseHex(hexKey);
            return true;
        } catch (IllegalArgumentException e) {
            return false;
        }
    }
}
