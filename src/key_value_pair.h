#pragma once

#include <cstdint>
#include <cstring>


template <uint32_t KeyLength, uint32_t ValueLength>
struct KeyValuePair {
    static constexpr uint32_t KEY_LENGTH = KeyLength;
    static constexpr uint32_t VALUE_LENGTH = ValueLength;

    char key[KeyLength];
    char value[ValueLength];

    KeyValuePair() {
        
    }

    static KeyValuePair<KeyLength, ValueLength> inf() {
        KeyValuePair<KeyLength, ValueLength> k;
        for (int i=0; i<KeyLength; i++) {
            k.key[i] = char(255);
        }
        return k;
    }

    static KeyValuePair<KeyLength, ValueLength> from_ptr(void *ptr) {
        KeyValuePair<KeyLength, ValueLength> k;
        std::memcpy(k.value, reinterpret_cast<char*>(ptr) + KeyLength, ValueLength);
        std::memcpy(k.key, ptr, KeyLength);
        return k;
    }

    bool operator < (const KeyValuePair<KeyLength, ValueLength> &other) const {
        return memcmp(this->key, other.key, KeyLength) < 0;
    }

    bool operator == (const KeyValuePair<KeyLength, ValueLength> &other) const {
        return memcmp(this->key, other.key, KeyLength) == 0;
    }

    void set_key(void *key) {
        std::memcpy(this->key, key, KeyLength);
    }

    void set_value(void *value) {
        std::memcpy(this->value, value, ValueLength);
    }
};


template <uint32_t ValueLength>
struct KeyValuePair<8, ValueLength> {
    static constexpr uint32_t KEY_LENGTH = 8;
    static constexpr uint32_t VALUE_LENGTH = ValueLength;
    uint64_t key;
    char value[ValueLength];

    KeyValuePair() {}

    static KeyValuePair<8, ValueLength> inf() {
        KeyValuePair<8, ValueLength> k;
        k.key = uint64_t(-1);
        return k;
    }

    static KeyValuePair<8, ValueLength> from_ptr(void *ptr) {
        KeyValuePair<8, ValueLength> k;
        std::memcpy(k.value, reinterpret_cast<char*>(ptr) + sizeof(uint64_t), ValueLength);
        k.key = *reinterpret_cast<uint64_t*>(ptr);
        return k;
    }

    bool operator < (const KeyValuePair<8, ValueLength> &other) const {
        return  __builtin_bswap64(this->key) < __builtin_bswap64(other.key);
    }

    bool operator == (const KeyValuePair<8, ValueLength> &other) const {
        return this->key == other.key;
    }

    void set_key(void *key) {
        this->key = *reinterpret_cast<uint64_t*>(key);
    }

    void set_value(void *value) {
        std::memcpy(this->value, value, ValueLength);
    }
};

template <>
struct KeyValuePair<8, 4> {
    static constexpr uint32_t KEY_LENGTH = 8;
    static constexpr uint32_t VALUE_LENGTH = 4;
    uint64_t key;

    uint32_t value;

    KeyValuePair() {}

    static KeyValuePair<8, 4> inf() {
        KeyValuePair<8, 4> k;
        k.key = uint64_t(-1);
        return k;
    }

    static KeyValuePair<8, 4> from_ptr(void *ptr) {
        KeyValuePair<8, 4> k;
        k.key = *reinterpret_cast<uint64_t*>(ptr);
        k.value = *reinterpret_cast<uint32_t*>((uint8_t*)ptr + sizeof(uint64_t));
        return k;
    }

    bool operator < (const KeyValuePair<8, 4> &other) const {
        return __builtin_bswap64(this->key) < __builtin_bswap64(other.key);
    }

    bool operator == (const KeyValuePair<8, 4> &other) const {
        return this->key == other.key;
    }

    void set_key(void *key) {
        this->key = *reinterpret_cast<uint64_t*>(key);
    }

    void set_value(void *value) {
        this->value = *reinterpret_cast<uint32_t*>(value);
    }
};

template <>
struct KeyValuePair<8, 0> {
    static constexpr uint32_t KEY_LENGTH = 8;
    static constexpr uint32_t VALUE_LENGTH = 0;
    uint64_t key;

    KeyValuePair() {}

    static KeyValuePair<8, 0> inf() {
        KeyValuePair<8, 0> k;
        k.key = uint64_t(-1);
        return k;
    }

    static KeyValuePair<8, 0> from_ptr(void *ptr) {
        KeyValuePair<8, 0> k;
        k.key = *reinterpret_cast<uint64_t*>(ptr);
        return k;
    }

    bool operator < (const KeyValuePair<8, 0> &other) const {
        return __builtin_bswap64(this->key) < __builtin_bswap64(other.key);
    }

    bool operator == (const KeyValuePair<8, 0> &other) const {
        return this->key == other.key;
    }

    void set_key(void *key) {
        this->key = *reinterpret_cast<uint64_t*>(key);
    }

    void set_value(void *value) {
    }
};