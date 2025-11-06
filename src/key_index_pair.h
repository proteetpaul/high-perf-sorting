#pragma once

#include <cstdint>
#include <cstring>


template <uint32_t KeyLength, uint32_t ValueLength>
struct KeyValuePair {
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
};


template <uint32_t ValueLength>
struct KeyValuePair<8, ValueLength> {
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
        return this->key < other.key;
    }

    bool operator == (const KeyValuePair<8, ValueLength> &other) const {
        return this->key == other.key;
    }
};

template <>
struct KeyValuePair<8, 0> {
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
        return this->key < other.key;
    }

    bool operator == (const KeyValuePair<8, 0> &other) const {
        return this->key == other.key;
    }
};