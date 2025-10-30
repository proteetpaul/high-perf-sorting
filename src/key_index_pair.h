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

    KeyValuePair(char *key, char *value) {
        std::memcpy(this->value, value, ValueLength);
        std::memcpy(this->key, key, KeyLength);
    }

    bool operator < (const KeyValuePair<KeyLength, ValueLength> &other) const {
        return memcmp(this->key, other.key, KeyLength) < 0;
    }

    bool operator == (const KeyValuePair<KeyLength, ValueLength> &other) const {
        return memcmp(this->key, other.key, KeyLength) == 0;
    }
};