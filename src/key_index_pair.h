#pragma once

#include <cstdint>
#include <cstring>

template <uint32_t KeyLength, uint32_t ValueLength>
struct KeyValuePair {
    char key[KeyLength];
    char value[ValueLength];

    KeyValuePair() {
        
    }

    KeyValuePair(char *key, char *value) {
        std::memcpy(this->value, value, ValueLength);
        std::memcpy(this->key, key, KeyLength);
    }

    bool operator < (const KeyValuePair<KeyLength, ValueLength> &other) const {
        return memcmp(this->key, other.key, KeyLength) < 0;
    }
};