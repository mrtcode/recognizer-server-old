
// Rabin-Karp rolling hash, based on https://github.com/zbackup/zbackup/blob/master/rolling_hash.hh

#include <stdint.h>
#include "rh.h"

void rh_reset(rh_state_t *state) {
    state->factor = 0;
    state->nextFactor = 1;
    state->value = 0;
}

void rh_rollin(rh_state_t *state, uint8_t c) {
    state->factor = state->nextFactor;
    state->nextFactor = (state->nextFactor << 8) + state->nextFactor; // nextFactor *= 257
    state->value = (state->value << 8) + state->value;
    state->value += c;
}

void rh_rotate(rh_state_t *state, uint8_t in, uint8_t out) {
    state->value -= (uint64_t) out * state->factor;
    state->value = (state->value << 8) + state->value; // value *= 257
    state->value += in;
}

uint64_t rh_digest(rh_state_t *state) {
    return state->value + state->nextFactor;
}
