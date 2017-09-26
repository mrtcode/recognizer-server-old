#ifndef RECOGNIZER_SERVER_RH_H
#define RECOGNIZER_SERVER_RH_H

typedef struct rh_state {
    uint64_t factor;
    uint64_t nextFactor;
    uint64_t value;
} rh_state_t;

void rh_reset(rh_state_t *state);

void rh_rollin(rh_state_t *state, uint8_t c);

void rh_rotate(rh_state_t *state, uint8_t in, uint8_t out);

uint64_t rh_digest(rh_state_t *state);

#endif //RECOGNIZER_SERVER_RH_H
