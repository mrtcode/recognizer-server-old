/*
 * A Bitap algorithm implementation adapted from G. Myers work.
 * Myers, Gene. "A fast bit-vector algorithm for approximate string
 * matching based on dynamic programming."
 * Journal of the ACM (JACM) 46.3 (1999): 395-415.
 *
 * Added unicode support
 */

#include <stdio.h>
#include <string.h>
#include <jemalloc/jemalloc.h>
#include "inttypes.h"
#include "fuzzysearch.h"

#define HT_SIZE 16384

typedef uint64_t WORD;

static int W = sizeof(WORD) * 8;

typedef struct {
    WORD P;
    WORD M;
    int V;
} Scell;

typedef struct row {
    uint8_t *slots;
    uint32_t slots_len;
} row_t;

typedef struct state {
    Scell *S;
    uint32_t seg;
    uint32_t rem;
    uint64_t *na;
    uint32_t *upattern;
    uint32_t upattern_len;
    uint32_t *utext;
    uint32_t utext_len;
    row_t htrows[HT_SIZE];
} state_t;

static WORD All = -1;
static WORD Ebit;

uint64_t *ht_add(row_t *htrows, uint32_t uchar, uint32_t seg) {
    row_t *htrow = htrows + (uchar & (HT_SIZE - 1));
    uint32_t slot_size = sizeof(int32_t) + (sizeof(uint64_t) * seg);

    for (int i = 0; i < htrow->slots_len; i++) {
        if (*((int32_t *) (htrow->slots + slot_size * i)) == uchar) {
            return 0;
        }
    }

    if (htrow->slots) {
        if (!(htrow->slots = realloc(htrow->slots, slot_size * (htrow->slots_len + 1)))) {
            return 0;
        }
    } else {
        if (!(htrow->slots = malloc(slot_size))) {
            return 0;
        }
    }

    *((int32_t *) (htrow->slots + slot_size * htrow->slots_len)) = uchar;
    htrow->slots_len++;
    return (uint64_t *) (htrow->slots + slot_size * (htrow->slots_len - 1) + 4);
}

uint64_t *ht_get(row_t *htrows, uint32_t uchar, uint32_t seg) {
    row_t *htrow = htrows + (uchar & (HT_SIZE - 1));
    uint32_t slot_size = sizeof(int32_t) + (sizeof(uint64_t) * seg);
    for (int i = 0; i < htrow->slots_len; i++) {
        if (*((int32_t *) (htrow->slots + slot_size * i)) == uchar) {
            return (uint64_t *) (htrow->slots + slot_size * i + 4);
        }
    }
    return 0;
}

void setup_search(state_t *state) {
    register WORD *b, bvc, one;
    register int p, i, k;

    state->seg = (state->upattern_len - 1) / W + 1;
    state->rem = state->seg * W - state->upattern_len;

    state->na = malloc(sizeof(WORD) * state->seg);

    uint32_t *a = state->upattern;

    do {
        b = ht_add(state->htrows, *a, state->seg);
        if (!b) continue;

        for (p = 0; p < state->upattern_len; p += W) {
            bvc = 0;
            one = 1;
            k = p + W;
            if (state->upattern_len < k) k = state->upattern_len;
            for (i = p; i < k; i++) {
                if (*a == state->upattern[i])
                    bvc |= one;

                one <<= 1;
            }
            k = p + W;
            while (i++ < k) {
                bvc |= one;
                one <<= 1;
            }

            *b++ = bvc;
        }
    } while (*++a);

    b = state->na;
    for (p = 0; p < state->upattern_len; p += W) {
        bvc = 0;
        one = 1;
        k = p + W;
        if (state->upattern_len < k) k = state->upattern_len;
        for (i = p; i < k; i++) {
            one <<= 1;
        }
        k = p + W;
        while (i++ < k) {
            bvc |= one;
            one <<= 1;
        }

        *b++ = bvc;
    }

    Ebit = (((WORD) 1) << (W - 1));
    state->S = (Scell *) malloc(sizeof(Scell) * state->seg);
}

int32_t search(state_t *state, int32_t dif) {
    int32_t num, i, base, diw, Cscore;
    Scell *s, *sd;
    WORD pc, mc;
    register WORD *e;
    register WORD P, M, U, X, Y;
    Scell *S, *SE;

    S = state->S;

    SE = S + (state->seg - 1);

    diw = dif + W;

    sd = S + (dif - 1) / W;
    for (s = S; s <= sd; s++) {
        s->P = All;
        s->M = 0;
        s->V = ((s - S) + 1) * W;
    }

    base = 1 - state->rem;

    num = state->utext_len;

    i = 0;
    if (sd == S) {
        P = S->P;
        M = S->M;
        Cscore = S->V;
        for (; i < num; i++) {
            e = ht_get(state->htrows, state->utext[i], state->seg);

            if (e)
                U = *e;
            else
                U = *state->na;

            X = (((U & P) + P) ^ P) | U;
            U |= M;

            Y = P;
            P = M | ~(X | Y);
            M = Y & X;

            if (P & Ebit)
                Cscore += 1;
            else if (M & Ebit)
                Cscore -= 1;

            Y = P << 1;
            P = (M << 1) | ~(U | Y);
            M = Y & U;

            if (Cscore <= dif)
                break;
        }

        S->P = P;
        S->M = M;
        S->V = Cscore;

        if (i < num) {
            if (sd == SE) {
                return base + i;
            }

            i += 1;
        }
    }

    for (; i < num; i++) {
        e = ht_get(state->htrows, state->utext[i], state->seg);
        if (!e) e = state->na;
        pc = mc = 0;
        s = S;
        while (s <= sd) {
            U = *e++;
            P = s->P;
            M = s->M;

            Y = U | mc;
            X = (((Y & P) + P) ^ P) | Y;
            U |= M;

            Y = P;
            P = M | ~(X | Y);
            M = Y & X;

            Y = (P << 1) | pc;
            s->P = (M << 1) | mc | ~(U | Y);
            s->M = Y & U;

            U = s->V;
            pc = mc = 0;
            if (P & Ebit) {
                pc = 1;
                s->V = U + 1;
            } else if (M & Ebit) {
                mc = 1;
                s->V = U - 1;
            }

            s += 1;
        }

        if (U == dif && (*e & 0x1 | mc) && s <= SE) {
            s->P = All;
            s->M = 0;
            if (pc == 1)
                s->M = 0x1;
            if (mc != 1)
                s->P <<= 1;
            s->V = U = diw - 1;
            sd = s;
        } else {
            U = sd->V;
            while (U > diw) {
                U = (--sd)->V;
            }
        }
        if (sd == SE && U <= dif) {
            return base + i;
        }
    }

    while (sd > S) {
        i = sd->V;
        P = sd->P;
        M = sd->M;

        Y = Ebit;
        for (X = 0; X < W; X++) {
            if (P & Y) {
                i -= 1;
                if (i <= dif) break;
            } else if (M & Y)
                i += 1;
            Y >>= 1;
        }
        if (i <= dif) break;
        sd -= 1;
    }

    if (sd == SE) {
        P = sd->P;
        M = sd->M;
        U = sd->V;
        for (i = 0; i < state->rem; i++) {
            if (P & Ebit)
                U -= 1;
            else if (M & Ebit)
                U += 1;
            P <<= 1;
            M <<= 1;
            if (U <= dif) {
                return base + i;
            }
        }
    }
    return -1;
}

int cleanup(state_t *state) {
    free(state->na);
    free(state->S);
    for (uint32_t i = 0; i < state->upattern_len; i++) {
        uint32_t uchar = state->upattern[i];
        row_t *htrow = state->htrows + (uchar & (HT_SIZE - 1));
        if (htrow->slots) {
            free(htrow->slots);
            htrow->slots = 0;
        }
    }
}

int fuzzysearch(uint32_t *upattern, uint32_t upattern_len,
                uint32_t *utext, uint32_t utext_len,
                uint32_t *match_offset, uint32_t *match_distance,
                uint32_t max_distance) {

    state_t state;
    memset(&state, 0, sizeof(state_t));

    if (max_distance > upattern_len) return 0;

    state.upattern = upattern;
    state.upattern_len = upattern_len;
    state.utext = utext;
    state.utext_len = utext_len;

    setup_search(&state);

    for (int i = 0; i <= max_distance; i++) {
        int32_t ret = search(&state, i);
        if (ret >= 0) {
            *match_offset = ret;
            *match_distance = i;
            cleanup(&state);
            return 1;
        }
    }

    cleanup(&state);
    return 0;
}
