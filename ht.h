#ifndef RECOGNIZER_SERVER_HT_H
#define RECOGNIZER_SERVER_HT_H

#define HASHTABLE_SIZE 16777216
#define ROW_SLOTS_MAX 256

typedef struct stats {
    uint32_t used_rows;
    uint32_t total_ah_slots;
    uint32_t total_th_slots;
    uint8_t max_ah_slots;
    uint8_t max_th_slots;
    uint8_t ah_slots_dist[ROW_SLOTS_MAX + 1];
    uint8_t th_slots_dist[ROW_SLOTS_MAX + 1];
} stats_t;

// 32
#pragma pack(push, 1)
typedef struct slot {
    uint32_t hash32;
} slot_t;
#pragma pack(pop)

typedef struct row {
    slot_t *slots;
    uint8_t ah_len;
    uint8_t th_len;
    uint8_t updated;
} row_t;

uint32_t ht_init();

uint32_t ht_save();

stats_t ht_stats();

slot_t *ht_get_slot(uint8_t type, uint64_t hash);

uint32_t ht_add_slot(uint8_t type, uint64_t hash);

#endif //RECOGNIZER_SERVER_HT_H
