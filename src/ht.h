#ifndef RECOGNIZER_SERVER_HT_H
#define RECOGNIZER_SERVER_HT_H

#define HASHTABLE_SIZE 16777216
#define ROW_SLOTS_MAX 512

typedef struct stats {
    uint32_t used_rows;
    uint32_t total_titles;
} stats_t;

#pragma pack(push, 1)
typedef struct slot {
    uint32_t title_hash32;
    uint8_t title_hash8;
    uint8_t a1_len;
    uint8_t a2_len;
    uint32_t a1_hash;
    uint32_t a2_hash;
    uint32_t doi_id;
} slot_t;
#pragma pack(pop)

typedef struct row {
    slot_t *slots;
    uint32_t len;
    uint8_t updated;
} row_t;

uint32_t ht_init();

uint32_t ht_save();

stats_t ht_stats();

slot_t *ht_get_slots(uint64_t title_hash, slot_t **slots, uint32_t *slots_len);

uint32_t ht_add_slot(uint64_t title_hash, uint8_t a1_len, uint8_t a2_len, uint32_t a1_hash, uint32_t a2_hash);


#endif //RECOGNIZER_SERVER_HT_H
