#ifndef RECOGNIZER_SERVER_DB_H
#define RECOGNIZER_SERVER_DB_H

typedef struct shard {
    sqlite3 *sqlite;
    sqlite3 *sqlite_read;
    sqlite3_stmt *stmt_insert;
} shard_t;

typedef struct iterator {
    sqlite3_stmt *stmt1;
    sqlite3_stmt *stmt2;
} iterator_t;

int db_normal_mode_init(char *directory);

int db_indexing_mode_init(char *directory);

int db_indexing_mode_finish();

int db_close();


int db_fields_save();

int db_fhth_save();

int db_ahth_save();

int db_fields_insert(uint64_t hash, uint8_t *data, uint32_t data_len);

int db_fhth_insert(uint64_t fh, uint64_t th);

int db_ahth_insert(uint64_t ah, uint64_t th);

sqlite3_stmt *db_fhth_get_stmt(uint64_t fh);

uint64_t db_fhth_get_next_th(sqlite3_stmt *stmt);

sqlite3_stmt *db_ahth_get_stmt(uint64_t ah);

uint64_t db_ahth_get_next_th(sqlite3_stmt *stmt);

sqlite3_stmt *db_get_fields_stmt(uint64_t hash);

int db_get_next_field(sqlite3_stmt *stmt, uint8_t **data, uint32_t *data_len);

int db_ht_save(row_t *rows, uint32_t rows_len);

int db_ht_load(row_t *rows);

uint32_t db_fields_in_transaction();

uint32_t db_fhth_in_transaction();

uint32_t db_ahth_in_transaction();

#endif //RECOGNIZER_SERVER_DB_H
