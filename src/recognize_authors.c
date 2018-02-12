#include <stdio.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <jemalloc/jemalloc.h>
#include <stdlib.h>
#include <sqlite3.h>
#include <jansson.h>
#include <math.h>
#include <unicode/ustdio.h>
#include <unicode/ustring.h>
#include <unicode/unorm2.h>
#include <unicode/uregex.h>
#include "defines.h"
#include "doidata.h"
#include "text.h"
#include "recognize.h"
#include "log.h"
#include "word.h"
#include "journal.h"
#include "recognize_authors.h"
#include "recognize_title.h"

extern UNormalizer2 *unorm2;

uint32_t line_to_uchars(line_t *line, uchar_t *uchars, uint32_t *uchars_len) {

  *uchars_len = 0;

  for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
    word_t *word = line->words + word_i;
    uint8_t *text = word->text;

    uint32_t s, i = 0;
    UChar32 c;

    do {
      s = i;
      U8_NEXT(text, i, -1, c);

      if (!c) break;

      if (u_charType(c) == U_NON_SPACING_MARK && *uchars_len >= 1) {
        int32_t c_prev = uchars[*uchars_len - 1].c;
        int32_t c_combined = unorm2_composePair(unorm2, c_prev, c);
        // Not always spacing mark can be combined
        if (c_combined > 0) {
          uchars[*uchars_len - 1].c = c_combined;
        }
        continue;
      }

      if (i > 500) break;

      uchars[*uchars_len].word = word;
      uchars[*uchars_len].c = c;

      (*uchars_len)++;

    } while (1);

    if (word->space) {
      uchars[*uchars_len].word = word;
      uchars[*uchars_len].c = ' ';
      (*uchars_len)++;
    }

  }
  return 0;
}

int32_t get_word_type(uint8_t *name) {
  int32_t a = 0, b = 0, c = 0;

  uint8_t output_text[300];
  uint32_t output_text_len = 200;

  text_process(name, output_text, &output_text_len);
//
//
  uint64_t word_hash = text_hash64(output_text, output_text_len);
  word_get(word_hash, &a, &b, &c);
//log_debug("%u %u %u\n", a, b, c);
  if (b + c == 0) return -a;
  if (a == 0) return b + c;

  if (b + c > a) {
    return (b + c) / a;
  } else if (b + c < a)
    return -a / (b + c);
  else return 0;
  //log_debug("%s %u %u %u\n", name, a, b, c);
}

uint32_t is_conjunction(uint32_t *utext, uint32_t utext_len) {
  static int32_t con[10][32] = {
          {'a', 'n', 'd', 0},
          {'u', 'n', 'd', 0},
  };

  for (uint32_t j = 0; j < 2; j++) {
    int32_t *c2 = &con[j][0];
    uint32_t n = 0;
    uint8_t found = 0;
    for (uint32_t i = 0; i < utext_len; i++) {
      if (!*c2) {
        found = 1;
        break;
      }
      int32_t c = utext[i];
      if (u_tolower(c) != u_tolower(*c2)) {
        break;
      }

      c2++;
      n++;
    }

    if (n != utext_len) return 0;

    if (found) {
      return 1;
    }
  }

  return 0;
}


uint32_t extract_authors_from_line(uchar_t *ustr, uint32_t ustr_len, author_t *authors, uint32_t *authors_len) {
  UBool error = 0;

  int32_t names[4][128];
  uint32_t names_lens[4] = {0};
  uint8_t names_len = 0;
  double font_size = 0;
  double baseline = 0;

  for (uint32_t i = 0; i < ustr_len; i++) {
    uchar_t *uchar = &ustr[i];

//        u_printf("%c\n", uchar->c);
//
//        if (names_lens[names_len] == 3
//            && names[names_len][0] == 'B'
//            && names[names_len][1] == 'a'
//            && names[names_len][2] == 'r') {
//            log_debug("%c\n", uchar->c);
//        }

    if (!names_lens[names_len]) {

      if (names_len == 1) {
        authors[*authors_len].font_size = uchar->word->font_size;
        authors[*authors_len].font = uchar->word->font;
      }

      if (uchar->c == ' ') { // Skip all spaces before name
        continue;
      }

      if (font_size > 1 && fabs(uchar->word->font_size - font_size) > 1.0 &&
          fabs(uchar->word->baseline - baseline) > 1.0) {
        authors[*authors_len].ref = 1;
        goto end;
      }

      if (uchar->c != 0xe6 && uchar->c != 0xc6 && u_isUAlphabetic(uchar->c)) {
        names[names_len][names_lens[names_len]++] = uchar->c;

        if (!names_len) {
          font_size = ustr->word->font_size;
          baseline = ustr->word->baseline;
        }
      } else { // If symbol is not a letter
        goto end;
      }
    } else {


      if (fabs(uchar->word->font_size - font_size) > 1.0 && fabs(uchar->word->baseline - baseline) > 1.0 ||
          uchar->c == '*') {
        authors[*authors_len].ref = 1;
        goto end;
      } else if ((uchar->c != 0xe6 && uchar->c != 0xc6 && u_isUAlphabetic(uchar->c)) || u_hasBinaryProperty(uchar->c, UCHAR_HYPHEN)) {
        names[names_len][names_lens[names_len]++] = uchar->c;
      } else if (uchar->c == '.' || uchar->c == ' ') { // if names separator

        if (is_conjunction(names[names_len], names_lens[names_len])) {
          names_lens[names_len] = 0;
          continue;
        }

        if (!u_isUUppercase(names[names_len][0])) { // if name doesn't start with upper case letter
          names_lens[names_len] = 0;
          goto end;
        }

        names_len++; // increase names count
        if (names_len >= 4) {
          goto end;
        }
      } else {
        goto end;
      }
    }

    if (i < ustr_len - 1) continue;
    end:

    if (is_conjunction(names[names_len], names_lens[names_len])) {
      names_lens[names_len] = 0;
    }

    if (names_lens[names_len]) {
      names_len++;
    }
    if (names_len >= 2 && names_len <= 4) {

      if (names_lens[names_len - 1] < 2) return 0;

      authors[*authors_len].names_len = names_len;

      for (uint32_t a = 0; a < names_len; a++) {
        uint32_t cur_name_len = 0;
        for (uint32_t b = 0; b < names_lens[a]; b++) {
          int32_t c = names[a][b];
          U8_APPEND(authors[*authors_len].names[a], cur_name_len, 100, c, error);
          authors[*authors_len].names[a][cur_name_len] = 0;
          //log_debug("author name: %s\n", authors[*authors_len].names[a]);
          if (error) return 0;
        }
      }
      (*authors_len)++;
    } else if (names_len != 0) // If names len is less than 2 or more than 4 - something is wrong
      return 0;
    names_len = 0;
    memset(names_lens, 0, sizeof(names_lens));
  }

  return 1;
}

uint32_t get_authors2(line_block_t *line_block, uint8_t *authors_str, uint32_t authors_str_max_len) {
  uchar_t *ustr = malloc(sizeof(uchar_t) * 1000);
  author_t *authors = malloc(sizeof(author_t) * 1000);

  memset(authors, 0, sizeof(author_t) * 1000);

  *authors_str = 0;

  uint32_t confidence = 0;

  for (uint32_t line_i = 0; line_i < line_block->lines_len; line_i++) {
    line_t *line = line_block->lines[line_i];

    uint32_t ustr_len;
    line_to_uchars(line, ustr, &ustr_len);


//    printf("A:\n");
//    for (uint32_t m = 0; m < line->words_len; m++) {
//      printf("%s ", line->words[m].text);
//    }
//    printf("\n");


    uint32_t authors_len = 0;
    extract_authors_from_line(ustr, ustr_len, authors, &authors_len);
    if (!authors_len) goto end;

    for (uint32_t j = 0; j < authors_len; j++) {
      author_t *author = &authors[j];

      uint32_t negative = 0;
      for (uint32_t k = 0; k < author->names_len; k++) {
        log_debug("%s ", author->names[k]);

        // Skip initials
        if (strlen(author->names[k]) <= 1) continue;

        int32_t type = get_word_type(author->names[k]);
        if (type < 0) negative++;
        log_debug(" (%d) ", type);
      }

      uint32_t c=0;

      if (author->ref) c = 2;


      if(negative==0) c=2;

      if (authors_len == 1) {
        if(negative<author->names_len) c = 1;
      } else {
        if(!negative) c = 2;
      }

      if(c>confidence) confidence=c;

      uint32_t len = 0;

      for (uint32_t k = 0; k < author->names_len; k++) {
        len += strlen(author->names[k]) + 1;
      }

      if (len > authors_str_max_len - strlen(authors_str) - 1) {
        *authors_str = 0;
        goto end;
      }

      for (uint32_t k = 0; k < author->names_len; k++) {
        strcat(authors_str, author->names[k]);

        if (k < author->names_len - 2) {
          strcat(authors_str, " ");
        } else if (k == author->names_len - 2) {
          strcat(authors_str, "\t");
        } else if (k == author->names_len - 1) {
          strcat(authors_str, "\n");
        }
      }

      log_debug("%f %d\n", author->font_size, author->font);
    }
  }


  end:
  free(ustr);
  free(authors);

  return confidence;
}
