#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <math.h>
#include "defines.h"
#include "doidata.h"
#include "text.h"
#include "recognize.h"
#include "log.h"
#include "recognize_title.h"
#include "recognize_authors.h"

int print_line(line_t *line) {
    printf("%g %g %g %g ", line->x_min, line->x_max, line->y_min, line->y_max);
    for (int k = 0; k < line->words_len; k++) {
        word_t *word = &line->words[k];
        printf("%s", word->text);
        if (word->space) printf(" ");
    }
    printf("\n");
}

double get_line_max_font_size(line_t *line) {
    double max_font_size = 0;
    for (int k = 0; k < line->words_len; k++) {
        word_t *word = &line->words[k];
        if (word->font_size > max_font_size) max_font_size = word->font_size;
    }
    return max_font_size;
}


uint32_t
line_block_to_text(line_block_t *line_block, uint32_t from, uint8_t *text, uint32_t *text_len, uint32_t max_text_size) {
    *text_len = 0;
    for (uint32_t line_i = from; line_i < line_block->lines_len; line_i++) {
        line_t *line = line_block->lines[line_i];

        if (line_i != 0) {
            *(text + *text_len) = ' ';
            (*text_len)++;
        }

        for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
            word_t *word = line->words + word_i;

            if ((*text_len) + word->text_len >= max_text_size) return 0;
            memcpy(text + *text_len, word->text, word->text_len);
            (*text_len) += word->text_len;

            if (word->space) {
                *(text + *text_len) = ' ';
                (*text_len)++;
            }
        }
    }
    *(text + *text_len) = 0;
    return 1;
}

uint32_t get_line_dominating_font(line_t *line) {
    uint32_t fonts[100] = {0};
    uint32_t fonts_count[100] = {0};
    uint32_t fonts_len = 0;

    for (int k = 0; k < line->words_len; k++) {
        word_t *word = &line->words[k];

        uint32_t found = 0;
        for (uint32_t i = 0; i < fonts_len; i++) {
            if (fonts[i] == word->font) {
                fonts_count[i] += word->text_len;
                found = 1;
                break;
            }
        }
        if (!found) {
            fonts[fonts_len] = word->font;
            fonts_count[fonts_len] = word->text_len;
            fonts_len++;
        }
    }

    uint32_t dominating_font = 0;
    uint32_t dominating_font_count = 0;

    for (uint32_t i = 0; i < fonts_len; i++) {
        if (fonts_count[i] > dominating_font_count) {
            dominating_font = fonts[i];
            dominating_font_count = fonts_count[i];
        }
    }

    return dominating_font;
}

double get_line_dominating_font_size(line_t *line) {
    double font_sizes[100] = {0};
    uint32_t font_sizes_count[100] = {0};
    uint32_t font_sizes_len = 0;

    for (int k = 0; k < line->words_len; k++) {
        word_t *word = &line->words[k];

        uint32_t found = 0;
        for (uint32_t i = 0; i < font_sizes_len; i++) {
            if (font_sizes[i] == word->font_size) {
                font_sizes_count[i] += word->text_len;
                found = 1;
                break;
            }
        }
        if (!found) {
            font_sizes[font_sizes_len] = word->font_size;
            font_sizes_count[font_sizes_len] = word->text_len;
            font_sizes_len++;
        }
    }

    double dominating_font_size = 0;
    uint32_t dominating_font_size_count = 0;

    for (uint32_t i = 0; i < font_sizes_len; i++) {
        if (font_sizes_count[i] > dominating_font_size_count) {
            dominating_font_size = font_sizes[i];
            dominating_font_size_count = font_sizes_count[i];
        }
    }

    return dominating_font_size;
}

uint32_t is_line_upper(line_t *line) {
    uint32_t total_num = 0;
    uint32_t upper_num = 0;
    for (uint32_t word_i = 0; word_i < line->words_len; word_i++) {
        word_t *word = line->words + word_i;

        text_info_t text_info = text_get_info(word->text);

        total_num += text_info.lowercase + text_info.uppercase;
        upper_num += text_info.uppercase;
    }
    if (total_num == 0) return 0;
    if (upper_num * 100 / total_num >= 90) return 1;

    return 0;
}

//uint32_t is_line_centered(page_t *page, line_t *line) {
//  if (fabs((page->width - (line->x_max - line->x_min)) / 2 - line->x_min) < 3.0) {
//    return 1;
//  }
//
//  return 0;
//}

uint32_t line_fonts_equal(line_t *l1, line_t *l2) {
    uint32_t font1, font2;

    if (!l1->words_len || !l2->words_len) return 0;

    for (uint32_t i = 0; i < l1->words_len; i++) {
        word_t *word = &l1->words[i];
        if (i == 0) {
            font1 = word->font;
        } else {
            if (word->font != font1) {
                return 0;
            }
        }
    }

    for (uint32_t i = 0; i < l2->words_len; i++) {
        word_t *word = &l2->words[i];
        if (i == 0) {
            font2 = word->font;
        } else {
            if (word->font != font2) {
                return 0;
            }
        }
    }

    if (font1 == font2) return 1;

    return 2;
}

uint32_t allow_upper_nonupper(line_t *l1, line_t *l2) {

    for (uint32_t i = 0; i < l1->words_len; i++) {
        word_t *word1 = &l1->words[i];

        for (uint32_t j = 0; j < l2->words_len; j++) {
            word_t *word2 = &l2->words[j];

            if (word1->font == word2->font && word1->font_size == word2->font_size &&
                word1->y_max - word1->y_min == word2->y_max - word2->y_min)
                return 1;
        }
    }

    return 0;
}

uint32_t has_equal_font_word(line_t *l1, line_t *l2) {
    for (uint32_t i = 0; i < l1->words_len; i++) {
        word_t *word1 = &l1->words[i];

        for (uint32_t j = 0; j < l2->words_len; j++) {
            word_t *word2 = &l2->words[j];
            if (word1->font == word2->font && word1->font_size == word2->font_size) return 1;
        }
    }
    return 0;
}

int add_line(line_block_t *line_blocks, uint32_t *line_blocks_len, line_t *line, line_t *line2) {

    if (*line_blocks_len >= MAX_LINE_BLOCKS) return 0;

    double max_font_size = get_line_dominating_font_size(line);
    uint32_t line_dominating_font = get_line_dominating_font(line);
    uint8_t upper = is_line_upper(line);

    uint32_t n = *line_blocks_len;

    if (n > 0) {
        n--;
        line_block_t *tb = &line_blocks[n];

        // Space between lines must be more or less equal. But <sup> can increase that space
        uint8_t skip = 0;
        if (line2 && fabs(((line->y_min - tb->y_max) - (line2->y_min - line->y_max))) > tb->max_font_size / 3) skip = 1;

        uint32_t lfe = line_fonts_equal(line, tb->lines[tb->lines_len - 1]);

        double max_line_gap;

        if (tb->max_font_size <= 12.0 && !tb->upper) {
            max_line_gap = tb->max_font_size;
        } else {
            max_line_gap = tb->max_font_size * 2.5;
        }

        if (!skip &&
            (tb->upper == upper || (line->y_min - tb->y_max < tb->max_font_size &&
                                    allow_upper_nonupper(line, tb->lines[tb->lines_len - 1]))) &&
            ((lfe == 0 && (line_dominating_font == tb->dominating_font ||
                           (tb->max_font_size == max_font_size && line->y_min - tb->y_max < tb->max_font_size * 1))) ||
             lfe == 1) &&
            line->words[0].bold == tb->bold &&
            fabs(tb->max_font_size - max_font_size) <= 1.0 &&
            line->y_min - tb->y_max < max_line_gap &&
            ((line->x_min >= tb->x_min || fabs(line->x_min - tb->x_min) < 2.0) &&
             (line->x_max <= tb->x_max || fabs(line->x_max - tb->x_max) < 2.0) ||
             (line->x_min <= tb->x_min || fabs(line->x_min - tb->x_min) < 2.0) &&
             (line->x_max >= tb->x_max || fabs(line->x_max - tb->x_max) < 2.0))) {

            tb->lines[tb->lines_len++] = line;
            if (line->x_min < tb->x_min) tb->x_min = line->x_min;
            if (line->y_min < tb->y_min) tb->y_min = line->y_min;
            if (line->x_max > tb->x_max) tb->x_max = line->x_max;
            if (line->y_max > tb->y_max) tb->y_max = line->y_max;

            tb->char_len += line->char_len;

            return 1;
        }
    }

    line_blocks[*line_blocks_len].lines_len = 1;
    line_blocks[*line_blocks_len].lines[0] = line;
    line_blocks[*line_blocks_len].y_min = line->y_min;
    line_blocks[*line_blocks_len].y_max = line->y_max;
    line_blocks[*line_blocks_len].x_min = line->x_min;
    line_blocks[*line_blocks_len].x_max = line->x_max;
    line_blocks[*line_blocks_len].max_font_size = max_font_size;
    line_blocks[*line_blocks_len].bold = line->words[0].bold;
    line_blocks[*line_blocks_len].dominating_font = line_dominating_font;
    line_blocks[*line_blocks_len].upper = upper;
    line_blocks[*line_blocks_len].char_len = line->char_len;
    (*line_blocks_len)++;

    return 0;
}

uint32_t
get_line_blocks(page_t *page, line_block_t *line_blocks, uint32_t *line_blocks_len) {
    for (uint32_t flow_i = 0; flow_i < page->flows_len; flow_i++) {
        flow_t *flow = page->flows + flow_i;

        for (uint32_t block_i = 0; block_i < flow->blocks_len; block_i++) {
            block_t *block = flow->blocks + block_i;

            for (uint32_t line_i = 0; line_i < block->lines_len; line_i++) {
                line_t *line = block->lines + line_i;

                line_t *line2 = 0;

                if (line_i < block->lines_len - 1) line2 = block->lines + line_i + 1;

                add_line(line_blocks, line_blocks_len, line, line2);

                //print_line(line);

                //printf("\n\n");
            }
        }
    }
}

uint32_t print_block(line_block_t *gb) {
    for (int j = 0; j < gb->lines_len; j++) {
        line_t *line = gb->lines[j];
        printf("%g %g %d %d %g ", get_line_dominating_font_size(line), gb->max_font_size, gb->upper, gb->bold,
               line->y_min);
//          printf("%g %g  %g %g  %g %g ",gb->x_min, gb->x_max, line->x_min, line->x_max, line->y_min, line->y_max);
        for (int k = 0; k < line->words_len; k++) {
            word_t *word = &line->words[k];
            printf("%s", word->text);
            if (word->space) printf(" ");
        }
        printf("\n");
    }
}


uint32_t extract_additional_authors(line_block_t *line_blocks, uint32_t line_blocks_len, uint32_t title_line_block_i,
                                    uint8_t *authors_str) {

    uint32_t a1 = 0;
    uint32_t a2 = 0;

    uint8_t a1_str[10000] = {0};
    uint8_t a2_str[10000] = {0};

    line_block_t *first_lb = &line_blocks[title_line_block_i];

    // Horizontal authors
    for (uint32_t i = title_line_block_i; i < line_blocks_len; i++) {
        line_block_t *lb = &line_blocks[i];
        if (fabs(first_lb->y_min - lb->y_min) < 3.0) {
//      printf("found another author block:\n");
//      print_block(lb);
//      printf("\n\n");
            if (get_authors2(lb, a1_str + strlen(a1_str), sizeof(a1_str) - strlen(a1_str))) {
                a1++;
            } else {
                break;
            }
        }
    }

    // Authors from the same font blocks
    for (uint32_t i = title_line_block_i; i < line_blocks_len; i++) {
        line_block_t *lb = &line_blocks[i];
        if (first_lb->lines[0]->words[0].font == lb->lines[0]->words[0].font &&
            first_lb->lines[0]->words[0].font_size == lb->lines[0]->words[0].font_size) {
//      printf("found another author block:\n");
//      print_block(lb);
//      printf("\n\n");
            if (get_authors2(lb, a2_str + strlen(a2_str), sizeof(a2_str) - strlen(a2_str))) {
                a2++;
            } else {
                break;
            }
        }
    }

    if (a1 > a2) {
        strcpy(authors_str, a1_str);
    } else {
        strcpy(authors_str, a2_str);
    }

    //printf("more authors: %s\n", a2_str);
}

uint32_t extract_authors(line_block_t *line_blocks, uint32_t line_blocks_len, uint32_t title_line_block_i,
                         uint8_t *authors_str) {
    uint32_t ret = 0;

    line_block_t *alb;
    line_block_t *slb;
    line_block_t *tlb;

    uint32_t i = title_line_block_i;

    uint32_t a_i = 0;

    tlb = &line_blocks[i];

    uint32_t a1 = 0;
    uint32_t a2 = 0;
    uint32_t a3 = 0;

    uint8_t a1_str[10000] = {0};
    uint8_t a2_str[10000] = {0};
    uint8_t a3_str[10000] = {0};

    if (i + 1 < line_blocks_len) {
        alb = &line_blocks[i + 1];
        if (tlb->y_max < alb->y_min) {
            a1 = get_authors2(alb, a1_str, sizeof(a1_str));
        }
    }

    if (i + 2 < line_blocks_len) {
        alb = &line_blocks[i + 2];
        slb = &line_blocks[i + 1];

        uint32_t total_chars = 0;
        for (uint32_t k = 0; k < slb->lines_len; k++) {
            total_chars += slb->lines[k]->char_len;
        }

        if (total_chars < 300 && slb->y_min - tlb->y_max < (alb->y_min - slb->y_max) * 2 &&
            tlb->max_font_size >= slb->max_font_size && tlb->max_font_size >= alb->max_font_size &&
            tlb->y_max < slb->y_min && slb->y_max < alb->y_min) {
            a2 = get_authors2(alb, a2_str, sizeof(a2_str));
        }
    }

    if (i > 0) {
        alb = &line_blocks[i - 1];
        if (tlb->max_font_size >= alb->max_font_size && tlb->y_min > alb->y_max) {
            a3 = get_authors2(alb, a3_str, sizeof(a3_str));
        }
    }

    if (a1 > 0 && a1 >= a2 && a1 >= a3) {
        strcpy(authors_str, a1_str);
        ret = 1;
        a_i = i + 1;
    } else if (a2 > 0 && a2 >= a1 && a2 >= a3) {
        strcpy(authors_str, a2_str);
        ret = 1;
        a_i = i + 2;
    } else if (a3 > 0 && a3 >= a1 && a3 >= a2) {
        strcpy(authors_str, a3_str);
        ret = 1;
        a_i = i - 1;
    }

    if (ret)
        extract_additional_authors(line_blocks, line_blocks_len, a_i, authors_str);

//  printf("a1: %d\n%s\n\na2: %d\n%s\n\na3: %d\n%s\n\n", a1, a1_str, a2, a2_str, a3, a3_str);

    return ret;
}

uint32_t get_biggest_font_block(page_t *page, line_block_t *line_blocks, uint32_t line_blocks_len) {
    uint32_t blb_i = 0;
    double blb_font_size = 0;

    uint8_t t[5000];
    uint32_t t_len;

    for (uint32_t i = 0; i < line_blocks_len; i++) {
        line_block_t *lb = &line_blocks[i];
        line_block_to_text(lb, 0, t, &t_len, 500);
        if (get_alphabetic_percent(t) < 70) continue;
        if (lb->max_font_size > blb_font_size) {
            blb_i = i;
            blb_font_size = lb->max_font_size;
        }

    }
    return blb_i;
}

double get_average_font_size_threshold(page_t *page) {
    uint32_t max_font_size = 0;
    uint32_t min_font_size = 0;
    uint32_t additional = 3;
    for (uint32_t i = 0; i < page->fs_dist_len; i++) {
        uint32_t font_size = i;
        uint32_t font_count = page->fs_dist[i];
        if (font_count >= 5) max_font_size = font_size;
        if (font_count > 500) min_font_size = font_size;
    }

    double threshold_fold_size = max_font_size * 0.7;
//    if (threshold_fold_size < min_font_size + 3) threshold_fold_size = min_font_size + 1;
    threshold_fold_size = min_font_size+1;

    return threshold_fold_size;
}

typedef struct slb {
    uint32_t i;
    line_block_t *line_block;
} slb_t;

int compare_slb(const slb_t *b, const slb_t *a) {
    if (((slb_t *) a)->line_block->max_font_size == ((slb_t *) b)->line_block->max_font_size) return 0;
    return ((slb_t *) a)->line_block->max_font_size < ((slb_t *) b)->line_block->max_font_size ? -1 : 1;
}

uint32_t get_sorted_blocks_by_font_size(line_block_t *line_blocks, uint32_t line_blocks_len, slb_t *slbs) {
    for (uint32_t i = 0; i < line_blocks_len; i++) {
        slbs[i].i = i;
        slbs[i].line_block = &line_blocks[i];
    }
    qsort(slbs, line_blocks_len, sizeof(slb_t), compare_slb);
}

uint32_t is_visually_separated(line_block_t *line_blocks, uint32_t line_blocks_len, uint32_t title_line_block_i) {
    line_block_t *lb_prev = 0;

    if (title_line_block_i > 0) lb_prev = &line_blocks[title_line_block_i - 1];

    line_block_t *lb = &line_blocks[title_line_block_i];

    line_block_t *lb_next = 0;

    if (title_line_block_i + 1 < line_blocks_len) lb_next = &line_blocks[title_line_block_i + 1];

    uint8_t before = 1;
    uint8_t after = 1;

    if (lb_prev && lb->y_min - lb_prev->y_max < fmax(lb->max_font_size, lb_prev->max_font_size)) before = 0;

    if (lb_next && lb_next->y_min - lb->y_max < fmax(lb->max_font_size, lb_next->max_font_size)) after = 0;

    if (!before && !after) return 0;

    return 1;
}

uint32_t extract_title_author(page_t *page, uint8_t *title, uint8_t *authors_str) {

    line_block_t line_blocks[MAX_LINE_BLOCKS];
    uint32_t line_blocks_len = 0;
    get_line_blocks(page, line_blocks, &line_blocks_len);

    uint8_t t[5000];
    uint32_t t_len;

    uint32_t font_size_threshold = get_average_font_size_threshold(page);

    slb_t slbs[MAX_LINE_BLOCKS];
    get_sorted_blocks_by_font_size(line_blocks, line_blocks_len, slbs);

//  for (uint32_t i = 0; i < line_blocks_len; i++) {
//    line_block_t *tlb = &line_blocks[i];
//    print_block(tlb);
//    printf("\n\n");
//
//  }
//
//return 0;
    for (uint32_t i = 0; i < line_blocks_len; i++) {
        line_block_t *tlb = slbs[i].line_block;

//    print_block(tlb);
//    printf("\n\n");

        if (tlb->max_font_size < font_size_threshold) continue;

        line_block_to_text(tlb, 0, t, &t_len, 500);

        if (strlen(t) < 25 || strlen(t) > 400) continue;

        if (get_alphabetic_percent(t) < 60) continue;

        if (!tlb->upper && tlb->max_font_size < font_size_threshold && tlb->y_min>page->height/3) continue;

        if (extract_authors(line_blocks, line_blocks_len, slbs[i].i, authors_str)) {
            uint32_t len;
            line_block_to_text(tlb, 0, title, &len, 300);
            return 1;
        }
    }

    for (uint32_t i = 0; i < line_blocks_len; i++) {
        line_block_t *tlb = &line_blocks[i];
//    print_block(tlb);
//    printf("\n\n");
//    continue;
        if (!tlb->upper) continue;

//    print_block(tlb);
//    printf("\n\n");

        line_block_to_text(tlb, 0, t, &t_len, 500);
        if (strlen(t) < 20 || strlen(t) > 400) continue;
        if (get_alphabetic_percent(t) < 60) continue;

        if(!is_visually_separated(line_blocks, line_blocks_len, i)) continue;

        if (extract_authors(line_blocks, line_blocks_len, i, authors_str)) {
            uint32_t len;
            line_block_to_text(tlb, 0, title, &len, 300);
            return 1;
        }
    }

//  for (uint32_t i = 0; i < line_blocks_len; i++) {
//    line_block_t *tlb = &line_blocks[i];
//
//    if (tlb->y_min > page->height / 4) continue;
//    line_block_to_text(tlb, 0, t, &t_len, 500);
//
//    if (strlen(t) < 20 || strlen(t) > 400) continue;
//    if (get_alphabetic_percent(t) < 70) continue;
//
//    if(extract_authors(line_blocks, line_blocks_len, i, authors_str)) {
//      uint32_t len;
//      line_block_to_text(tlb, 0, title, &len, 300);
//      return 1;
//    }
//  }

    return 0;
}

uint8_t find_author(uint8_t *text, uint32_t text_len, uint32_t author_hash, uint32_t author_len) {
    for (uint32_t i = 0; i < text_len - author_len; i++) {
        if (author_hash == text_hash32(text + i, author_len)) {
            return 1;
        }
    }
    return 0;
}

uint32_t get_doi_by_title(uint8_t *title, uint8_t *processed_text, uint32_t processed_text_len, uint8_t *doi) {
    uint8_t output_text[MAX_LOOKUP_TEXT_LEN];
    uint32_t output_text_len = MAX_LOOKUP_TEXT_LEN;
    text_process(title, output_text, &output_text_len);

    //printf("t: %s\n", output_text);

    uint64_t title_hash = text_hash64(output_text, output_text_len);
    //log_debug("lookup: %lu %.*s\n", title_hash, title_end-title_start+1, output_text+title_start);

    doidata_t doidatas[10];
    uint32_t dois_len;

    uint8_t res = doidata_get(title_hash, doidatas, &dois_len);

    if (res) {
        for (uint32_t j = 0; j < dois_len; j++) {
            doidata_t *doidata = &doidatas[j];
            uint8_t author1_found =
                    doidata->author1_len >= 4 && find_author(processed_text, processed_text_len, doidata->author1_hash,
                                                             doidata->author1_len);
            uint8_t author2_found =
                    doidata->author2_len >= 4 && find_author(processed_text, processed_text_len, doidata->author2_hash,
                                                             doidata->author2_len);

            if (author1_found || author2_found) {
                strcpy(doi, doidata->doi);
                log_debug("recognized by title: %s (%d, %d) %s\n", title, author1_found, author2_found, doi);
                return 1;
            }
        }
    }

    return 0;
}
