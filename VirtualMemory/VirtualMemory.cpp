#include "VirtualMemory.h"
#include "PhysicalMemory.h"
#include "MemoryConstants.h"
#include <algorithm>
#include <limits>

// Zero out a frame (used for tables)
static void clearTable(uint64_t frameIdx) {
    for (uint64_t i = 0; i < PAGE_SIZE; ++i)
        PMwrite(frameIdx * PAGE_SIZE + i, 0);
}

// Init: clear root table
void VMinitialize() { clearTable(0); }

// Shortest distance in page ring (for eviction)
static inline uint64_t cyclicDistance(uint64_t a, uint64_t b) {
    uint64_t diff = (a > b) ? (a - b) : (b - a);
    return std::min<uint64_t>(NUM_PAGES - diff, diff);
}

// Search page tables for: max used, empty table, or best page to evict
static void dfs(uint64_t frame, int depth, uint64_t virtPrefix, const uint64_t *forbid,
                uint64_t parentAddr, uint64_t targetPage,
                uint64_t &highestUsed, uint64_t &emptyTblFrame, uint64_t &emptyTblParent,
                uint64_t &bestLeafFrame, uint64_t &bestLeafPage,
                uint64_t &bestLeafParent, uint64_t &bestLeafDist)
{
    highestUsed = std::max(highestUsed, frame);
    bool forbiddenHere = false;
    for (int i = 0; i < TABLES_DEPTH && !forbiddenHere; ++i)
        forbiddenHere = forbid[i] == frame;

    if (depth == TABLES_DEPTH) { // leaf page
        if (!forbiddenHere) {
            uint64_t dist = cyclicDistance(virtPrefix, targetPage);
            if (dist > bestLeafDist || (dist == bestLeafDist && virtPrefix < bestLeafPage)) {
                bestLeafDist   = dist;
                bestLeafFrame  = frame;
                bestLeafPage   = virtPrefix;
                bestLeafParent = parentAddr;
            }
        }
        return;
    }

    bool tableEmpty = true;
    for (uint64_t i = 0; i < PAGE_SIZE; ++i) {
        word_t child;
        PMread(frame * PAGE_SIZE + i, &child);
        if (child == 0) continue;
        tableEmpty = false;
        uint64_t childPrefix = (virtPrefix << OFFSET_WIDTH) | i;
        dfs(child, depth + 1, childPrefix, forbid, frame * PAGE_SIZE + i, targetPage,
            highestUsed, emptyTblFrame, emptyTblParent,
            bestLeafFrame, bestLeafPage, bestLeafParent, bestLeafDist);
    }

    if (tableEmpty && !forbiddenHere && emptyTblFrame == 0) {
        emptyTblFrame  = frame;
        emptyTblParent = parentAddr;
    }
}

// Decide which frame to use (unused, empty table, or evict)
static uint64_t chooseFrame(uint64_t targetPage, const uint64_t *forbid, bool asTable)
{
    uint64_t highestUsed = 0, emptyTblFrame = 0, emptyTblParent = (uint64_t)-1;
    uint64_t bestLeafFrame = 0, bestLeafPage = 0, bestLeafParent = (uint64_t)-1, bestLeafDist = 0;
    dfs(0, 0, 0, forbid, static_cast<uint64_t>(-1), targetPage,
        highestUsed, emptyTblFrame, emptyTblParent,
        bestLeafFrame, bestLeafPage, bestLeafParent, bestLeafDist);

    if (highestUsed + 1 < NUM_FRAMES) {
        uint64_t newFrame = highestUsed + 1;
        if (asTable) clearTable(newFrame);
        return newFrame;
    }
    if (emptyTblFrame) {
        PMwrite(emptyTblParent, 0);
        if (asTable) clearTable(emptyTblFrame);
        return emptyTblFrame;
    }
    PMevict(bestLeafFrame, bestLeafPage);
    PMwrite(bestLeafParent, 0);
    if (asTable) clearTable(bestLeafFrame);
    return bestLeafFrame;
}

// Convert virtual address to physical address (walks/creates tables as needed)
static uint64_t toPhysical(uint64_t vaddr)
{
    if (TABLES_DEPTH == 0) return vaddr;
    uint64_t frame = 0, forbid[TABLES_DEPTH];
    for (int i = 0; i < TABLES_DEPTH; ++i) forbid[i] = std::numeric_limits<uint64_t>::max();
    forbid[0] = 0;

    uint64_t page_num = vaddr >> OFFSET_WIDTH;
    uint64_t total_page_num_bits = VIRTUAL_ADDRESS_WIDTH - OFFSET_WIDTH;
    uint64_t top_level_bits = total_page_num_bits % OFFSET_WIDTH;
    if (top_level_bits == 0 && TABLES_DEPTH > 0) top_level_bits = OFFSET_WIDTH;
    else if (TABLES_DEPTH == 0) top_level_bits = 0;

    for (int depth = 0; depth < TABLES_DEPTH; ++depth) {
        uint64_t current_level_shift_in_page_num;
        if (depth == 0)
            current_level_shift_in_page_num = total_page_num_bits - top_level_bits;
        else
            current_level_shift_in_page_num = total_page_num_bits - top_level_bits - (depth * OFFSET_WIDTH);

        uint64_t index = (page_num >> current_level_shift_in_page_num) & ((1ULL << OFFSET_WIDTH) - 1);
        word_t entry;
        PMread(frame * PAGE_SIZE + index, &entry);

        if (entry == 0) {
            bool needTable = (depth + 1 < TABLES_DEPTH);
            uint64_t newFrame = chooseFrame(page_num, forbid, needTable);
            if (!needTable)
                PMrestore(newFrame, page_num);
            PMwrite(frame * PAGE_SIZE + index, newFrame);
            entry = newFrame;
        }
        frame = entry;
        if (depth + 1 < TABLES_DEPTH)
            forbid[depth + 1] = frame;
    }
    return frame * PAGE_SIZE + (vaddr & (PAGE_SIZE - 1));
}

// Virtual memory API: read
int VMread(uint64_t vaddr, word_t *val)
{
    if (vaddr >= VIRTUAL_MEMORY_SIZE) return 0;
    PMread(toPhysical(vaddr), val);
    return 1;
}

// Virtual memory API: write
int VMwrite(uint64_t vaddr, word_t val)
{
    if (vaddr >= VIRTUAL_MEMORY_SIZE) return 0;
    PMwrite(toPhysical(vaddr), val);
    return 1;
}
