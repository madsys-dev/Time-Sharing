#include <smmintrin.h>
#include <emmintrin.h>

unsigned long long nt_load (void* mem_addr, int index) {
    __m128i a = _mm_stream_load_si128((__m128i*) mem_addr);
    unsigned long long b[2];
    _mm_storeu_si128((__m128i*)b, a);
    return b[index];
}
