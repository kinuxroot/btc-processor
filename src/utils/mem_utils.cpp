#include "utils/mem_utils.h"

#ifdef __GNUC__
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#elif defined(_MSC_VER)
#include <Windows.h>
#include <psapi.h>
#endif //__GNUC__

namespace utils::mem {
    size_t getAllocatedMemory() {
        #ifdef __GNUC__
        rusage rup;
        getrusage(RUSAGE_SELF, &rup);

        return rup.ru_maxrss;
        #elif defined(_MSC_VER)
        HANDLE hCurProcess = GetCurrentProcess();
        PROCESS_MEMORY_COUNTERS pms;
        GetProcessMemoryInfo(hCurProcess, &pms, sizeof(PROCESS_MEMORY_COUNTERS));

        return pms.WorkingSetSize / 1024;
        #endif //__GNUC__
    }
}