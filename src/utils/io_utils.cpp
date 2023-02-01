#include "btc-config.h"
#include "utils/io_utils.h"

#include <fstream>

namespace utils {
    std::string readFile(const std::string& filePath)
    {
        if (std::ifstream is{ "test.txt", std::ios::binary | std::ios::ate }) {
            auto size = is.tellg();
            std::string str(size, '\0');
            is.seekg(0);
            if (is.read(&str[0], size)) {
                return str;
            }
        }

        return "";
    }

    void copyStream(std::istream& is, std::ostream& os) {
        is.seekg(0, std::ios::end);
        auto size = is.tellg();
        std::string str(size, '\0'); // construct string to stream size

        is.seekg(0);
        if (is.read(&str[0], size)) {
            os.write(str.c_str(), size);
        }
    }
}
