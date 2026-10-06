#include "util/Phone.h"

#include <cctype>

namespace ctraderplus::util {

std::string normalizePhone(const std::string &phone) {
    std::string digits;
    bool plus = false;
    for (char c : phone) {
        if (c == '+') plus = true;
        else if (std::isdigit(static_cast<unsigned char>(c))) digits.push_back(c);
    }
    if (digits.size() >= 2 && digits[0] == '0' && digits[1] == '0') {
        digits.erase(0, 2);
        plus = true;
    }
    if (digits.empty()) return plus ? "+" : "";
    return "+" + digits;
}

bool isE164(const std::string &phone) {
    if (phone.size() < 9 || phone.size() > 16 || phone[0] != '+') return false;
    if (phone[1] < '1' || phone[1] > '9') return false;
    for (size_t i = 1; i < phone.size(); ++i) {
        if (!std::isdigit(static_cast<unsigned char>(phone[i]))) return false;
    }
    return true;
}

}  // namespace ctraderplus::util
