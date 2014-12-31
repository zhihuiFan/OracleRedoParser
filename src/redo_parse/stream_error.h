#include <map>
#include <string>

namespace databus {
  const std::map<uint32_t, const char*> errors{{1, "error1"}, {2, "eror2"}};
  class ParseException {
   public:
    ParseException(const std::string& msg) : msg_(msg) {}
    std::string& msg() { return msg_; }

   private:
    std::string msg_;
  };
}
