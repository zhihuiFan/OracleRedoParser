#include "physical_elems.h"

namespace databus {

  uint32_t ChangeHeader::changeSize() const {
    int elem_count = partCount();
    if (elem_count == -1) return 0;  // lol() == 0

    uint32_t total_length = 0;

    for (int i = 1; i <= elem_count; ++i) {
      Ushort len = *(Ushort*)(length_ + i * 2);
      total_length += align4(len);
    }

    return total_length + headerSize();
  }

  Ushort* ChangeHeader::partLen(int part_no) const {
    return (Ushort*)(length_ + part_no * 2);
  }

  const char* ChangeHeader::part(int part_no) const {
    uint32_t offset = 0;
    for (int i = 0; i < part_no; ++i) {
      offset += align4(*partLen(i));
    }
    return length_ + offset;
  }
}
