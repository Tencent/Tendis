#include <iostream>
#include <string>
#include "tendisplus/utils/base64.h"
#include "gtest/gtest.h"

namespace tendisplus {

TEST(Base64, common) {
    std::string data = "aa";
    std::string encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    std::string decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);

    data = "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM2222222222222222222222222222222222";
    encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);

    data = "MMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMMM22222222222222222222222222222222 \
        ------------------------------****************************************************** \
        ########################################zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz";
    encode = Base64::Encode((unsigned char*)data.c_str(), data.size());
    decode = Base64::Decode(encode.c_str(), encode.size());
    EXPECT_EQ(data, decode);
}

}  // namespace tendisplus
