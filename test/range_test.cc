#include <assert.h>
#include <stdio.h>
#include <string>

#include "include/engine.h"
#include "test_util.h"

using namespace polar_race;

#define KV_CNT 10000

class PrintVisitor : public Visitor {
  void Visit(const PolarString &key, const PolarString &value) {
    printf("%s %s\n", key.data(), value.data());
  }
};

int main() {

    Engine *engine = NULL;
    printf_(
        "======================= range thread test "
        "============================");
    std::string engine_path =
        std::string("./data/test-") + std::to_string(asm_rdtsc());
    RetCode ret = Engine::Open(engine_path, &engine);
    assert(ret == kSucc);
    printf("open engine_path: %s\n", engine_path.c_str());

    PrintVisitor vis;


    /////////////////////////////////

    ret = engine->Write(PolarString("aaa"), "1");
    assert(ret == kSucc);

    ret = engine->Write(PolarString("bbb"), "2");
    assert(ret == kSucc);

    ret = engine->Write(PolarString("ccc"), "3");
    assert(ret == kSucc);

    ret = engine->Write(PolarString("ccd"), "4");
    assert(ret == kSucc);

    printf_("expected 4 entries");
    ret = engine->Range(PolarString(""), PolarString(""), vis);
    assert(ret == kSucc);

    printf_("expected 4 entries");
    ret = engine->Range(PolarString("aaa"), PolarString(""), vis);
    assert(ret == kSucc);

    printf_("expected 3 entries");
    ret = engine->Range(PolarString("aab"), PolarString(""), vis);
    assert(ret == kSucc);

    printf_("expected 1 entries");
    ret = engine->Range(PolarString("aaa"), PolarString("bb"), vis);
    assert(ret == kSucc);

    printf_("expected 2 entries");
    ret = engine->Range(PolarString("c"), PolarString(""), vis);
    assert(ret == kSucc);

    printf_("expected 2 entries");
    ret = engine->Range(PolarString("ccc"), PolarString(""), vis);
    assert(ret == kSucc);

    printf_("expected 2 entries");
    ret = engine->Range(PolarString("ccc"), PolarString("ccdd"), vis);
    assert(ret == kSucc);

    printf_("expected 1 entries");
    ret = engine->Range(PolarString("ccc"), PolarString("ccd"), vis);
    assert(ret == kSucc);

    printf_(
        "======================= range thread test pass :) "
        "======================");

    return 0;
}
