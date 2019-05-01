@0x903e2b6ccac6f980;

struct Checkpoint {}


struct Watermark {}

struct Element {
  id            @0  :UInt64;
  timestamp     @1  :UInt64;
  data          @2  :Data;
  taskId        @3  :Text;
}

struct TaskMsg {
  union {
    checkpoint  @0  :Checkpoint;
    watermark   @1  :Watermark;
    element     @2  :Element;
  }
}
