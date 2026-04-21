# `tqsdk-wait`

Python 风格的 single-owner wait facade。

这个 crate 建立在 `tqsdk-core` 和 `tqsdk-session` 之上，把面向策略作者的 `wait_update()` / `TqApi` 式使用体验放到独立层里，同时避免把消费风格回写进 core。

它面向的是上层策略代码，不是底层协议适配。
