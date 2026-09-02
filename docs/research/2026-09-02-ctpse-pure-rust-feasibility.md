# CTP 穿透式客户端信息纯 Rust 重写可行性调研

日期：2026-09-02

状态：research note，不是架构规范，不授权替换生产采集库。

范围：官方 TQSDK Python 源码、`tqsdk-ctpse 1.2.0` Linux/Windows 官方发行物、CTP/看穿式监管规范原文；未访问交易账户，未发起登录、查询或交易。

## 结论

结论需要拆成三层：

| 目标 | 判断 | 原因 |
| --- | --- | --- |
| 用 Rust 采集同类 OS/硬件字段 | **可行** | Linux 库采集的是可由标准系统接口取得的时间、IP、MAC、主机名、内核版本、磁盘/CPU/BIOS 标识；不存在必须由 Python 计算的内容。 |
| 用 Rust 调用官方 `.so`/DLL，去掉 Python 运行时 | **可行，推荐** | Python 包只是 `ctypes` 加载官方原生库、调用 `CTP_GetSystemInfo(char*, int&)`，再做 Base64；Rust FFI/helper 可以等价承担这层薄封装。 |
| 纯 Rust、无官方库，直接兼容 `SHINNY_TQ_1.0` 的生产密文 | **不应作为当前实现方案** | 虽然 Linux 1.2.0 的当前封装已能被静态分析，但密文还依赖库内嵌的 RSA 公钥、第二层 AES 封装、私有头部、版本/评测环境和认证关系；这些不是公开稳定协议，也没有可用的再分发/重实现授权证据。 |
| 以自己的厂商 ID、AppID、密钥和认证流程开发纯 Rust 采集器 | **条件可行** | 规范允许交易软件商申请自己的加密密钥和函数名并开发动态链接库，但这是新的合规产品与认证项目，不是对信易科技官方库的无缝替换。 |

因此，短期正确路线是“**Rust 原生 helper + 官方采集库**”，可以消除 Python 依赖，但仍保留官方加密和认证边界。纯 Rust clean-room 重写只能在取得自己的厂商身份、密钥、明确授权并完成期货公司评测后考虑。

## 证据等级

本文严格使用以下标签：

- **已证实（源码/规范）**：官方源码、官方发行物或规范直接给出。
- **已证实（静态分析）**：对固定哈希二进制的导出符号、反汇编、常量或导入表直接观察。
- **动态观察**：只对本机、该二进制版本成立；不自动外推到其他系统或版本。
- **推断**：由多项证据支持，但没有公开契约保证。
- **未知**：现有证据无法回答。

核心规范证据是中国期货市场监控中心于 2018 年 9 月发布的《期货公司客户交易终端信息采集及接入认证技术规范》。本文引用[渤海期货官方站托管副本](https://www.bhfcc.com/files/document/20190527/6369458571301246915173819.pdf)；PDF 首页明确标注发布机构和日期。托管站不是规范制定者，事实依据来自 PDF 正文。

## 调研对象与版本锚点

### TQSDK Python

- 本地官方源码：`/opt/tqsdk-python`
- Git commit：`78c99226f11056b2860c39369f453808938edde2`
- `setup.py` 版本：`3.10.2`
- 工作树在调研时为 clean。

官方 `TqAccount` 登录流程使用固定 AppID `SHINNY_TQ_1.0`，独立生成 `client_mac_address`，只有系统信息采集成功时才附带 `client_app_id` 和 `client_system_info`：

- `/opt/tqsdk-python/tqsdk/account.py:57-101`
- `/opt/tqsdk-python/tqsdk/tradeable/otg/tqaccount.py:73-99`
- [固定 commit 上的 `tqaccount.py`](https://github.com/shinnytech/tqsdk-python/blob/78c99226f11056b2860c39369f453808938edde2/tqsdk/tradeable/otg/tqaccount.py#L73-L99)

### `tqsdk-ctpse`

- 发行版：`tqsdk-ctpse 1.2.0`
- PyPI 项目描述：`TianQin SDK - ctpse lib`
- Linux wheel：`manylinux1_x86_64`
- Linux 原生库：ELF x86-64，动态链接，未 strip
- 本地 `.so` SHA-256：`52655277923484173bff194edfa41320015739b061bbe7f7de6232d3a041fc45`
- 包元数据 `License: UNKNOWN`；这表示当前元数据没有提供可依赖的许可授予，不能据此断言允许复制、反编译结果再实现或把原生库随 Rust crate 再分发。

[PyPI 1.2.0 发行页](https://pypi.org/project/tqsdk-ctpse/1.2.0/)显示官方同时发布 Windows x86/x64、Linux x86-64、macOS universal2 和源码包。Windows x64 wheel 同时包含生产与评测 DLL。

## Python 层实际上做了什么

**已证实（源码）**：`tqsdk_ctpse.get_system_info()` 只有以下工作：

1. 分配 344 字节缓冲区；
2. 根据平台加载 `WinDataCollect*.dll`、`LinuxDataCollect64.so` 或 macOS framework；
3. 调用平台修饰名对应的 `CTP_GetSystemInfo(char*, int&)`；
4. 返回码为 0 时，对实际长度的二进制结果做标准 Base64 编码；
5. 非零时抛出采集错误。

证据：`/tmp/tqsdk-python-official-venv/lib/python3.11/site-packages/tqsdk_ctpse/__init__.py:20-47`。

这证明 Python 不是采集或加密算法的实现者；它只是原生库的进程内 FFI 包装。

另一个版本边界：1.2.0 包装器仅在 Windows x64 和 macOS 根据 `CTPSE_RUN_MODE` 选择 Test/Production 文件；Linux 分支始终加载同一个 `LinuxDataCollect64.so`。静态字符串中也没有发现 `CTPSE_RUN_MODE`。库是否以其他不可见方式区分 Linux 评测/生产仍属**未知**，不能假设环境变量在 Linux 上一定切换密钥。

## Linux `.so` 返回的到底是什么

### 1. 导出层次

**已证实（静态分析）**：固定哈希的 Linux 1.2.0 库导出三层相关函数：

```text
CTP_GetRealSystemInfo(char*, int&)
CTP_GetSystemInfoUnAesEncode(char*, int&)
CTP_GetSystemInfo(char*, int&)
```

`nm -D --defined-only ... | c++filt` 同时暴露了采集与密码函数名，包括：

```text
getLocalMacInfo
GetDeviceNameAndOsVersion
GetScsiTypeHardDiskID
GetCpuSerial
GetBIOSSerial
RSA_EncodeCollectData
AES_EncodeCollectData
AES_DecodeCollectData
```

其中前两个辅助入口没有被 Python 包公开；它们是该 Linux 构建的二进制导出，不应被当作跨版本公共 API。

### 2. 明文层：11 个 `@` 分隔字段

**已证实（规范 + 静态分析）**：`CTP_GetRealSystemInfo` 先按以下顺序组装 ASCII 字符串：

| 索引 | 字段 | Linux 1.2.0 的采集方式 |
| ---: | --- | --- |
| 0 | 终端类型 | 固定字符 `2`，即 Linux |
| 1 | 信息采集时间 | `localtime`，格式 `YYYY-MM-DD HH:MM:SS` |
| 2 | 私网 IPv4 1 | `socket/ioctl` 枚举接口 |
| 3 | 私网 IPv4 2 | 同上 |
| 4 | 网卡 MAC 1 | 12 位十六进制，无 `-`/`:` |
| 5 | 网卡 MAC 2 | 同上 |
| 6 | 设备名 | `uname().nodename`，库强制截断到最多 9 字节 |
| 7 | 操作系统版本 | `uname().release`，库强制截断到最多 5 字节 |
| 8 | 硬盘序列号 | 优先 `/dev/sda`/`/dev/hda` 的 `HDIO_GET_IDENTITY`，失败后尝试 SCSI inquiry，最多 16 字节 |
| 9 | CPU 序列号 | 执行 `dmidecode -t 4 \| grep ID` 并规范化，最多 16 字节 |
| 10 | BIOS 序列号 | 执行 `dmidecode -t 1 \| grep "Serial Number"` 并规范化，最多 10 字节 |

这与《期货公司客户交易终端信息采集及接入认证技术规范》中的 Linux PC 字段顺序、最大长度和采集方法一致。规范将 Linux 编码定义为 `2`，要求每次登录采集，并规定空字段仍需保留分隔符；终端采集串的核心顺序为“终端类型、采集时间、两个私网 IP、两个 MAC、设备名、操作系统版本、硬盘、CPU、BIOS”。见该规范第 4.2.3、4.2.4、4.3 和 4.4 节及附录 B.3：[规范 PDF](https://www.bhfcc.com/files/document/20190527/6369458571301246915173819.pdf)。

**动态观察**：在本机调用明文辅助入口时：

- 返回码为 0；
- 字符串恰有 11 段；
- 类型、时间、两个 IPv4、两个 MAC 均通过格式分类；
- 设备名/内核版本符合上述 9/5 字节截断行为；
- 间隔约 1 秒再次采集时，只有时间字段变化，其余 10 个字段保持不变。

配套 `strace` 只读跟踪还观察到 `SIOCGIFCONF`、`SIOCGIFFLAGS`、`SIOCGIFADDR`、`SIOCGIFHWADDR`、`/dev/sda` + `HDIO_GET_IDENTITY`、两个固定 `dmidecode` 管道和 `uname`，与反汇编得到的字段映射一致。

报告和命令输出均未打印字段原值、哈希或可逆表示。

### 3. 第一层密码封装：RSA 2048 PKCS#1

**已证实（规范 + 静态分析）**：`CTP_GetSystemInfoUnAesEncode` 的行为是：

1. 调用 `CTP_GetRealSystemInfo`；
2. 使用库内材料构造 RSA 公钥；
3. 以 `RSA_public_encrypt(..., RSA_PKCS1_PADDING)` 加密完整明文；
4. 得到固定 256 字节 RSA 密文；
5. 在其前面加 8 字节私有头部，总长 264 字节。

8 字节头部在该版本中可观察为：

```text
byte 0      版本/类型值，当前为 1
byte 1      ASCII 数字；本次成功路径观察为 '0'，精确定义未知
byte 2..7   两位年、月、日、时、分、秒
```

规范第 4.4.1 节明确要求终端采集信息使用 RSA 2048 PKCS#1 加密，之后 Base64 且不得换行。该规范同时要求交易软件商向监控中心申请加密密钥、密钥版本和厂商 ID；见第 4.3.2、附录 A.1。

**动态观察**：同一秒内连续两次调用时，8 字节头部相同，但 256 字节 RSA 区不同，符合 PKCS#1 v1.5 随机填充特征。因此官方输出不能用固定 golden ciphertext 验证；只能验证解封后的语义、包结构或由官方接收端验证。

### 4. 第二层密码封装：只处理首个 16 字节块的 AES-128 ECB

**已证实（静态分析）**：`CTP_GetSystemInfo` 取得上述 264 字节后，调用 `AES_EncodeCollectData`。该函数从库内混淆表重建 16 字节密钥，执行一次 `AES_set_encrypt_key(..., 128)` 和一次 `AES_ecb_encrypt`，原地加密第一个 16 字节块；第 16 字节之后保持原样。

规范附录 B.1 说明：为证明上报信息来自信息采集动态链接库，CTP 采集库会对已经使用监控中心密钥加密的数据“二次加密”。二进制中的这一步与该说明相符，但规范没有公开本版本 AES 头部/密钥的稳定线协议。

**动态观察**：

- `CTP_GetSystemInfo` 返回 264 字节高熵二进制；
- 调用库内 AES 解码辅助函数后，头部恢复为上述结构；
- 第 16 字节之后与解码前完全一致；
- 当前 Python 包输出为 352 字符的规范 Base64，解码后正好 264 字节，无换行。

因此，`client_system_info` 并不是“Base64 后的明文系统信息”；它是：

```text
Base64(
  AES-128-ECB(前 16 字节：[私有 8 字节头 + RSA 密文前 8 字节])
  || RSA 密文剩余 248 字节
)
```

### 5. 缺字段与时区行为

这两点直接影响重写兼容性：

1. **缺字段不一定报错。** 将 `PATH` 限制为不含 `dmidecode` 后，CPU 和 BIOS 字段为空，但 `CTP_GetRealSystemInfo` 以及外层 `get_system_info()` 仍返回成功，11 个字段位置仍保留。也就是说，返回码 0 不代表所有监管字段都采集齐全。
2. **时间受进程时区影响。** 在 `TZ=UTC` 的隔离进程中，采集时间匹配 UTC 而不是规范要求的东八区。静态分析也确认使用 `localtime`。部署时应明确提供 `Asia/Shanghai`/UTC+8 时区；纯 Rust 实现若出现，应按规范固定东八区，而不是依赖宿主机默认时区。

以上是**动态观察**，不是其他平台或版本的契约。

## Windows DLL 能确认到什么

对 PyPI 1.2.0 官方 Windows x64 wheel 做了只读静态检查：

- 包含 `WinDataCollect64.dll` 和 `WinDataCollect64Test.dll`，二者哈希不同；
- 两个 DLL 都是 PE32+ x86-64；
- 生产 DLL 只导出修饰后的 `CTP_GetSystemInfo(char*, int&)`，没有像该 Linux 构建一样导出明文辅助入口；
- 导入表包含 `GetAdaptersInfo`、`NetWkstaGetInfo`、`GetVolumeInformationA`、`CreateFileW`、进程/管道和系统时间相关 API，与规范附录 B.3 的 Windows 采集方法相符；
- 同样内置 OpenSSL RSA/AES 代码。

未在 Windows 上动态执行 DLL，因此以下均为**未知**：

- Windows 1.2.0 的最终二进制长度是否始终为 264；
- 生产/评测 DLL 的头部和密钥差异；
- 网卡、磁盘、分区、CPU、BIOS 的精确选择和规范化细节；
- 与 Linux 是否使用完全相同的第二层 AES 格式。

不能用 Linux 的反汇编结果冒充 Windows 公共协议。

## 纯 Rust 重写的技术拆分

### A. 采集层：可实现

Linux 可用 Rust + 小范围 `libc`/`nix` 完成：

- `clock_gettime`/时区库：东八区采集时间；
- `getifaddrs` 或 netlink：激活网卡、IPv4/IPv6、MAC；
- `uname`：设备名与内核版本；
- sysfs、`HDIO_GET_IDENTITY`、SG_IO/NVMe ioctl：磁盘标识；
- CPUID/DMI/sysfs：CPU 与 BIOS 标识。

但“能取到字段”不等于“能生成完全相同字段”。必须固定：

- 网卡排序、激活判定、虚拟网卡/loopback 排除规则；
- 两个 IP/MAC 的配对关系；
- 字节长度截断而非 Unicode 字符截断；
- 大小写、空格、分隔符和空字段；
- SATA/SCSI/NVMe/虚拟机/容器中的设备选择；
- 权限不足时的错误或空字段策略；
- Windows/macOS 各自不同的系统 API 语义。

当前官方 Linux 库仍偏向 `/dev/sda`、SCSI 和 `dmidecode`；在 NVMe、容器和最小化发行版上容易缺字段。纯 Rust 可以提升覆盖率，但只要采集结果与已认证版本不同，就需要重新评测，不能把“更完整”自动等同于“兼容”。

### B. 明文序列化层：可实现

11 字段、`@` 分隔、空字段保留、最大长度都已由规范和本版本二进制交叉确认。Linux 这一层 clean-room 实现难度低。

### C. RSA 层：算法公开，密钥与身份不是

RSA-2048 PKCS#1 v1.5 本身可用成熟 Rust 密码库实现。真正的阻塞项是：

- 应使用哪一个监控中心派发的公钥/密钥版本；
- 密钥对应哪个交易软件厂商 ID 和 AppID；
- 生产/评测环境如何切换和轮换；
- 是否允许在新的 Rust 产品中使用信易科技发行物内的材料。

规范明确要求交易软件商申请自己的密钥并妥善保护。逆向提取并复制 `tqsdk-ctpse` 内嵌材料虽然在技术上可能，但没有公开契约或授权支持，不应成为工程方案。

### D. 私有头部和第二层 AES：可逆向，但不是可维护协议

本版本头部和 AES 行为可以复刻，但缺少：

- 官方字段定义和版本协商；
- 密钥轮换规则；
- Linux/Windows/macOS 一致性保证；
- 后端是否校验库指纹、头部版本或环境密钥的说明；
- 下一版保持兼容的承诺。

这一层是“技术上能写出来、产品上不应依赖”的典型私有实现细节。

### E. 合规认证层：代码无法替代

规范要求期货公司核验终端是否集成符合要求的采集动态库、是否准确采集，并以 AppID/授权码完成接入认证。SimNow 官方 API 下载页也提供生产/评测采集库、隐私政策与明文自检工具，并列出采集信息类别：[SimNow API 下载](https://www.simnow.com.cn/static/apiDownload.action)。

因此，即使纯 Rust 输出在字节层“看起来相同”，也不等于获得了对应 AppID、厂商密钥和生产准入。

## 推荐路线

### 推荐 1：Rust helper 动态加载官方库

目标是去掉 Python，而不是去掉官方采集库：

```text
tqsdk-session
  -> 受限子进程 tqsdk-ctpse-helper
      -> dlopen/LoadLibrary 用户提供的官方 .so/DLL
          -> CTP_GetSystemInfo(buffer, length)
      -> 仅输出 Base64
```

建议约束：

- helper 是唯一 `unsafe`/FFI 边界；主 session 保持安全 Rust；
- 原生库路径显式配置，不自动扫描不可信目录；
- 子进程继承最小环境，并显式设置 `TZ=Asia/Shanghai`；
- 超时、输出上限、退出码、崩溃隔离与现有 Python collector 相同；
- 不记录原始二进制、Base64、MAC、账户或密码；
- 不在 crates.io 包中捆绑官方二进制，直到取得明确再分发许可；
- 生产/评测库选择必须显式，不能静默回退；
- 提供本地自检，只报告字段“存在/缺失”，不输出值。

### 推荐 2：若要做真正纯 Rust，先完成非代码前置条件

开始实现前必须取得书面答案：

1. 信易科技是否授权对 `SHINNY_TQ_1.0` 使用非官方采集实现；
2. 监控中心/期货公司是否为本项目分配独立厂商 ID、AppID、密钥版本和生产/评测密钥；
3. TQ 交易服务是否只转发密文，还是校验外层私有封装；
4. Linux/Windows/macOS 的认证测试向量或自检工具是否可用于自动验收；
5. 原生库的再分发、动态加载和逆向兼容边界是什么。

只有 1-5 得到肯定且可执行的书面结论后，才应做 clean-room Rust 采集器。届时它应使用新的产品身份，而不是复制当前二进制中的密钥材料。

## 未知项与风险清单

| 项目 | 当前状态 | 关闭方式 |
| --- | --- | --- |
| 8 字节头部 byte 1 的正式含义 | 未知 | 向 SFIT/信易科技索取格式说明或测试向量 |
| 第二层 AES 的版本/轮换规则 | 未知 | 官方协议或多版本差分测试 |
| Linux `CTPSE_RUN_MODE` 是否有效 | 未证实；包装器不切库 | 官方说明 + 评测/生产接收端验证 |
| Windows/macOS 的精确明文与封装 | 未动态验证 | 在隔离的对应平台运行脱敏分类探针 |
| TQ 后端对 `client_system_info` 的校验深度 | 未知 | 信易科技书面接口说明；不通过账户试错推断 |
| `tqsdk-ctpse` 原生库再分发/逆向许可 | 元数据为 UNKNOWN | 向权利人取得许可证或书面授权 |
| 缺 CPU/BIOS 等字段是否会被每家期货公司接受 | 不一致且与权限有关 | 官方自检工具 + 各期货公司评测 |
| 生产密钥轮换后旧纯 Rust 实现如何升级 | 未知 | 正式版本协商和密钥发布机制 |

## 可复现的脱敏证据命令

以下命令不打印系统指纹原值。

### 二进制身份与导出符号

```bash
ctpse_lib=/path/to/tqsdk_ctpse/LinuxDataCollect64.so
file "$ctpse_lib"
sha256sum "$ctpse_lib"
nm -D --defined-only "$ctpse_lib" | c++filt \
  | rg 'CTP_Get(SystemInfo|RealSystemInfo|SystemInfoUnAesEncode)|Get(Cpu|BIOS|Scsi)|AES_|RSA_'
```

### 静态确认采集和密码调用

```bash
ctpse_lib=/path/to/tqsdk_ctpse/LinuxDataCollect64.so
objdump -d -C -Mintel --no-show-raw-insn "$ctpse_lib" \
  | rg 'CTP_GetRealSystemInfo|CTP_GetSystemInfoUnAesEncode|CTP_GetSystemInfo|RSA_public_encrypt|AES_ecb_encrypt|uname|ioctl|inet_ntoa'
strings -a "$ctpse_lib" \
  | rg 'dmidecode -t 4|dmidecode -t 1|/dev/sda|/dev/hda'
```

### 明文 schema 脱敏探针

下面的探针只输出总长度、字段数、schema 布尔值和三个硬件字段的存在位图，不输出字段内容、摘要或 Base64：

```bash
CTPSE_LIBRARY=/path/to/tqsdk_ctpse/LinuxDataCollect64.so python3 - <<'PY'
import ctypes
import datetime
import ipaddress
import os
import re

lib = ctypes.CDLL(os.environ["CTPSE_LIBRARY"])
buffer = ctypes.create_string_buffer(344)
length = ctypes.c_int(344)
rc = lib._Z21CTP_GetRealSystemInfoPcRi(buffer, ctypes.byref(length))
fields = buffer.raw[:length.value].split(b"@")

def is_time(value):
    try:
        datetime.datetime.strptime(value.decode("ascii"), "%Y-%m-%d %H:%M:%S")
        return True
    except (UnicodeDecodeError, ValueError):
        return False

def is_ip(value):
    try:
        ipaddress.ip_address(value.decode("ascii"))
        return True
    except (UnicodeDecodeError, ValueError):
        return False

schema_ok = len(fields) == 11 and all((
    fields[0] == b"2",
    is_time(fields[1]),
    is_ip(fields[2]),
    is_ip(fields[3]),
    bool(re.fullmatch(rb"[0-9A-Fa-f]{12}", fields[4])),
    bool(re.fullmatch(rb"[0-9A-Fa-f]{12}", fields[5])),
    fields[6] == os.uname().nodename.encode()[:9],
    fields[7] == os.uname().release.encode()[:5],
))
print({
    "rc": rc,
    "length": length.value,
    "field_count": len(fields),
    "schema_ok": schema_ok,
    "hardware_present": [bool(value) for value in fields[8:11]],
})
PY
```

不要在共享终端中直接打印 `CTP_GetRealSystemInfo` 或 `get_system_info()` 的返回值。

## 调研边界

- 没有访问任何实盘或仿真账户。
- 没有登录交易服务、确认结算、下单、撤单或转账。
- 没有把原始终端指纹、其 Base64、哈希或账户信息写入报告。
- 动态探针仅在进程内短暂读取本机字段，输出聚合分类与布尔结果。
- 没有提取、记录或复制库内 RSA/AES 密钥字节。
- 本报告不是法律意见；许可与合规结论必须由权利人、监控中心和接入期货公司书面确认。
