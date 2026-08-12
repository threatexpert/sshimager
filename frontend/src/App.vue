<script setup lang="ts">
import { computed, onMounted, reactive, ref } from 'vue'

interface RemoteDisk {
  name: string
  device: string
  model: string
  size: number
  sizeLabel: string
}

interface ConnectionResult {
  remote: string
  isRoot: boolean
  disks: RemoteDisk[]
}

interface Partition {
  number: number
  device: string
  fileSystem: string
  label: string
  mountpoint: string
  size: number
  sizeLabel: string
  copyMode: 'full' | 'used' | 'skip'
  supportsUsedOnly: boolean
}

interface DiskInfo {
  device: string
  model: string
  size: number
  sizeLabel: string
  tableType: string
  partitions: Partition[]
}

interface ImagingEvent {
  kind: 'progress' | 'stage' | 'log' | 'reconnecting'
  stage?: string
  message?: string
  done?: number
  total?: number
  dataWritten?: number
  percent?: number
  speedMBps?: number
  etaSeconds?: number
}

interface BackupDone {
  success: boolean
  cancelled: boolean
  output: string
  error?: string
}

const api = () => window.go.main.GUIApp
const step = ref(1)
const version = ref('')
const busy = ref(false)
const errorMessage = ref('')
const showAdvanced = ref(false)
const showPassword = ref(false)
const showAbout = ref(false)

const connection = reactive({
  host: '',
  port: 22,
  username: 'root',
  password: '',
  sudoPassword: '',
})
const transport = reactive({
  useAgent: false,
  compression: 'zstd-fast',
})
const connected = ref<ConnectionResult | null>(null)
const selectedDevice = ref('')
const disk = ref<DiskInfo | null>(null)
const format = ref('vmdk')
const output = ref('')
const bufferMB = ref(4)
const logs = ref<string[]>([])
const done = ref<BackupDone | null>(null)
const cancelling = ref(false)
const progress = reactive({
  percent: 0,
  stage: '准备备份',
  done: 0,
  total: 0,
  dataWritten: 0,
  speedMBps: 0,
  etaSeconds: 0,
})

const steps = [
  { number: 1, title: '连接主机', subtitle: 'SSH 身份验证' },
  { number: 2, title: '选择磁盘', subtitle: '发现远程设备' },
  { number: 3, title: '备份设置', subtitle: '分区与输出格式' },
  { number: 4, title: '创建镜像', subtitle: '进度与结果' },
]

const selectedDisk = computed(() => connected.value?.disks.find((item) => item.device === selectedDevice.value))
const activePartitionCount = computed(() => {
  if (!disk.value) return 0
  if (disk.value.partitions.length === 0) return 1 // whole-disk copy
  return disk.value.partitions.filter((item) => item.copyMode !== 'skip').length
})
const canStart = computed(() => Boolean(output.value && activePartitionCount.value > 0 && !busy.value))

function errorText(error: unknown): string {
  if (error instanceof Error) return error.message
  if (typeof error === 'string') return error
  return '操作失败，请检查连接参数后重试'
}

function setError(error: unknown) {
  errorMessage.value = errorText(error).replace(/^Error:\s*/i, '')
}

async function connectHost() {
  busy.value = true
  errorMessage.value = ''
  try {
    connected.value = await api().Connect({ ...connection }) as ConnectionResult
    selectedDevice.value = connected.value.disks[0]?.device ?? ''
    disk.value = null
    output.value = ''
    step.value = 2
  } catch (error) {
    setError(error)
  } finally {
    busy.value = false
  }
}

async function scanSelectedDisk() {
  if (!selectedDevice.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    disk.value = await api().ScanDisk({
      device: selectedDevice.value,
      useAgent: transport.useAgent,
      compression: transport.compression,
    }) as DiskInfo
    bufferMB.value = transport.useAgent ? 4 : 8
    step.value = 3
  } catch (error) {
    setError(error)
  } finally {
    busy.value = false
  }
}

function suggestedFilename() {
  const host = connection.host.replace(/[^a-zA-Z0-9._-]+/g, '-') || 'remote'
  const device = selectedDevice.value.split('/').pop() || 'disk'
  const date = new Date().toISOString().slice(0, 10).replaceAll('-', '')
  return `${host}-${device}-${date}.${format.value === 'dd' ? 'dd' : format.value}`
}

async function chooseOutput() {
  errorMessage.value = ''
  try {
    const path = await api().SelectOutputPath(format.value, suggestedFilename())
    if (path) output.value = path
  } catch (error) {
    setError(error)
  }
}

function selectFormat(value: string) {
  format.value = value
  output.value = ''
}

async function startBackup() {
  if (!canStart.value || !disk.value) return
  busy.value = true
  errorMessage.value = ''
  done.value = null
  cancelling.value = false
  logs.value = []
  Object.assign(progress, {
    percent: 0,
    stage: '正在初始化备份',
    done: 0,
    total: disk.value.size,
    dataWritten: 0,
    speedMBps: 0,
    etaSeconds: 0,
  })
  try {
    await api().StartBackup({
      output: output.value,
      format: format.value,
      bufferMB: bufferMB.value,
      overwrite: true,
      partitions: disk.value.partitions.map((item) => ({ number: item.number, mode: item.copyMode })),
    })
    step.value = 4
  } catch (error) {
    setError(error)
  } finally {
    busy.value = false
  }
}

async function cancelBackup() {
  if (!window.confirm('确定要取消当前备份吗？已生成的文件将作为未完成镜像保留。')) return
  cancelling.value = true
  await api().CancelBackup()
}

async function revealOutput() {
  if (!done.value?.output) return
  try {
    await api().RevealOutput(done.value.output)
  } catch (error) {
    setError(error)
  }
}

async function openProjectPage() {
  await api().OpenProjectPage()
}

async function disconnectHost() {
  if (step.value === 4 && !done.value) return
  busy.value = true
  errorMessage.value = ''
  try {
    await api().Disconnect()
    returnToConnection()
  } catch (error) {
    setError(error)
  } finally {
    busy.value = false
  }
}

function addLog(message?: string) {
  if (!message) return
  const stamp = new Date().toLocaleTimeString('zh-CN', { hour12: false })
  logs.value.push(`${stamp}  ${message}`)
  if (logs.value.length > 120) logs.value.splice(0, logs.value.length - 120)
}

function handleImagingEvent(payload: unknown) {
  const event = payload as ImagingEvent
  if (event.stage) progress.stage = event.stage
  if (event.kind === 'progress') {
    progress.percent = event.percent ?? progress.percent
    progress.done = event.done ?? progress.done
    progress.total = event.total ?? progress.total
    progress.dataWritten = event.dataWritten ?? progress.dataWritten
    progress.speedMBps = event.speedMBps ?? progress.speedMBps
    progress.etaSeconds = event.etaSeconds ?? progress.etaSeconds
  } else {
    addLog(event.message || event.stage)
  }
}

function handleBackupDone(payload: unknown) {
  done.value = payload as BackupDone
  cancelling.value = false
  if (done.value.success) {
    progress.percent = 100
    progress.stage = '镜像创建完成'
    addLog('备份完成，镜像已设置为只读')
  } else if (done.value.cancelled) {
    progress.stage = '备份已取消'
    addLog('用户取消了备份任务')
  } else {
    progress.stage = '备份失败'
    addLog(done.value.error || '备份任务异常结束')
  }
}

function formatBytes(value: number) {
  if (!value) return '0 B'
  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  const index = Math.min(Math.floor(Math.log(value) / Math.log(1000)), units.length - 1)
  return `${(value / Math.pow(1000, index)).toFixed(index > 1 ? 2 : 0)} ${units[index]}`
}

function formatETA(seconds: number) {
  if (!seconds) return '计算中'
  if (seconds < 60) return `${seconds} 秒`
  if (seconds < 3600) return `${Math.floor(seconds / 60)} 分 ${seconds % 60} 秒`
  return `${Math.floor(seconds / 3600)} 小时 ${Math.floor((seconds % 3600) / 60)} 分`
}

function returnToConnection() {
  step.value = 1
  connected.value = null
  disk.value = null
  done.value = null
  output.value = ''
  errorMessage.value = ''
}

onMounted(async () => {
  version.value = await api().GetVersion()
  window.runtime.EventsOn('backup:event', handleImagingEvent)
  window.runtime.EventsOn('backup:done', handleBackupDone)
})
</script>

<template>
  <div class="app-shell">
    <aside class="sidebar">
      <div class="brand">
        <div class="brand-mark"><img src="/appicon.png" alt="" /></div>
        <div>
          <strong>SSH Imager</strong>
          <small>Remote disk imaging</small>
        </div>
      </div>

      <nav class="steps" aria-label="备份步骤">
        <div v-for="item in steps" :key="item.number" class="step-item"
          :class="{ active: step === item.number, complete: step > item.number }">
          <div class="step-number">
            <span v-if="step <= item.number">{{ item.number }}</span>
            <span v-else>✓</span>
          </div>
          <div>
            <strong>{{ item.title }}</strong>
            <small>{{ item.subtitle }}</small>
          </div>
        </div>
      </nav>

      <div v-if="connected" class="remote-card" :title="`已连接：${connected.remote}`">
        <div class="remote-status-line">
          <span class="status-dot"></span>
          <small>已连接 · {{ connected.isRoot ? 'ROOT' : 'SUDO' }}</small>
          <button class="disconnect-button" title="断开 SSH 连接" :disabled="step === 4 && !done" @click="disconnectHost">断开</button>
        </div>
        <strong class="remote-address" :title="connected.remote">{{ connected.remote }}</strong>
      </div>

      <div class="sidebar-footer">
        <span>v{{ version }}</span>
        <button @click="showAbout = true">关于</button>
      </div>
    </aside>

    <main class="workspace">
      <header class="topbar">
        <div>
          <span class="eyebrow">安全磁盘备份</span>
          <h1>{{ steps[step - 1].title }}</h1>
        </div>
        <div class="secure-pill"><span>●</span> SSH 加密传输</div>
      </header>

      <div v-if="errorMessage" class="alert error-alert">
        <span class="alert-icon">!</span>
        <div><strong>操作未完成</strong><p>{{ errorMessage }}</p></div>
        <button class="icon-button" @click="errorMessage = ''">×</button>
      </div>

      <section v-if="step === 1" class="content-grid connection-page">
        <div class="panel primary-panel">
          <div class="panel-heading">
            <div><span class="section-kicker">01 / CONNECT</span><h2>连接远程 Linux 主机</h2></div>
            <div class="protocol-badge">SSH</div>
          </div>
          <p class="panel-description">使用拥有磁盘读取权限的账户。凭据仅保存在当前进程内存中。</p>

          <form class="form-grid" @submit.prevent="connectHost">
            <label class="field">
              <span>主机地址</span>
              <div class="input-wrap">
                <span class="input-prefix">⌁</span>
                <input v-model.trim="connection.host" required autocomplete="off" placeholder="192.168.1.50 或 server.example.com" />
              </div>
            </label>
            <label class="field port-field">
              <span>端口</span>
              <input v-model.number="connection.port" required type="number" min="1" max="65535" />
            </label>
            <label class="field field-wide">
              <span>用户名</span>
              <input v-model.trim="connection.username" required autocomplete="username" placeholder="root" />
            </label>
            <label class="field field-wide">
              <span>SSH 密码</span>
              <div class="input-wrap">
                <span class="input-prefix">⌘</span>
                <input v-model="connection.password" required :type="showPassword ? 'text' : 'password'" autocomplete="current-password" placeholder="输入 SSH 密码" />
                <button type="button" class="input-action" @click="showPassword = !showPassword">{{ showPassword ? '隐藏' : '显示' }}</button>
              </div>
            </label>

            <button type="button" class="advanced-toggle" @click="showAdvanced = !showAdvanced">
              <span>{{ showAdvanced ? '−' : '+' }}</span> 高级连接设置
            </button>
            <div v-if="showAdvanced" class="advanced-box field-wide">
              <label class="field">
                <span>Sudo 密码 <em>可选</em></span>
                <input v-model="connection.sudoPassword" type="password" autocomplete="off" placeholder="默认使用 SSH 密码" />
              </label>
              <p>非 root 用户需要 sudo 权限才能读取块设备。</p>
            </div>

            <button class="button primary-button field-wide" type="submit" :disabled="busy">
              <span v-if="busy" class="spinner"></span>
              {{ busy ? '正在连接并发现磁盘…' : '连接并发现磁盘' }}
              <span v-if="!busy" class="arrow">→</span>
            </button>
          </form>
        </div>

        <div class="side-info">
          <div class="info-graphic">
            <div class="node local-node"><span>本机</span><strong>VMDK</strong></div>
            <div class="connection-line"><i></i><i></i><i></i></div>
            <div class="node server-node"><span>远程</span><strong>Linux</strong></div>
          </div>
          <h3>无侵入式成像</h3>
          <p>无需在目标主机长期安装服务，仅通过 SSH 读取磁盘，并在本机直接创建虚拟磁盘。</p>
          <ul class="feature-list">
            <li><span>✓</span> 自动断线重连与断点续传</li>
            <li><span>✓</span> 文件系统已用块智能复制</li>
            <li><span>✓</span> VMDK / VHD / VDI / DD</li>
          </ul>
        </div>
      </section>

      <section v-else-if="step === 2" class="page-section">
        <div class="section-header">
          <div><span class="section-kicker">02 / DISCOVER</span><h2>选择需要备份的物理磁盘</h2><p>仅显示远程主机上的整盘设备，不包含 loop、ram 和光驱。</p></div>
        </div>

        <div class="disk-list">
          <button v-for="item in connected?.disks" :key="item.device" class="disk-card"
            :class="{ selected: selectedDevice === item.device }" @click="selectedDevice = item.device">
            <span class="radio-indicator"><i></i></span>
            <span class="disk-icon"><i></i><i></i></span>
            <span class="disk-details">
              <span class="disk-title"><strong>{{ item.device }}</strong><em>{{ item.sizeLabel }}</em></span>
              <span>{{ item.model || '未提供设备型号' }}</span>
            </span>
            <span class="disk-arrow">›</span>
          </button>
        </div>

        <div class="transport-panel">
          <div>
            <strong>传输模式</strong>
            <p v-if="transport.useAgent" class="agent-notice">会将 Agent 模块上传到目标主机的临时目录，用于流式读取和压缩传输；任务完成后自动删除。</p>
            <p v-else>使用目标主机现有的 SFTP 服务读取磁盘，不会上传 Agent 模块。</p>
          </div>
          <div class="segmented-control">
            <button :class="{ active: !transport.useAgent }" @click="transport.useAgent = false">SFTP</button>
            <button :class="{ active: transport.useAgent }" @click="transport.useAgent = true">高速 Agent</button>
          </div>
          <label v-if="transport.useAgent" class="compact-field">
            <span>压缩</span>
            <select v-model="transport.compression">
              <option value="zstd-fast">ZSTD Fast</option>
              <option value="zstd">ZSTD</option>
              <option value="none">不压缩</option>
            </select>
          </label>
        </div>

        <div class="action-row">
          <div v-if="selectedDisk" class="selection-summary">已选择 <strong>{{ selectedDisk.device }}</strong> · {{ selectedDisk.sizeLabel }}</div>
          <button class="button primary-button" :disabled="busy || !selectedDevice" @click="scanSelectedDisk">
            <span v-if="busy" class="spinner"></span>{{ busy ? '正在扫描分区…' : '扫描磁盘分区' }}<span v-if="!busy" class="arrow">→</span>
          </button>
        </div>
      </section>

      <section v-else-if="step === 3 && disk" class="page-section settings-page">
        <div class="section-header compact">
          <div><span class="section-kicker">03 / CONFIGURE</span><h2>配置分区复制策略</h2><p>{{ disk.device }} · {{ disk.model || '未知型号' }} · {{ disk.sizeLabel }} · {{ disk.tableType }}</p></div>
          <button class="button ghost-button" @click="step = 2">重新选盘</button>
        </div>

        <div class="partition-table-wrap">
          <table class="partition-table">
            <thead><tr><th>分区</th><th>文件系统</th><th>挂载点 / 标签</th><th>容量</th><th>复制策略</th></tr></thead>
            <tbody>
              <tr v-for="item in disk.partitions" :key="item.number" :class="{ skipped: item.copyMode === 'skip' }">
                <td><strong>{{ item.device }}</strong><small>#{{ item.number }}</small></td>
                <td><span class="fs-badge">{{ item.fileSystem }}</span></td>
                <td>{{ item.mountpoint || item.label || '—' }}</td>
                <td>{{ item.sizeLabel }}</td>
                <td>
                  <select v-model="item.copyMode" class="mode-select">
                    <option value="full">完整复制</option>
                    <option v-if="item.supportsUsedOnly" value="used">仅已用块</option>
                    <option value="skip">跳过分区</option>
                  </select>
                </td>
              </tr>
              <tr v-if="disk.partitions.length === 0"><td colspan="5" class="empty-row">未检测到分区，将按整个磁盘复制。</td></tr>
            </tbody>
          </table>
        </div>

        <div class="output-grid">
          <div class="panel output-panel">
            <div class="mini-heading"><strong>输出格式</strong><span>选择目标虚拟化平台</span></div>
            <div class="format-grid">
              <button v-for="item in [
                { id: 'vmdk', title: 'VMDK', sub: 'VMware' },
                { id: 'vhd', title: 'VHD', sub: 'Hyper-V' },
                { id: 'vdi', title: 'VDI', sub: 'VirtualBox' },
                { id: 'dd', title: 'DD', sub: 'Raw image' }
              ]" :key="item.id" :class="{ selected: format === item.id }" @click="selectFormat(item.id)">
                <strong>{{ item.title }}</strong><small>{{ item.sub }}</small>
              </button>
            </div>
          </div>

          <div class="panel output-panel">
            <div class="mini-heading"><strong>保存位置</strong><span>镜像文件写入本机磁盘</span></div>
            <button class="path-picker" @click="chooseOutput">
              <span class="folder-icon">▱</span>
              <span><small>{{ output ? '输出文件' : '尚未选择路径' }}</small><strong>{{ output || '点击选择保存位置' }}</strong></span>
              <em>浏览…</em>
            </button>
            <label class="buffer-control">
              <span><strong>I/O 缓冲区</strong><small>较大的缓冲区可能提高高速网络吞吐</small></span>
              <span class="number-input"><input v-model.number="bufferMB" type="number" min="1" max="256" /><i>MB</i></span>
            </label>
          </div>
        </div>

        <div class="action-row settings-actions">
          <div class="selection-summary"><strong>{{ activePartitionCount }}</strong> 个{{ disk.partitions.length ? '分区' : '整盘区域' }}将写入 {{ format.toUpperCase() }} 镜像</div>
          <button class="button primary-button" :disabled="!canStart" @click="startBackup">开始创建镜像 <span class="arrow">→</span></button>
        </div>
      </section>

      <section v-else-if="step === 4" class="page-section progress-page">
        <div class="section-header compact">
          <div><span class="section-kicker">04 / IMAGING</span><h2>{{ done ? progress.stage : '正在创建磁盘镜像' }}</h2><p>{{ output }}</p></div>
          <span class="live-badge" :class="{ finished: done }"><i></i>{{ done ? '任务结束' : '实时传输' }}</span>
        </div>

        <div class="progress-hero" :class="{ success: done?.success, failed: done && !done.success }">
          <div class="progress-ring" :style="{ '--progress': `${progress.percent * 3.6}deg` }">
            <div><strong>{{ progress.percent.toFixed(1) }}<small>%</small></strong><span>{{ done?.success ? '完成' : done ? '停止' : '总进度' }}</span></div>
          </div>
          <div class="progress-main">
            <span class="current-stage">{{ progress.stage }}</span>
            <div class="linear-progress"><i :style="{ width: `${progress.percent}%` }"></i></div>
            <div class="progress-size"><strong>{{ formatBytes(progress.done) }}</strong><span>/ {{ formatBytes(progress.total) }}</span></div>
          </div>
        </div>

        <div class="metric-grid">
          <div class="metric-card"><small>当前速度</small><strong>{{ progress.speedMBps.toFixed(1) }} <em>MB/s</em></strong><span>平滑实时速率</span></div>
          <div class="metric-card"><small>实际传输</small><strong>{{ formatBytes(progress.dataWritten) }}</strong><span>不包含稀疏空白块</span></div>
          <div class="metric-card"><small>预计剩余</small><strong>{{ done ? '—' : formatETA(progress.etaSeconds) }}</strong><span>根据当前速度计算</span></div>
        </div>

        <div class="log-panel">
          <div class="log-heading"><strong>任务日志</strong><span>{{ logs.length }} 条事件</span></div>
          <div class="log-content">
            <p v-if="logs.length === 0"><span>···</span> 正在等待首个数据块</p>
            <p v-for="(line, index) in logs" :key="index"><span>›</span> {{ line }}</p>
          </div>
        </div>

        <div v-if="done" class="result-banner" :class="{ success: done.success, cancelled: done.cancelled }">
          <div class="result-icon">{{ done.success ? '✓' : done.cancelled ? '■' : '!' }}</div>
          <div>
            <strong>{{ done.success ? '磁盘镜像创建完成' : done.cancelled ? '备份任务已取消' : '备份任务失败' }}</strong>
            <p>{{ done.success ? done.output : done.cancelled ? '未完成的镜像文件已保留，可根据需要手动删除。' : done.error }}</p>
          </div>
          <div class="result-actions">
            <button v-if="done.success" class="button ghost-button" @click="revealOutput">在资源管理器中显示</button>
            <button class="button primary-button" @click="done.success ? step = 3 : disconnectHost()">{{ done.success ? '创建另一个镜像' : '断开并返回连接' }}</button>
          </div>
        </div>
        <div v-else class="cancel-row">
          <span>关闭窗口或取消任务不会自动删除未完成的输出文件。</span>
          <button class="button danger-button" :disabled="cancelling" @click="cancelBackup">{{ cancelling ? '正在取消…' : '取消备份' }}</button>
        </div>
      </section>
    </main>

    <div v-if="showAbout" class="modal-backdrop" @click.self="showAbout = false">
      <section class="about-dialog" role="dialog" aria-modal="true" aria-labelledby="about-title">
        <button class="about-close" title="关闭" @click="showAbout = false">×</button>
        <div class="about-logo"><img src="/appicon.png" alt="SSH Imager 图标" /></div>
        <span class="section-kicker">OPEN SOURCE SOFTWARE</span>
        <h2 id="about-title">SSH Imager</h2>
        <p>通过 SSH 对远程 Linux 主机的物理磁盘进行镜像，并在本机直接生成 VMDK、VHD、VDI 或 DD 文件。</p>
        <dl class="about-meta">
          <div><dt>版本</dt><dd>v{{ version }}</dd></div>
          <div><dt>开源许可</dt><dd>MIT License</dd></div>
        </dl>
        <button class="project-link" @click="openProjectPage">
          <span>GitHub</span>
          <strong>github.com/threatexpert/sshimager</strong>
          <em>在浏览器中打开 ↗</em>
        </button>
        <small class="about-copyright">© 2026 threatexpert.cn</small>
      </section>
    </div>
  </div>
</template>
