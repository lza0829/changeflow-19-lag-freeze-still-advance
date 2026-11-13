<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'
const streams = ref([])
const streamId = ref(1)
const last = ref(null)
const runs = ref([])
const lagConfig = ref(null)
const err = ref('')
const busy = ref(false)
async function refreshRuns() {
  runs.value = await getJSON('/api/runs')
}
async function runApply() {
  busy.value = true
  err.value = ''
  try {
    last.value = await postJSON('/api/apply', { stream_id: streamId.value })
    streams.value = await getJSON('/api/streams')
    await refreshRuns()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}
function runTag(r) {
  if (r.status === 'succeeded') return 'ok'
  if (r.status === 'frozen') return 'danger'
  return 'warn'
}
function fmtLag(sec) {
  if (sec === null || sec === undefined) return '—'
  if (sec >= 3600) return `${(sec / 3600).toFixed(2)}h`
  if (sec >= 60) return `${(sec / 60).toFixed(1)}m`
  return `${sec}s`
}
onMounted(async () => {
  streams.value = await getJSON('/api/streams')
  if (streams.value.length) streamId.value = streams.value[0].id
  lagConfig.value = await getJSON('/api/lag-sla/config')
  await refreshRuns()
})
</script>
<template>
  <div class="page">
    <h1>应用台</h1>
    <p class="muted">
      按游标连续序应用；缺口默认停并进隔离区。滞后 SLA 超阈时按当前策略冻结游标或记票放行
      （当前阈值 {{ lagConfig?.lag_sla_threshold_seconds }}s · 策略 {{ lagConfig?.lag_sla_policy }}，
      <router-link to="/lag-sla">去配置</router-link>）。
    </p>
    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId">
          <option v-for="s in streams" :key="s.id" :value="s.id">{{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}）</option>
        </select>
      </label>
      <button :disabled="busy" @click="runApply">{{ busy ? '应用中…' : '执行 Apply' }}</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="last">
      <h3>最近一次</h3>
      <p>
        run #{{ last.id }} ·
        <span class="tag" :class="runTag(last)">{{ last.status }}</span>
        · loaded {{ last.loaded_count }} / applied {{ last.applied_count }} /
        quarantined {{ last.quarantined_count }} / skipped {{ last.skipped_count }}
        · cursor {{ last.cursor_before }} → {{ last.cursor_after }}
      </p>
      <div v-if="last.report?.lag_sla" class="panel" style="margin-bottom:0.5rem">
        <h3>滞后 SLA 裁决（与滞后页同源）</h3>
        <p>
          落地 {{ last.report.lag_sla.land_ts }} · 阈值
          {{ fmtLag(last.report.lag_sla.threshold_seconds) }} · 策略
          {{ last.report.lag_sla.policy }}
        </p>
        <p>
          样本 {{ last.report.lag_sla.summary.sample_count }} 条 ·
          中位 {{ fmtLag(last.report.lag_sla.summary.lag_median_seconds) }} ·
          P95 {{ fmtLag(last.report.lag_sla.summary.lag_p95_seconds) }} ·
          超阈
          <span class="tag" :class="last.report.lag_sla.breach ? 'danger' : 'ok'">
            {{ last.report.lag_sla.summary.over_threshold_count }} 条
          </span>
        </p>
        <p>
          动作 <span class="tag" :class="last.report.lag_sla.action === 'freeze_cursor' ? 'danger' : (last.report.lag_sla.action === 'ticket_and_advance' ? 'warn' : 'ok')">{{ last.report.lag_sla.action }}</span>
          · 游标推进
          <span class="tag" :class="last.report.lag_sla.cursor_advanced ? 'ok' : 'warn'">
            {{ last.report.lag_sla.cursor_advanced ? '是' : '否' }}
          </span>
          <template v-if="last.report.lag_sla.over_threshold_seqs.length">
            · 超阈 seq [{{ last.report.lag_sla.over_threshold_seqs.join(', ') }}]
          </template>
          <template v-if="last.report.lag_sla.frozen_seqs.length">
            · 冻结 seq [{{ last.report.lag_sla.frozen_seqs.join(', ') }}]（未写 Sink）
          </template>
        </p>
        <p v-if="last.status === 'frozen'" class="tag danger">
          本批因滞后超阈被冻结：游标未抬高，已写账本/违约票，可到滞后 SLA 页查票或调整策略后重跑
        </p>
      </div>
      <pre class="mono">{{ JSON.stringify(last.report, null, 2) }}</pre>
    </div>
    <div class="panel">
      <h3>历史 Runs</h3>
      <table>
        <thead>
          <tr><th>ID</th><th>流</th><th>状态</th><th>loaded</th><th>applied</th><th>quarantine</th><th>skipped</th><th>cursor</th><th>SLA 动作</th><th>时间</th></tr>
        </thead>
        <tbody>
          <tr v-for="r in runs" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td><span class="tag" :class="runTag(r)">{{ r.status }}</span></td>
            <td>{{ r.loaded_count }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.quarantined_count }}</td>
            <td>{{ r.skipped_count }}</td>
            <td>{{ r.cursor_before }}→{{ r.cursor_after }}</td>
            <td>{{ r.report?.lag_sla?.action || '—' }}</td>
            <td>{{ r.created_at }}</td>
          </tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
