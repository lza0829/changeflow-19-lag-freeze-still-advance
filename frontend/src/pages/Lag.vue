<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, putJSON } from '../api'

const config = ref(null)
const runs = ref([])
const tickets = ref([])
const threshold = ref(600)
const policy = ref('freeze_cursor')
const err = ref('')
const saved = ref(false)
const busy = ref(false)

async function refresh() {
  config.value = await getJSON('/api/lag-sla/config')
  threshold.value = config.value.lag_sla_threshold_seconds
  policy.value = config.value.lag_sla_policy
  runs.value = await getJSON('/api/lag-sla/runs')
  tickets.value = await getJSON('/api/lag-sla/tickets')
}

async function saveConfig() {
  busy.value = true
  err.value = ''
  saved.value = false
  try {
    config.value = await putJSON('/api/lag-sla/config', {
      threshold_seconds: Number(threshold.value),
      policy: policy.value,
    })
    saved.value = true
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function fmtLag(sec) {
  if (sec === null || sec === undefined) return '—'
  if (sec >= 3600) return `${(sec / 3600).toFixed(2)}h`
  if (sec >= 60) return `${(sec / 60).toFixed(1)}m`
  return `${sec}s`
}

function actionTag(action) {
  if (action === 'freeze_cursor') return 'danger'
  if (action === 'ticket_and_advance') return 'warn'
  return 'ok'
}

onMounted(refresh)
</script>
<template>
  <div class="page">
    <h1>滞后 SLA 与游标冻结</h1>
    <p class="muted">
      口径：lag = 落地时刻 − event_ts（秒），样本仅含本批拟应用事件，分位数 nearest-rank。
      超阈后两策略互斥：冻结游标（不落 Sink、不抬水位）或记票放行（推进但留违约票）。
    </p>

    <div class="panel">
      <h3>SLA 配置（保存到本地 SQLite）</h3>
      <div class="row-actions">
        <label>超阈阈值（秒）
          <input v-model.number="threshold" type="number" min="1" style="width: 9rem" />
        </label>
        <label>超阈策略
          <select v-model="policy">
            <option value="freeze_cursor">freeze_cursor · 冻结游标</option>
            <option value="ticket_and_advance">ticket_and_advance · 记票放行</option>
          </select>
        </label>
        <button :disabled="busy" @click="saveConfig">{{ busy ? '保存中…' : '保存配置' }}</button>
        <span v-if="saved" class="tag ok">已保存</span>
        <span v-if="err" class="tag danger">{{ err }}</span>
      </div>
    </div>

    <div class="panel">
      <h3>各次分析滞后分布</h3>
      <table>
        <thead>
          <tr>
            <th>run</th><th>流</th><th>样本数</th><th>min</th><th>中位</th><th>P95</th><th>max</th>
            <th>阈值</th><th>超阈</th><th>策略</th><th>动作</th><th>游标推进</th><th>落地时刻</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in runs" :key="r.run_id">
            <td>{{ r.run_id }}</td>
            <td>{{ r.stream_id }}</td>
            <td>{{ r.sample_count }}</td>
            <td>{{ fmtLag(r.lag_min_seconds) }}</td>
            <td>{{ fmtLag(r.lag_median_seconds) }}</td>
            <td>{{ fmtLag(r.lag_p95_seconds) }}</td>
            <td>{{ fmtLag(r.lag_max_seconds) }}</td>
            <td>{{ fmtLag(r.threshold_seconds) }}</td>
            <td>
              <span class="tag" :class="r.over_threshold_count ? 'danger' : 'ok'">
                {{ r.over_threshold_count }}
              </span>
            </td>
            <td>{{ r.policy }}</td>
            <td><span class="tag" :class="actionTag(r.action)">{{ r.action }}</span></td>
            <td>
              <span class="tag" :class="r.cursor_advanced ? 'ok' : 'warn'">
                {{ r.cursor_advanced ? '是' : '否' }}
              </span>
            </td>
            <td>{{ r.land_ts }}</td>
          </tr>
          <tr v-if="!runs.length"><td colspan="13" class="muted">暂无分析记录，先到应用台执行一次 Apply</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel">
      <h3>违约票（超阈事件逐张可查）</h3>
      <table>
        <thead>
          <tr>
            <th>票 ID</th><th>run</th><th>流</th><th>seq</th><th>entity</th>
            <th>event_ts</th><th>lag</th><th>阈值</th><th>策略动作</th><th>开票时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="t in tickets" :key="t.id">
            <td>{{ t.id }}</td>
            <td>{{ t.run_id }}</td>
            <td>{{ t.stream_id }}</td>
            <td>{{ t.seq }}</td>
            <td>{{ t.entity_key }}</td>
            <td>{{ t.event_ts }}</td>
            <td><span class="tag danger">{{ fmtLag(t.lag_seconds) }}</span></td>
            <td>{{ fmtLag(t.threshold_seconds) }}</td>
            <td><span class="tag" :class="actionTag(t.action)">{{ t.action }}</span></td>
            <td>{{ t.created_at }}</td>
          </tr>
          <tr v-if="!tickets.length"><td colspan="10" class="muted">暂无违约票</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
