const state = {
  pollutant: "pm25",
  metric: "hotspot_score",
  borough: "All",
  summary: null,
  zones: null,
  meta: null,
  zoneLayer: null,
  selectedZoneId: null,
  timeseriesCache: new Map(),
  predictionCache: new Map(),
  trendChart: null,
  timeChart: null,
  analysisChart: null,
  boroughChart: null,
  predictionChart: null,
  predictionResidualChart: null,
  predictionMeta: null,
  predictionSummary: null,
};

const metricLabels = {
  mean_turnover: "Average Traffic Activity",
  mean_pollution: "Average Pollution Level",
  best_lag_corr: "Strongest Delayed Relationship",
  hotspot_score: "Attention Score",
};

const metricDescriptions = {
  mean_turnover: "Average hourly taxi pickups and dropoffs.",
  mean_pollution: "Average interpolated pollution level.",
  best_lag_corr: "Strongest traffic-pollution relationship after a time delay.",
  hotspot_score: "Combined priority indicator based on delay strength, shared peaks, and pollution level.",
};

const pollutantLabel = (value) => {
  const special = {
    pm25: "PM2.5",
    pm1: "PM1",
    no2: "NO2",
    nox: "NOx",
    o3: "O3",
    co: "CO",
    no: "NO",
    um003: "UM003",
  };
  return special[value] || value;
};

const fmt = {
  number(value, digits = 2) {
    if (value == null || Number.isNaN(value)) return "—";
    return Number(value).toFixed(digits);
  },
  integer(value) {
    if (value == null || Number.isNaN(value)) return "—";
    return Math.round(Number(value)).toLocaleString();
  },
  percent(value) {
    if (value == null || Number.isNaN(value)) return "—";
    return `${(Number(value) * 100).toFixed(1)}%`;
  },
};

function formatTooltipValue(value, valueType = "default") {
  if (value == null || Number.isNaN(Number(value))) return "—";
  const numeric = Number(value);
  const digits =
    valueType === "traffic"
      ? 1
      : valueType === "pollution" || valueType === "change"
        ? 2
        : Math.abs(numeric) >= 100
          ? 1
          : 2;
  return numeric.toFixed(digits);
}

function formatAxisTooltip(params, valueTypes = {}) {
  if (!params?.length) return "";
  const lines = [`<strong>${params[0].axisValueLabel}</strong>`];
  params.forEach((item) => {
    const valueType = valueTypes[item.seriesName] || "default";
    lines.push(
      `${item.marker}${item.seriesName}: <strong>${formatTooltipValue(item.value, valueType)}</strong>`
    );
  });
  return lines.join("<br/>");
}

function computeRollingAverage(values, windowSize = 7) {
  return values.map((_, index) => {
    const start = Math.max(0, index - windowSize + 1);
    const slice = values.slice(start, index + 1).filter((value) => value != null && !Number.isNaN(value));
    if (!slice.length) return null;
    return slice.reduce((sum, value) => sum + Number(value), 0) / slice.length;
  });
}

function buildLagAdjustedTrafficSeries(zoneSeries, lagHours) {
  const lag = Math.max(0, Number(lagHours || 0));
  return zoneSeries.map((_, index) => {
    const shiftedIndex = index - lag;
    if (shiftedIndex < 0) return null;
    return Number(zoneSeries[shiftedIndex].turnover ?? null);
  });
}

function getRecentPredictionWindow(records, days = 7) {
  if (!records?.length) return [];
  const end = new Date(records[records.length - 1].target_datetime);
  const start = new Date(end);
  start.setDate(start.getDate() - days + 1);
  return records.filter((item) => new Date(item.target_datetime) >= start);
}

function getFilteredRecords() {
  const records = state.summary.records.filter((record) => record.parameter === state.pollutant);
  if (state.borough === "All") return records;
  return records.filter((record) => record.borough === state.borough);
}

function getSelectedRecord() {
  const records = getFilteredRecords();
  if (!records.length) return null;
  const targetId = state.selectedZoneId ?? records[0].zone_id;
  return records.find((record) => record.zone_id === targetId) || records[0];
}

function populateControls() {
  const pollutantSelect = document.getElementById("pollutant-select");
  const metricSelect = document.getElementById("metric-select");
  const boroughSelect = document.getElementById("borough-select");

  state.summary.pollutants.forEach((pollutant) => {
    const option = document.createElement("option");
    option.value = pollutant;
    option.textContent = pollutantLabel(pollutant);
    pollutantSelect.appendChild(option);
  });

  state.summary.metrics.forEach((metric) => {
    const option = document.createElement("option");
    option.value = metric.id;
    option.textContent = metricLabels[metric.id] || metric.label;
    metricSelect.appendChild(option);
  });

  const boroughs = ["All", ...new Set(state.summary.records.map((record) => record.borough).filter(Boolean))];
  boroughs.sort((a, b) => a.localeCompare(b));
  boroughs.forEach((borough) => {
    const option = document.createElement("option");
    option.value = borough;
    option.textContent = borough;
    boroughSelect.appendChild(option);
  });

  pollutantSelect.value = state.pollutant;
  metricSelect.value = state.metric;
  boroughSelect.value = state.borough;

  pollutantSelect.addEventListener("change", async (event) => {
    state.pollutant = event.target.value;
    state.selectedZoneId = null;
    renderAll();
    await updateDetailPanel();
  });

  metricSelect.addEventListener("change", (event) => {
    state.metric = event.target.value;
    renderAll();
  });

  boroughSelect.addEventListener("change", async (event) => {
    state.borough = event.target.value;
    state.selectedZoneId = null;
    renderAll();
    await updateDetailPanel();
  });
}

function setDatasetMeta() {
  const range = state.meta.analysis_window;
  if (range?.start && range?.end) {
    document.getElementById(
      "dataset-range"
    ).textContent = `Formal analysis window: ${range.start.slice(0, 10)} to ${range.end.slice(0, 10)}`;
  } else {
    document.getElementById("dataset-range").textContent = "Formal analysis window: unavailable";
  }
  document.getElementById(
    "dataset-summary"
  ).textContent = `${state.meta.zone_count} taxi zones · ${state.meta.pollutant_count} pollutant series`;
}

function setContextTitles() {
  const pollutant = pollutantLabel(state.pollutant);
  document.getElementById("overview-title").textContent = `${metricLabels[state.metric]} of ${pollutant}`;
  document.getElementById("ranking-title").textContent = `Top Zones by ${metricLabels[state.metric]} for ${pollutant}`;
  document.getElementById("analysis-title").textContent = `Pattern Summary for ${pollutant}`;
}

function buildMap() {
  const map = L.map("map", {
    zoomControl: true,
    attributionControl: false,
    preferCanvas: true,
    scrollWheelZoom: false,
  });
  map.fitBounds([
    [state.meta.bounds[1], state.meta.bounds[0]],
    [state.meta.bounds[3], state.meta.bounds[2]],
  ]);
  map.setMinZoom(map.getZoom());

  state.zoneLayer = L.geoJSON(state.zones, {
    style: featureStyle,
    onEachFeature(feature, layer) {
      layer.on({
        mouseover: (event) => {
          const record = lookupRecord(feature.properties.zone_id);
          event.target.setStyle({ weight: 2.2, color: "#18201c", fillOpacity: 0.92 });
          if (record) {
            layer.bindTooltip(zoneTooltipHtml(record), {
              sticky: true,
              className: "zone-tooltip",
              direction: "top",
            });
            layer.openTooltip();
          }
        },
        mouseout: (event) => {
          state.zoneLayer.resetStyle(event.target);
        },
        click: async () => {
          state.selectedZoneId = feature.properties.zone_id;
          highlightSelectedZone();
          await updateDetailPanel();
        },
      });
    },
  }).addTo(map);

  state.map = map;
}

function lookupRecord(zoneId) {
  return getFilteredRecords().find((record) => record.zone_id === zoneId) || null;
}

function getMetricConfig() {
  return state.summary.metrics.find((metric) => metric.id === state.metric);
}

function getMetricDomain(records) {
  const values = records
    .map((record) => record[state.metric])
    .filter((value) => value != null && !Number.isNaN(Number(value)))
    .map(Number);
  if (!values.length) return [0, 1];
  return [Math.min(...values), Math.max(...values)];
}

function interpolateColor(start, end, factor) {
  const s = start.match(/\w\w/g).map((hex) => parseInt(hex, 16));
  const e = end.match(/\w\w/g).map((hex) => parseInt(hex, 16));
  const blended = s.map((value, index) => Math.round(value + (e[index] - value) * factor));
  return `#${blended.map((value) => value.toString(16).padStart(2, "0")).join("")}`;
}

function scaleColor(value, min, max, scale) {
  if (value == null || Number.isNaN(Number(value))) return "#d9d1c4";
  if (scale === "diverging") {
    const extent = Math.max(Math.abs(min), Math.abs(max), 0.001);
    const normalized = Math.max(-1, Math.min(1, Number(value) / extent));
    if (normalized < 0) {
      return interpolateColor("d8e7ea", "2b5b63", Math.abs(normalized));
    }
    return interpolateColor("f0dccb", "bb6a3c", normalized);
  }
  const safeMax = max === min ? min + 1 : max;
  const factor = (Number(value) - min) / (safeMax - min);
  return interpolateColor("efe7d9", "8f4d33", Math.max(0, Math.min(1, factor)));
}

function featureStyle(feature) {
  const record = lookupRecord(feature.properties.zone_id);
  const records = getFilteredRecords();
  const [min, max] = getMetricDomain(records);
  const config = getMetricConfig();
  const fillColor = scaleColor(record?.[state.metric], min, max, config?.scale);
  const selected = state.selectedZoneId === feature.properties.zone_id;
  return {
    fillColor,
    weight: selected ? 2.4 : 0.85,
    opacity: 1,
    color: selected ? "#18201c" : "rgba(24, 32, 28, 0.32)",
    fillOpacity: selected ? 0.95 : 0.84,
  };
}

function zoneTooltipHtml(record) {
  return `
    <div>
      <h3>${record.zone_name}</h3>
      <p>${record.borough} · ${pollutantLabel(record.parameter)}</p>
      <p>${metricLabels[state.metric]}: <strong>${fmt.number(record[state.metric])}</strong></p>
      <p>Average traffic activity: <strong>${fmt.number(record.mean_turnover)}</strong></p>
      <p>Average pollution level: <strong>${fmt.number(record.mean_pollution)}</strong></p>
    </div>
  `;
}

function highlightSelectedZone() {
  state.zoneLayer.setStyle(featureStyle);
}

function renderLegend() {
  const legend = document.getElementById("legend");
  const records = getFilteredRecords();
  const [min, max] = getMetricDomain(records);
  const config = getMetricConfig();
  const gradient =
    config.scale === "diverging"
      ? "linear-gradient(90deg, #2b5b63 0%, #dce6e5 50%, #bb6a3c 100%)"
      : "linear-gradient(90deg, #efe7d9 0%, #8f4d33 100%)";
  legend.innerHTML = `
    <div class="legend-swatch" style="background:${gradient}"></div>
    <div class="legend-labels">
      <span>${fmt.number(min)}</span>
      <span>${metricLabels[state.metric]}</span>
      <span>${fmt.number(max)}</span>
    </div>
    <p class="panel-note">${metricDescriptions[state.metric]}</p>
  `;
}

function renderRanking() {
  const list = document.getElementById("ranking-list");
  const records = [...getFilteredRecords()]
    .sort((a, b) => Number(b[state.metric] ?? -Infinity) - Number(a[state.metric] ?? -Infinity))
    .slice(0, 10);

  list.innerHTML = "";
  records.forEach((record, index) => {
    const item = document.createElement("li");
    item.className = "ranking-item";
    item.innerHTML = `
      <span class="ranking-index">${String(index + 1).padStart(2, "0")}</span>
      <div class="ranking-meta">
        <strong>${record.zone_name}</strong>
        <span>${record.borough}</span>
      </div>
      <span class="ranking-value">${fmt.number(record[state.metric])}</span>
    `;
    item.addEventListener("click", async () => {
      state.selectedZoneId = record.zone_id;
      highlightSelectedZone();
      await updateDetailPanel();
    });
    list.appendChild(item);
  });
}

function renderKpis() {
  const records = getFilteredRecords();
  const best = [...records].sort((a, b) => Number(b.hotspot_score) - Number(a.hotspot_score))[0];
  const strongest = [...records].sort((a, b) => Number(b.best_lag_corr) - Number(a.best_lag_corr))[0];

  document.getElementById("kpi-top-zone").textContent = best ? best.zone_name : "—";
  document.getElementById("kpi-best-corr").textContent = strongest ? fmt.number(strongest.best_lag_corr) : "—";
  document.getElementById("kpi-zone-count").textContent = fmt.integer(records.length);
}

async function loadTimeseries(parameter, zoneId) {
  const cacheKey = `${parameter}:${zoneId}`;
  if (!state.timeseriesCache.has(cacheKey)) {
    const response = await fetch(`./public/data/timeseries/${parameter}/${zoneId}.json`);
    if (!response.ok) {
      throw new Error(`Failed to load timeseries for ${parameter} zone ${zoneId}`);
    }
    const payload = await response.json();
    state.timeseriesCache.set(cacheKey, payload.records);
  }
  return state.timeseriesCache.get(cacheKey);
}

function lookupPredictionRecord(zoneId) {
  return state.predictionSummary?.records.find((record) => record.zone_id === zoneId) || null;
}

async function loadPredictionSeries(zoneId) {
  if (!state.predictionMeta) return null;
  const cacheKey = String(zoneId);
  if (!state.predictionCache.has(cacheKey)) {
    const response = await fetch(`./public/data/predictions/pm25/zones/${zoneId}.json`);
    if (!response.ok) {
      state.predictionCache.set(cacheKey, null);
      return null;
    }
    const payload = await response.json();
    state.predictionCache.set(cacheKey, payload);
  }
  return state.predictionCache.get(cacheKey);
}

async function updateDetailPanel() {
  const record = getSelectedRecord();
  if (!record) return;
  state.selectedZoneId = record.zone_id;
  highlightSelectedZone();

  document.getElementById("detail-title").textContent = `${record.zone_name} · ${record.borough}`;
  document.getElementById("detail-subtitle").textContent = `${pollutantLabel(record.parameter)} · Click another zone or ranking item to compare.`;
  document.getElementById("detail-summary").innerHTML = `
    <div><dt>Attention Score</dt><dd>${fmt.number(record.hotspot_score)}</dd></div>
    <div><dt>Delay Time</dt><dd>${fmt.integer(record.best_lag_hours)} h</dd></div>
    <div><dt>Delay Strength</dt><dd>${fmt.number(record.best_lag_corr)}</dd></div>
    <div><dt>Average Traffic Activity</dt><dd>${fmt.number(record.mean_turnover)}</dd></div>
    <div><dt>Average Pollution Level</dt><dd>${fmt.number(record.mean_pollution)}</dd></div>
    <div><dt>Shared Peaks</dt><dd>${fmt.percent(record.cooccur_rate)}</dd></div>
  `;
  document.getElementById("detail-footnote").textContent = `${fmt.integer(record.sample_count)} hourly samples were used for this zone and pollutant.`;
  document.getElementById("detail-subtitle").textContent = `${pollutantLabel(record.parameter)} · Loading local time series…`;
  const zoneSeries = await loadTimeseries(state.pollutant, record.zone_id);
  document.getElementById("detail-subtitle").textContent = `${pollutantLabel(record.parameter)} · Click another zone or ranking item to compare.`;
  renderDetailCharts(zoneSeries, record);
  await updatePredictionPanel(record);
}

function aggregateDailySeries(zoneSeries) {
  const grouped = new Map();
  zoneSeries.forEach((item) => {
    const day = item.datetime_hour.slice(0, 10);
    if (!grouped.has(day)) {
      grouped.set(day, { day, count: 0, turnover: 0, pollution: 0 });
    }
    const row = grouped.get(day);
    row.count += 1;
    row.turnover += Number(item.turnover ?? 0);
    row.pollution += Number(item.pollution ?? 0);
  });
  return [...grouped.values()].map((row) => ({
    day: row.day,
    turnover: row.count ? row.turnover / row.count : 0,
    pollution: row.count ? row.pollution / row.count : 0,
  }));
}

function getFocusWindow(zoneSeries, days = 30) {
  if (!zoneSeries.length) return [];
  const end = new Date(zoneSeries[zoneSeries.length - 1].datetime_hour);
  const start = new Date(end);
  start.setDate(start.getDate() - days + 1);
  return zoneSeries.filter((item) => new Date(item.datetime_hour) >= start);
}

function renderDetailCharts(zoneSeries, record) {
  const dailySeries = aggregateDailySeries(zoneSeries);
  const focusSeries = getFocusWindow(zoneSeries, 14);
  document.getElementById("lag-summary").textContent = `Best delay: ${fmt.integer(record.best_lag_hours)} h · Delay strength: ${fmt.number(record.best_lag_corr)}`;
  document.getElementById("focus-window-label").textContent =
    focusSeries.length
      ? `Hourly values for ${focusSeries[0].datetime_hour.slice(0, 10)} to ${focusSeries[focusSeries.length - 1].datetime_hour.slice(0, 10)}.`
      : "Hourly values for the most recent 14 days.";
  renderTrendChart(dailySeries, record);
  renderTimeseriesChart(focusSeries, record);
}

function renderTrendChart(dailySeries, record) {
  if (!state.trendChart) {
    state.trendChart = echarts.init(document.getElementById("trend-chart"));
  }
  const axisLabels = dailySeries.map((item) => item.day.slice(5));
  const tickStep = Math.max(1, Math.floor(dailySeries.length / 10));
  const trafficValues = dailySeries.map((item) => Number(item.turnover));
  const pollutionValues = dailySeries.map((item) => Number(item.pollution));
  const trafficTrend = computeRollingAverage(trafficValues, 7);
  const pollutionTrend = computeRollingAverage(pollutionValues, 7);
  state.trendChart.setOption({
    animationDuration: 500,
    backgroundColor: "transparent",
    grid: [
      { left: 54, right: 18, top: 34, height: 132 },
      { left: 54, right: 18, top: 220, height: 132 },
    ],
    tooltip: {
      trigger: "axis",
      formatter(params) {
        return formatAxisTooltip(params, {
          "Traffic Daily": "traffic",
          "Traffic 7-day Trend": "traffic",
          [`${pollutantLabel(record.parameter)} Daily`]: "pollution",
          [`${pollutantLabel(record.parameter)} 7-day Trend`]: "pollution",
        });
      },
    },
    graphic: {
      type: "text",
      right: 18,
      top: 6,
      style: {
        text: "Faint line = daily value · bold line = 7-day trend",
        fill: "#5e675f",
        font: '12px "Avenir Next", "Segoe UI", sans-serif',
      },
    },
    xAxis: [
      {
        type: "category",
        gridIndex: 0,
        data: axisLabels,
        axisLabel: { show: false },
        axisTick: { show: false },
        axisLine: { lineStyle: { color: "rgba(31,39,34,0.18)" } },
      },
      {
        type: "category",
        gridIndex: 1,
        data: axisLabels,
        axisLabel: {
          color: "#5e675f",
          formatter(value, index) {
            if (index % tickStep !== 0 && index !== axisLabels.length - 1) return "";
            return value;
          },
        },
        axisLine: { lineStyle: { color: "rgba(31,39,34,0.18)" } },
      },
    ],
    yAxis: [
      {
        type: "value",
        gridIndex: 0,
        name: "Daily Traffic",
        nameTextStyle: { color: "#5e675f", padding: [0, 0, 8, 0] },
        axisLabel: {
          color: "#5e675f",
          formatter(value) {
            return formatTooltipValue(value, "traffic");
          },
        },
        splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
      },
      {
        type: "value",
        gridIndex: 1,
        name: `Daily ${pollutantLabel(record.parameter)}`,
        nameTextStyle: { color: "#5e675f", padding: [0, 0, 8, 0] },
        axisLabel: {
          color: "#5e675f",
          formatter(value) {
            return formatTooltipValue(value, "pollution");
          },
        },
        splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
      },
    ],
    series: [
      {
        name: "Traffic Daily",
        type: "line",
        smooth: true,
        showSymbol: false,
        xAxisIndex: 0,
        yAxisIndex: 0,
        lineStyle: { width: 1.2, color: "rgba(143,77,51,0.28)" },
        data: trafficValues,
      },
      {
        name: "Traffic 7-day Trend",
        type: "line",
        smooth: true,
        showSymbol: false,
        xAxisIndex: 0,
        yAxisIndex: 0,
        lineStyle: { width: 2.4, color: "#8f4d33" },
        data: trafficTrend,
      },
      {
        name: `${pollutantLabel(record.parameter)} Daily`,
        type: "line",
        smooth: true,
        showSymbol: false,
        xAxisIndex: 1,
        yAxisIndex: 1,
        lineStyle: { width: 1.2, color: "rgba(43,91,99,0.28)" },
        data: pollutionValues,
      },
      {
        name: `${pollutantLabel(record.parameter)} 7-day Trend`,
        type: "line",
        smooth: true,
        showSymbol: false,
        xAxisIndex: 1,
        yAxisIndex: 1,
        lineStyle: { width: 2.4, color: "#2b5b63" },
        data: pollutionTrend,
      },
    ],
  });
}

function renderTimeseriesChart(zoneSeries, record) {
  if (!state.timeChart) {
    state.timeChart = echarts.init(document.getElementById("timeseries-chart"));
  }
  const axisLabels = zoneSeries.map((item) => item.datetime_hour.slice(5, 16));
  const tickStep = Math.max(1, Math.floor(zoneSeries.length / 10));
  const shiftedTraffic = buildLagAdjustedTrafficSeries(zoneSeries, record.best_lag_hours);
  const rawTrafficLabel = "Traffic (raw)";
  const shiftedLabel = `Lagged traffic (${fmt.integer(record.best_lag_hours)}h)`;
  state.timeChart.setOption({
    animationDuration: 600,
    backgroundColor: "transparent",
    grid: { left: 48, right: 44, top: 74, bottom: 56 },
    legend: {
      top: 28,
      selected: {
        [rawTrafficLabel]: false,
      },
      textStyle: { color: "#5e675f", fontFamily: "Avenir Next, Segoe UI, sans-serif" },
    },
    tooltip: {
      trigger: "axis",
      formatter(params) {
        return formatAxisTooltip(params, {
          [rawTrafficLabel]: "traffic",
          [shiftedLabel]: "traffic",
          [pollutantLabel(record.parameter)]: "pollution",
        });
      },
    },
    dataZoom: [
      {
        type: "inside",
        start: 0,
        end: 100,
      },
      {
        type: "slider",
        height: 16,
        bottom: 12,
        borderColor: "rgba(31,39,34,0.08)",
        fillerColor: "rgba(43,91,99,0.12)",
        backgroundColor: "rgba(31,39,34,0.04)",
        handleStyle: {
          color: "#d7c6b5",
          borderColor: "rgba(31,39,34,0.12)",
        },
        moveHandleStyle: {
          color: "#d7c6b5",
        },
      },
    ],
    xAxis: {
      type: "category",
      data: axisLabels,
      axisLabel: {
        color: "#5e675f",
        rotate: 0,
        formatter(value, index) {
          if (index % tickStep !== 0 && index !== axisLabels.length - 1) return "";
          return value.slice(0, 5);
        },
      },
      axisLine: { lineStyle: { color: "rgba(31,39,34,0.18)" } },
    },
    yAxis: [
      {
        type: "value",
        name: "Hourly Traffic",
        axisLabel: {
          color: "#5e675f",
          formatter(value) {
            return formatTooltipValue(value, "traffic");
          },
        },
        splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
      },
      {
        type: "value",
        name: pollutantLabel(record.parameter),
        axisLabel: {
          color: "#5e675f",
          formatter(value) {
            return formatTooltipValue(value, "pollution");
          },
        },
        splitLine: { show: false },
      },
    ],
    series: [
      {
        name: rawTrafficLabel,
        type: "line",
        smooth: true,
        showSymbol: false,
        symbol: "circle",
        symbolSize: 6,
        yAxisIndex: 0,
        lineStyle: { width: 0.9, color: "rgba(143,77,51,0.22)" },
        itemStyle: { color: "rgba(143,77,51,0.25)", borderColor: "rgba(143,77,51,0.25)" },
        emphasis: {
          focus: "series",
          itemStyle: { color: "#8f4d33", borderColor: "#8f4d33" },
          lineStyle: { width: 1.1, color: "#8f4d33" },
        },
        data: zoneSeries.map((item) => item.turnover),
      },
      {
        name: pollutantLabel(record.parameter),
        type: "line",
        smooth: true,
        showSymbol: false,
        symbol: "circle",
        symbolSize: 6,
        yAxisIndex: 1,
        lineStyle: { width: 2.2, color: "#2b5b63" },
        itemStyle: { color: "#2b5b63", borderColor: "#2b5b63" },
        emphasis: {
          focus: "series",
          itemStyle: { color: "#2b5b63", borderColor: "#2b5b63" },
          lineStyle: { width: 2.2, color: "#2b5b63" },
        },
        data: zoneSeries.map((item) => item.pollution),
      },
      {
        name: shiftedLabel,
        type: "line",
        smooth: true,
        showSymbol: false,
        yAxisIndex: 0,
        lineStyle: { width: 1.8, color: "#d39c77", type: "dashed" },
        itemStyle: { color: "#d39c77" },
        data: shiftedTraffic,
      },
    ],
  });
}

function renderPredictionPlaceholder(title, note) {
  document.getElementById("prediction-subtitle").textContent = title;
  document.getElementById("prediction-cards").innerHTML = "";
  document.getElementById("prediction-meta-line").textContent = "";
  document.getElementById("prediction-note").textContent = note;
  if (!state.predictionChart) {
    state.predictionChart = echarts.init(document.getElementById("prediction-chart"));
  }
  if (!state.predictionResidualChart) {
    state.predictionResidualChart = echarts.init(document.getElementById("prediction-residual-chart"));
  }
  state.predictionChart.clear();
  state.predictionResidualChart.clear();
  state.predictionChart.setOption({
    animation: false,
    graphic: {
      type: "text",
      left: "center",
      top: "middle",
      style: {
        text: note,
        fill: "#5e675f",
        font: '14px "Avenir Next", "Segoe UI", sans-serif',
        textAlign: "center",
        width: 320,
      },
    },
  });
  state.predictionResidualChart.setOption({ animation: false, series: [] });
}

function renderPredictionCards(globalMeta, zoneMeta) {
  const container = document.getElementById("prediction-cards");
  container.innerHTML = `
    <dl class="prediction-card">
      <dt>Prediction Goal</dt>
      <dd>Next-hour PM2.5 change</dd>
    </dl>
    <dl class="prediction-card">
      <dt>Look-back Period</dt>
      <dd>Previous ${fmt.integer(globalMeta.window_hours)} hours</dd>
    </dl>
    <dl class="prediction-card">
      <dt>Overall Avg Error</dt>
      <dd>${fmt.number(globalMeta.test_metrics.mae)}</dd>
    </dl>
    <dl class="prediction-card">
      <dt>This Zone Avg Error</dt>
      <dd>${fmt.number(zoneMeta.mae)}</dd>
    </dl>
  `;
  document.getElementById("prediction-meta-line").textContent =
    `Overall fit: ${fmt.number(globalMeta.test_metrics.r2, 3)} · This zone fit: ${fmt.number(zoneMeta.r2, 3)} · Test points: ${fmt.integer(zoneMeta.sample_count)}`;
}

function renderPredictionChart(records) {
  if (!state.predictionChart) {
    state.predictionChart = echarts.init(document.getElementById("prediction-chart"));
  }
  const axisLabels = records.map((item) => item.target_datetime.slice(5, 16));
  const tickStep = Math.max(1, Math.floor(records.length / 8));
  const values = records.flatMap((item) => [
    Number(item.target_pollution_anomaly),
    Number(item.predicted_pollution_anomaly),
  ]);
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const span = Math.max(maxValue - minValue, 0.5);
  const padding = span * 0.16;
  const yMin = minValue - padding;
  const yMax = maxValue + padding;
  state.predictionChart.setOption({
    animationDuration: 500,
    backgroundColor: "transparent",
    grid: { left: 54, right: 22, top: 34, bottom: 32 },
    legend: {
      top: 0,
      textStyle: { color: "#5e675f", fontFamily: "Avenir Next, Segoe UI, sans-serif" },
    },
    tooltip: {
      trigger: "axis",
      formatter(params) {
        return formatAxisTooltip(params, {
          Observed: "change",
          Forecast: "change",
        });
      },
    },
    xAxis: {
      type: "category",
      data: axisLabels,
      axisLabel: {
        color: "#5e675f",
        formatter(value, index) {
          if (index % tickStep !== 0 && index !== axisLabels.length - 1) return "";
          return value.slice(0, 5);
        },
      },
      axisLine: { lineStyle: { color: "rgba(31,39,34,0.18)" } },
    },
    yAxis: {
      type: "value",
      min: yMin,
      max: yMax,
      scale: true,
      name: "PM2.5 Change",
      axisLabel: {
        color: "#5e675f",
        formatter(value) {
          return formatTooltipValue(value, "change");
        },
      },
      splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
    },
    series: [
      {
        name: "Observed",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 2, color: "#8f4d33" },
        itemStyle: { color: "#8f4d33" },
        data: records.map((item) => item.target_pollution_anomaly),
      },
      {
        name: "Forecast",
        type: "line",
        smooth: true,
        showSymbol: false,
        lineStyle: { width: 1.9, color: "#2b5b63", type: "dashed" },
        itemStyle: { color: "#2b5b63" },
        data: records.map((item) => item.predicted_pollution_anomaly),
      },
    ],
  });
}

function renderPredictionResidualChart(records) {
  if (!state.predictionResidualChart) {
    state.predictionResidualChart = echarts.init(document.getElementById("prediction-residual-chart"));
  }
  const axisLabels = records.map((item) => item.target_datetime.slice(5, 16));
  const tickStep = Math.max(1, Math.floor(records.length / 8));
  const residuals = records.map(
    (item) => Number(item.predicted_pollution_anomaly) - Number(item.target_pollution_anomaly)
  );
  state.predictionResidualChart.setOption({
    animationDuration: 500,
    backgroundColor: "transparent",
    grid: { left: 68, right: 22, top: 42, bottom: 26 },
    tooltip: {
      trigger: "axis",
      formatter(params) {
        return formatAxisTooltip(params, { Residual: "change" });
      },
    },
    xAxis: {
      type: "category",
      data: axisLabels,
      axisLabel: {
        color: "#5e675f",
        formatter(value, index) {
          if (index % tickStep !== 0 && index !== axisLabels.length - 1) return "";
          return value.slice(0, 5);
        },
      },
      axisLine: { lineStyle: { color: "rgba(31,39,34,0.18)" } },
    },
    yAxis: {
      type: "value",
      name: "Error",
      nameLocation: "end",
      nameGap: 14,
      scale: true,
      nameTextStyle: {
        color: "#5e675f",
        padding: [0, 0, 6, 0],
      },
      axisLabel: {
        color: "#5e675f",
        formatter(value) {
          return formatTooltipValue(value, "change");
        },
      },
      splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
    },
    series: [
      {
        name: "Residual",
        type: "bar",
        barWidth: "58%",
        itemStyle: {
          color(params) {
            return Number(params.value) >= 0 ? "rgba(43,91,99,0.68)" : "rgba(187,106,60,0.68)";
          },
        },
        data: residuals,
      },
    ],
  });
}

async function updatePredictionPanel(record) {
  if (!state.predictionMeta || !state.predictionSummary) {
    renderPredictionPlaceholder(
      "Prediction assets are unavailable.",
      "Run the prediction export step to populate frontend prediction assets."
    );
    return;
  }

  if (state.pollutant !== "pm25") {
    renderPredictionPlaceholder(
      "Prediction module is available for PM2.5 only.",
      "Switch the pollutant filter to PM2.5 to inspect the PyTorch forecasting module."
    );
    return;
  }

  const zoneMeta = lookupPredictionRecord(record.zone_id);
  if (!zoneMeta) {
    renderPredictionPlaceholder(
      `No held-out PM2.5 prediction record for ${record.zone_name}.`,
      "This zone does not appear in the exported PM2.5 test window used for the forecasting module."
    );
    return;
  }

  const payload = await loadPredictionSeries(record.zone_id);
  if (!payload?.records?.length) {
    renderPredictionPlaceholder(
      `Prediction data could not be loaded for ${record.zone_name}.`,
      "The frontend asset for this zone is missing or empty."
    );
    return;
  }

  renderPredictionCards(state.predictionMeta, zoneMeta);
  const recentPredictionWindow = getRecentPredictionWindow(payload.records, 7);
  renderPredictionChart(recentPredictionWindow);
  renderPredictionResidualChart(recentPredictionWindow);
  document.getElementById("prediction-subtitle").textContent =
    `${record.zone_name} · recent 7-day slice from the held-out test window`;
  document.getElementById("prediction-note").textContent =
    "This forecasting module supports short-term prediction. It does not by itself prove causality.";
}

function renderAnalysisCharts() {
  const records = getFilteredRecords();
  const topRecords = [...records]
    .sort((a, b) => Number(b.hotspot_score) - Number(a.hotspot_score))
    .slice(0, 10)
    .reverse();

  const boroughAgg = new Map();
  records.forEach((record) => {
    if (!boroughAgg.has(record.borough)) {
      boroughAgg.set(record.borough, { count: 0, hotspot: 0, corr: 0 });
    }
    const agg = boroughAgg.get(record.borough);
    agg.count += 1;
    agg.hotspot += Number(record.hotspot_score ?? 0);
    agg.corr += Number(record.best_lag_corr ?? 0);
  });

  const boroughRows = [...boroughAgg.entries()]
    .map(([borough, values]) => ({
      borough,
      avgHotspot: values.hotspot / values.count,
      avgCorr: values.corr / values.count,
    }))
    .sort((a, b) => b.avgHotspot - a.avgHotspot);

  if (!state.analysisChart) {
    state.analysisChart = echarts.init(document.getElementById("analysis-chart"));
  }
  if (!state.boroughChart) {
    state.boroughChart = echarts.init(document.getElementById("borough-chart"));
  }

  state.analysisChart.setOption({
    animationDuration: 600,
    backgroundColor: "transparent",
    grid: { left: 108, right: 20, top: 22, bottom: 26 },
    tooltip: { trigger: "axis", axisPointer: { type: "shadow" } },
    xAxis: {
      type: "value",
      axisLabel: { color: "#5e675f" },
      splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
    },
    yAxis: {
      type: "category",
      data: topRecords.map((record) => record.zone_name),
      axisLabel: { color: "#1f2722", width: 96, overflow: "truncate" },
    },
    series: [
      {
        type: "bar",
        data: topRecords.map((record) => Number(record.hotspot_score)),
        itemStyle: { color: "#8f4d33" },
      },
    ],
  });

  state.boroughChart.setOption({
    animationDuration: 600,
    backgroundColor: "transparent",
    grid: { left: 36, right: 24, top: 32, bottom: 40 },
    legend: {
      top: 0,
      textStyle: { color: "#5e675f", fontFamily: "Avenir Next, Segoe UI, sans-serif" },
    },
    tooltip: { trigger: "axis" },
    xAxis: {
      type: "category",
      data: boroughRows.map((row) => row.borough),
      axisLabel: { color: "#5e675f", rotate: 18 },
    },
    yAxis: {
      type: "value",
      min: 0,
      axisLabel: { color: "#5e675f" },
      splitLine: { lineStyle: { color: "rgba(31,39,34,0.08)" } },
    },
    series: [
      {
        name: "Avg Attention",
        type: "bar",
        data: boroughRows.map((row) => Number(row.avgHotspot.toFixed(3))),
        itemStyle: { color: "#bb6a3c" },
      },
      {
        name: "Avg Delay Strength",
        type: "bar",
        data: boroughRows.map((row) => Number(row.avgCorr.toFixed(3))),
        itemStyle: { color: "#2b5b63" },
      },
    ],
  });
}

function renderMap() {
  state.zoneLayer.setStyle(featureStyle);
}

function renderAll() {
  setContextTitles();
  renderLegend();
  renderMap();
  renderKpis();
  renderRanking();
  renderAnalysisCharts();
}

async function init() {
  const [metaResponse, summaryResponse, zonesResponse, predictionMetaResponse, predictionSummaryResponse] = await Promise.all([
    fetch("./public/data/meta.json"),
    fetch("./public/data/summary.json"),
    fetch("./public/data/zones.geojson"),
    fetch("./public/data/predictions/pm25/meta.json").catch(() => null),
    fetch("./public/data/predictions/pm25/summary.json").catch(() => null),
  ]);

  state.meta = await metaResponse.json();
  state.summary = await summaryResponse.json();
  state.zones = await zonesResponse.json();
  if (predictionMetaResponse?.ok && predictionSummaryResponse?.ok) {
    state.predictionMeta = await predictionMetaResponse.json();
    state.predictionSummary = await predictionSummaryResponse.json();
  }

  populateControls();
  setDatasetMeta();
  buildMap();
  renderAll();
  await updateDetailPanel();

  window.addEventListener("resize", () => {
    state.trendChart?.resize();
    state.timeChart?.resize();
    state.analysisChart?.resize();
    state.boroughChart?.resize();
    state.predictionChart?.resize();
    state.predictionResidualChart?.resize();
  });
}

init().catch((error) => {
  console.error(error);
  document.getElementById("detail-title").textContent = "Failed to load dashboard assets";
});
