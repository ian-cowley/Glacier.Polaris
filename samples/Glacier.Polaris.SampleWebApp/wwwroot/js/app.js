/* ==========================================================================
   Glacier.Polaris Studio - Frontend Application Logic
   ========================================================================== */

document.addEventListener('DOMContentLoaded', () => {
    // Chart instances
    let charts = {
        distribution: null,
        aggPie: null,
        aggBar: null,
        temporal: null,
        wrangling: null
    };

    // Global Dataset Metadata
    let datasetMeta = {};

    // Base API URL
    const API_BASE = '/api';

    // Theme state
    let isDark = document.documentElement.getAttribute('data-bs-theme') === 'dark';

    // Initialize application
    init();

    function init() {
        initThemeToggle();
        initControls();
        loadOverview();
        
        // Tab switching listeners to render/resize charts properly
        const tabElms = document.querySelectorAll('button[data-bs-toggle="tab"]');
        tabElms.forEach(tab => {
            tab.addEventListener('shown.bs.tab', event => {
                const targetId = event.target.getAttribute('data-bs-target').substring(1);
                if (targetId === 'distribution' && !charts.distribution) loadDistribution();
                if (targetId === 'aggregations' && (!charts.aggPie || !charts.aggBar)) loadAggregations();
                if (targetId === 'temporal' && !charts.temporal) loadTemporal();
                if (targetId === 'wrangling' && !charts.wrangling) loadWrangling();
            });
        });
    }

    /* ======================================================================
       Theme Management
       ====================================================================== */
    function initThemeToggle() {
        const toggleBtn = document.getElementById('themeToggle');
        toggleBtn.addEventListener('click', () => {
            isDark = !isDark;
            document.documentElement.setAttribute('data-bs-theme', isDark ? 'dark' : 'light');
            
            // Re-render charts with new theme colors
            Object.keys(charts).forEach(key => {
                if (charts[key]) {
                    charts[key].options.scales = getChartScales();
                    charts[key].options.plugins.legend.labels.color = isDark ? '#ffffff' : '#0f172a';
                    charts[key].update();
                }
            });
        });
    }

    function getChartColor(type, opacity = 1) {
        const colors = {
            primary: `rgba(59, 130, 246, ${opacity})`,
            purple: `rgba(139, 92, 246, ${opacity})`,
            success: `rgba(16, 185, 129, ${opacity})`,
            warning: `rgba(245, 158, 11, ${opacity})`,
            info: `rgba(6, 182, 212, ${opacity})`,
            danger: `rgba(239, 68, 68, ${opacity})`,
            border: isDark ? 'rgba(255, 255, 255, 0.1)' : 'rgba(0, 0, 0, 0.1)',
            text: isDark ? '#94a3b8' : '#475569'
        };
        return colors[type] || colors.primary;
    }

    function getChartScales(hideX = false) {
        const gridColor = getChartColor('border');
        const textColor = getChartColor('text');
        
        let scales = {
            y: {
                grid: { color: gridColor },
                ticks: { color: textColor, font: { family: 'Inter' } }
            }
        };

        if (!hideX) {
            scales.x = {
                grid: { color: gridColor },
                ticks: { color: textColor, font: { family: 'Inter' } }
            };
        } else {
            scales.x = { display: false };
        }

        return scales;
    }

    /* ======================================================================
       UI Control Binding
       ====================================================================== */
    function initControls() {
        // Distribution controls
        const binsRange = document.getElementById('binsRange');
        const binsLabel = document.getElementById('binsLabel');
        binsRange.addEventListener('input', e => binsLabel.textContent = e.target.value);

        const bwRange = document.getElementById('bwRange');
        const bwLabel = document.getElementById('bwLabel');
        bwRange.addEventListener('input', e => bwLabel.textContent = e.target.value);

        document.getElementById('updateDistBtn').addEventListener('click', loadDistribution);

        // Aggregation controls
        document.getElementById('aggGroupBySelect').addEventListener('change', loadAggregations);

        // Temporal controls
        const windowRange = document.getElementById('windowRange');
        const windowLabel = document.getElementById('windowLabel');
        windowRange.addEventListener('input', e => windowLabel.textContent = `${e.target.value} Days`);

        document.getElementById('updateTemporalBtn').addEventListener('click', loadTemporal);

        // Wrangling controls
        document.getElementById('updateWrangleBtn').addEventListener('click', loadWrangling);
    }

    function formatNumber(num) {
        return new Intl.NumberFormat().format(num);
    }

    function formatBytes(bytes) {
        if (bytes === 0) return '0 Bytes';
        const k = 1024;
        const sizes = ['Bytes', 'KB', 'MB', 'GB'];
        const i = Math.floor(Math.log(bytes) / Math.log(k));
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
    }

    /* ======================================================================
       API Data Fetching & Rendering
       ====================================================================== */

    // 1. Overview & Schema
    async function loadOverview() {
        try {
            const response = await fetch(`${API_BASE}/dataset/overview`);
            if (!response.ok) throw new Error('Failed to fetch dataset overview');
            const data = await response.json();

            // Populate KPIs
            document.getElementById('kpiRowCount').textContent = formatNumber(data.rowCount);
            document.getElementById('kpiColCount').textContent = formatNumber(data.colCount);
            document.getElementById('kpiMemory').textContent = formatBytes(data.estimatedMemory);

            // Populate Schema Table
            const schemaBody = document.querySelector('#schemaTable tbody');
            schemaBody.innerHTML = '';
            Object.entries(data.schema).forEach(([col, type]) => {
                const tr = document.createElement('tr');
                tr.innerHTML = `
                    <td class="fw-semibold">${col}</td>
                    <td><span class="badge bg-secondary-subtle text-secondary border border-secondary-subtle">${type}</span></td>
                `;
                schemaBody.appendChild(tr);
            });

            // Populate Sample Table
            const sampleHead = document.getElementById('sampleTableHeader');
            const sampleBody = document.getElementById('sampleTableBody');
            sampleHead.innerHTML = '';
            sampleBody.innerHTML = '';

            const cols = Object.keys(data.sample);
            cols.forEach(c => {
                const th = document.createElement('th');
                th.textContent = c;
                sampleHead.appendChild(th);
            });

            const rowCount = data.sample[cols[0]].length;
            for (let i = 0; i < rowCount; i++) {
                const tr = document.createElement('tr');
                cols.forEach(c => {
                    const td = document.createElement('td');
                    td.textContent = data.sample[c][i] !== null ? data.sample[c][i] : 'NULL';
                    tr.appendChild(td);
                });
                sampleBody.appendChild(tr);
            }

        } catch (error) {
            console.error(error);
        }
    }

    // 2. Distribution (Hist & KDE)
    async function loadDistribution() {
        try {
            const column = document.getElementById('distColumnSelect').value;
            const bins = document.getElementById('binsRange').value;
            const bandwidth = document.getElementById('bwRange').value;

            const response = await fetch(`${API_BASE}/analytics/distribution?column=${column}&bins=${bins}&bandwidth=${bandwidth}`);
            if (!response.ok) throw new Error('Failed to fetch distribution analytics');
            const data = await response.json();

            const hist = data.histogram;
            const kde = data.kde;

            // Prepare chart data
            const labels = hist.bin_start.map(val => Number(val).toFixed(1));
            
            if (charts.distribution) charts.distribution.destroy();

            const ctx = document.getElementById('distributionChart').getContext('2d');
            charts.distribution = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [
                        {
                            label: 'Histogram (Frequency)',
                            data: hist.count,
                            backgroundColor: getChartColor('primary', 0.6),
                            borderColor: getChartColor('primary', 1),
                            borderWidth: 2,
                            borderRadius: 8,
                            order: 2
                        },
                        {
                            label: 'KDE (Density Curve)',
                            data: kde.density.map(d => d * hist.count.reduce((a, b) => a + b, 0) * (hist.bin_start[1] - hist.bin_start[0])), // Scaled to histogram
                            type: 'line',
                            borderColor: getChartColor('warning', 1),
                            backgroundColor: getChartColor('warning', 0.1),
                            borderWidth: 3,
                            fill: true,
                            tension: 0.4,
                            pointRadius: 0,
                            order: 1
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: getChartScales(),
                    plugins: {
                        legend: { labels: { color: isDark ? '#ffffff' : '#0f172a', font: { family: 'Inter' } } }
                    }
                }
            });

        } catch (error) {
            console.error(error);
        }
    }

    // 3. GroupBy Aggregations
    async function loadAggregations() {
        try {
            const groupBy = document.getElementById('aggGroupBySelect').value;
            const response = await fetch(`${API_BASE}/analytics/aggregations?groupBy=${groupBy}`);
            if (!response.ok) throw new Error('Failed to fetch aggregations');
            const data = await response.json();

            const agg = data.aggregations;
            const groups = agg[groupBy];

            // Render Pie Chart (Total Revenue Share)
            if (charts.aggPie) charts.aggPie.destroy();
            const pieCtx = document.getElementById('aggPieChart').getContext('2d');
            charts.aggPie = new Chart(pieCtx, {
                type: 'doughnut',
                data: {
                    labels: groups,
                    datasets: [{
                        data: agg.revenue_sum,
                        backgroundColor: [
                            getChartColor('primary', 0.8),
                            getChartColor('purple', 0.8),
                            getChartColor('success', 0.8),
                            getChartColor('warning', 0.8),
                            getChartColor('info', 0.8)
                        ],
                        borderWidth: 2,
                        borderColor: isDark ? '#0a0c14' : '#ffffff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { position: 'bottom', labels: { color: isDark ? '#ffffff' : '#0f172a', font: { family: 'Inter' } } }
                    }
                }
            });

            // Render Bar Chart (Comparative KPIs)
            if (charts.aggBar) charts.aggBar.destroy();
            const barCtx = document.getElementById('aggBarChart').getContext('2d');
            charts.aggBar = new Chart(barCtx, {
                type: 'bar',
                data: {
                    labels: groups,
                    datasets: [
                        {
                            label: 'Avg Revenue ($)',
                            data: agg.revenue_mean,
                            backgroundColor: getChartColor('primary', 0.7),
                            borderRadius: 8
                        },
                        {
                            label: 'Avg Units Sold',
                            data: agg.units_sold_mean,
                            backgroundColor: getChartColor('success', 0.7),
                            borderRadius: 8
                        },
                        {
                            label: 'Customer Satisfaction (/5)',
                            data: agg.satisfaction_mean.map(s => s * 10), // Multiplied by 10 for visibility on scale
                            backgroundColor: getChartColor('warning', 0.7),
                            borderRadius: 8
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: getChartScales(),
                    plugins: {
                        legend: { labels: { color: isDark ? '#ffffff' : '#0f172a', font: { family: 'Inter' } } }
                    }
                }
            });

        } catch (error) {
            console.error(error);
        }
    }

    // 4. Temporal & Rolling
    async function loadTemporal() {
        try {
            const window = document.getElementById('windowRange').value;
            const response = await fetch(`${API_BASE}/analytics/temporal?window=${window}`);
            if (!response.ok) throw new Error('Failed to fetch temporal analytics');
            const data = await response.json();

            const temp = data.temporal;

            if (charts.temporal) charts.temporal.destroy();
            const ctx = document.getElementById('temporalChart').getContext('2d');
            charts.temporal = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: temp.day.map(d => `Day ${d}`),
                    datasets: [
                        {
                            label: 'Daily Revenue ($)',
                            data: temp.revenue_sum,
                            borderColor: getChartColor('primary', 0.4),
                            backgroundColor: getChartColor('primary', 0.05),
                            fill: true,
                            tension: 0.2,
                            pointRadius: 2
                        },
                        {
                            label: `${window}-Day Rolling Mean`,
                            data: temp.rolling_mean,
                            borderColor: getChartColor('danger', 1),
                            borderWidth: 3,
                            tension: 0.4,
                            pointRadius: 0
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: getChartScales(),
                    plugins: {
                        legend: { labels: { color: isDark ? '#ffffff' : '#0f172a', font: { family: 'Inter' } } }
                    }
                }
            });

        } catch (error) {
            console.error(error);
        }
    }

    // 5. Wrangling & Filtering
    async function loadWrangling() {
        try {
            const cat = document.getElementById('wrangleCatSelect').value;
            const response = await fetch(`${API_BASE}/analytics/wrangling?categoryFilter=${cat}`);
            if (!response.ok) throw new Error('Failed to fetch wrangling analytics');
            const data = await response.json();

            document.getElementById('filteredCountBadge').textContent = `Filtered Subset: ${formatNumber(data.filteredCount)} Rows`;

            const regCounts = data.regionCounts;

            if (charts.wrangling) charts.wrangling.destroy();
            const ctx = document.getElementById('wranglingChart').getContext('2d');
            charts.wrangling = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: regCounts.region,
                    datasets: [{
                        label: 'Record Count by Region',
                        data: regCounts.count,
                        backgroundColor: [
                            getChartColor('primary', 0.7),
                            getChartColor('purple', 0.7),
                            getChartColor('success', 0.7),
                            getChartColor('warning', 0.7)
                        ],
                        borderRadius: 12
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: getChartScales(),
                    plugins: {
                        legend: { display: false }
                    }
                }
            });

        } catch (error) {
            console.error(error);
        }
    }
});
