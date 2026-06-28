import React, { useState, useEffect, useMemo, useRef } from 'react';
import ReactECharts from 'echarts-for-react';
import * as echarts from 'echarts';
import { Loader2, LayoutGrid, Info, CheckSquare, Square, ChevronDown, Check, X } from 'lucide-react';
import LoadingScreen from '@/components/LoadingScreen';

// All KPIs grouped for the select dropdown
const KPI_GROUPS = [
    {
        label: 'Demographics',
        kpis: [
            { id: 'total_patients', name: 'Total Population', sentiment: 'higher-is-better', format: (v) => v.toLocaleString(), unit: 'patients' },
            { id: 'active_patient_rate', name: 'Active Patient Rate', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'gender_balance_ratio', name: 'Gender Balance (M/F)', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'mean_family_income', name: 'Mean Family Income', sentiment: 'higher-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'median_family_income', name: 'Median Family Income', sentiment: 'higher-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'avg_patient_age', name: 'Average Patient Age', sentiment: 'lower-is-better', format: (v) => `${v} yrs`, unit: 'years' },
            { id: 'married_rate', name: 'Marriage Rate', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'higher_education_rate', name: 'Higher Education Rate', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' }
        ]
    },
    {
        label: 'Clinical & Pathology',
        kpis: [
            { id: 'active_condition_burden', name: 'Active Condition Burden', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'cases' },
            { id: 'global_recovery_rate', name: 'Global Recovery Rate', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'patient_complexity_score', name: 'Patient Complexity Score', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'concepts' },
            { id: 'avg_cure_time', name: 'Average Cure Time', sentiment: 'lower-is-better', format: (v) => `${v} days`, unit: 'days' },
            { id: 'admission_rate_last_30_days', name: 'Admissions (30d)', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'admissions' },
            { id: 'total_diagnoses', name: 'Total Diagnoses', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'diagnoses' },
            { id: 'unique_conditions', name: 'Unique Conditions', sentiment: 'higher-is-better', format: (v) => v.toLocaleString(), unit: 'conditions' },
            { id: 'chronic_condition_burden', name: 'Chronic Burden', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'cases' }
        ]
    },
    {
        label: 'Allergies',
        kpis: [
            { id: 'total_allergic_population', name: 'Allergic Population', sentiment: 'lower-is-better', format: (v) => v.toLocaleString(), unit: 'patients' },
            { id: 'active_allergy_rate', name: 'Active Allergy Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'severe_incident_rate', name: 'Severe Incident Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'allergic_patient_rate', name: 'Allergic Patient Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'penicillin_allergy_delabeling_eligibility_rate', name: 'Penicillin De-labeling Eligibility', sentiment: 'higher-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'allergy_related_readmission_rate', name: 'Allergy Related Readmission Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'poly_allergen_patient_rate', name: 'Poly-Allergen Patient Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' },
            { id: 'drug_hypersensitivity_rate', name: 'Drug Hypersensitivity Rate', sentiment: 'lower-is-better', format: (v) => `${v}%`, unit: '%' }
        ]
    },
    {
        label: 'Financial & Operations',
        kpis: [
            { id: 'total_revenue', name: 'Total Revenue', sentiment: 'higher-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'average_encounter_duration_hours', name: 'Average Duration', sentiment: 'lower-is-better', format: (v) => `${v} hrs`, unit: 'hours' },
            { id: 'avg_out_of_pocket', name: 'Average Out-of-Pocket Cost', sentiment: 'lower-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'average_base_fee', name: 'Average Base Fee', sentiment: 'lower-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'insurer_covered', name: 'Insurer Covered', sentiment: 'higher-is-better', format: (v) => `$${v.toLocaleString()}`, unit: 'USD' },
            { id: 'unique_patients_seen', name: 'Unique Patients Seen', sentiment: 'higher-is-better', format: (v) => v.toLocaleString(), unit: 'patients' },
            { id: 'average_practitioner_load', name: 'Average Practitioner Load', sentiment: 'lower-is-better', format: (v) => `${v} load`, unit: 'encounters/prac' },
            { id: 'encounters_30d', name: 'Encounters (30d)', sentiment: 'higher-is-better', format: (v) => v.toLocaleString(), unit: 'visits' }
        ]
    }
];

const ALL_KPIS = KPI_GROUPS.flatMap(g => g.kpis);

const COLOR_PALETTES = {
    'higher-is-better': ['#e0f2f1', '#b2dfdb', '#4db6ac', '#009688', '#00796b', '#004d40', '#0a2f1d'],
    'lower-is-better': ['#fff3e0', '#ffe0b2', '#ffb74d', '#f57c00', '#e65100', '#bf360c', '#5d1b06']
};

const GeographicDashboard = () => {
    const [mapJson, setMapJson] = useState(null);
    const [activeKpiId, setActiveKpiId] = useState('total_patients');
    const [selectedStates, setSelectedStates] = useState([]); 
    const [hasInitializedStates, setHasInitializedStates] = useState(false);
    const [dropdownOpen, setDropdownOpen] = useState(false);
    const dropdownRef = useRef(null);
    
    const [cache, setCache] = useState({});
    const [loading, setLoading] = useState(true);
    const [kpiLoading, setKpiLoading] = useState(false);

    // Close dropdown on outside click
    useEffect(() => {
        const handleClickOutside = (event) => {
            if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
                setDropdownOpen(false);
            }
        };
        document.addEventListener('mousedown', handleClickOutside);
        return () => document.removeEventListener('mousedown', handleClickOutside);
    }, []);

    // 1. Fetch USA Map GeoJSON once on load
    useEffect(() => {
        const loadMap = async () => {
            try {
                const res = await fetch('/USA.json');
                const geoJson = await res.json();
                echarts.registerMap('USA', geoJson);
                setMapJson(geoJson);
            } catch (err) {
                console.error("Failed to load USA map GeoJSON", err);
            }
        };
        loadMap();
    }, []);

    // 2. Fetch KPI Data granularly and lazily
    useEffect(() => {
        if (!mapJson || !activeKpiId) return;

        const fetchKpiData = async () => {
            if (cache[activeKpiId]) {
                setLoading(false);
                return;
            }

            setKpiLoading(true);
            try {
                const res = await fetch(`/api/geographic_dashboard?kpi=${activeKpiId}`);
                const result = await res.json();
                
                if (result?.data) {
                    setCache(prev => ({
                        ...prev,
                        [activeKpiId]: result.data
                    }));
                }
            } catch (err) {
                console.error(`Error loading state KPI for ${activeKpiId}`, err);
            } finally {
                setKpiLoading(false);
                setLoading(false);
            }
        };

        fetchKpiData();
    }, [activeKpiId, mapJson, cache]);

    const activeKpiConfig = useMemo(() => {
        return ALL_KPIS.find(k => k.id === activeKpiId) || ALL_KPIS[0];
    }, [activeKpiId]);

    const stateData = useMemo(() => {
        return cache[activeKpiId] || {};
    }, [cache, activeKpiId]);

    // Data for ECharts Map Series
    const chartData = useMemo(() => {
        if (!stateData) return [];
        return Object.entries(stateData).map(([stateName, metrics]) => ({
            name: stateName,
            value: metrics.value || 0
        }));
    }, [stateData]);

    const allAvailableStates = useMemo(() => {
        return Object.keys(stateData).sort();
    }, [stateData]);

    // Initial check all states on first load
    useEffect(() => {
        if (allAvailableStates.length > 0 && !hasInitializedStates) {
            setSelectedStates(allAvailableStates);
            setHasInitializedStates(true);
        }
    }, [allAvailableStates, hasInitializedStates]);

    const valueRange = useMemo(() => {
        if (chartData.length === 0) return { min: 0, max: 100 };
        const vals = chartData.map(d => d.value);
        return {
            min: Math.min(...vals),
            max: Math.max(...vals)
        };
    }, [chartData]);

    // States filtered by checkbox selection, sorted by value depending on KPI sentiment
    const sortedStates = useMemo(() => {
        if (!stateData || !activeKpiConfig) return [];
        
        let entries = Object.entries(stateData).map(([name, metrics]) => ({
            name,
            value: metrics.value || 0,
            trend: metrics.trend || 0
        }));

        if (selectedStates.length > 0) {
            entries = entries.filter(e => selectedStates.includes(e.name));
        } else {
            // If nothing is selected, show empty chart rather than everything to match comparison context
            return [];
        }

        return entries.sort((a, b) => {
            if (activeKpiConfig.sentiment === 'higher-is-better') {
                return b.value - a.value;
            } else {
                return a.value - b.value;
            }
        });
    }, [stateData, activeKpiConfig, selectedStates]);

    const handleStateToggle = (stateName) => {
        setSelectedStates(prev => {
            if (prev.includes(stateName)) {
                return prev.filter(name => name !== stateName);
            }
            if (prev.length >= 20) {
                alert("You can compare a maximum of 20 states at a time.");
                return prev;
            }
            return [...prev, stateName];
        });
    };

    const handleSelectAll = () => {
        setSelectedStates(allAvailableStates.slice(0, 10)); // Limit to first 10 for compare safeguard if needed, or all 50
    };
    
    const handleSelectAll50 = () => {
        setSelectedStates(allAvailableStates);
    };

    const handleClearAll = () => {
        setSelectedStates([]);
    };

    // Map Option
    const getMapOption = () => {
        if (!mapJson) return {};
        const palette = COLOR_PALETTES[activeKpiConfig.sentiment];

        return {
            tooltip: {
                trigger: 'item',
                formatter: function (params) {
                    const val = params.value;
                    const name = params.name;
                    const record = stateData[name];
                    if (!record) return `${name}: No data`;
                    
                    return `
                        <div class="p-2 font-sans min-w-[150px]">
                            <div class="font-extrabold text-slate-800 border-b border-slate-100 pb-1 mb-1.5 text-xs">${name}</div>
                            <div class="flex justify-between items-center text-[11px] py-0.5">
                                <span class="text-slate-400 font-bold">${activeKpiConfig.name}:</span>
                                <span class="font-black text-slate-700 ml-1">${activeKpiConfig.format(val)}</span>
                            </div>
                        </div>
                    `;
                },
                backgroundColor: 'rgba(255, 255, 255, 0.98)',
                borderRadius: 8,
                borderWidth: 0,
                shadowColor: 'rgba(0, 0, 0, 0.05)',
                shadowBlur: 10
            },
            visualMap: {
                right: '5%',
                bottom: '5%',
                min: valueRange.min,
                max: valueRange.max,
                inRange: {
                    color: palette
                },
                text: ['High', 'Low'],
                calculable: true,
                textStyle: {
                    color: '#64748b',
                    fontSize: 10,
                    fontWeight: 'bold'
                }
            },
            series: [
                {
                    name: activeKpiConfig.name,
                    type: 'map',
                    map: 'USA',
                    roam: true, 
                    zoom: 1.15,
                    label: {
                        show: false
                    },
                    emphasis: {
                        label: {
                            show: true,
                            color: '#0f172a',
                            fontWeight: 'bold',
                            fontSize: 10
                        },
                        itemStyle: {
                            areaColor: '#e2e8f0'
                        }
                    },
                    data: chartData
                }
            ]
        };
    };

    // Bar + Trendline Option (Combined Chart)
    const getBarOption = () => {
        if (sortedStates.length === 0) return {};

        const displayZoom = selectedStates.length > 15;

        return {
            tooltip: {
                trigger: 'axis',
                axisPointer: { type: 'shadow' },
                formatter: function (params) {
                    const firstParam = params.find(p => p.seriesName === activeKpiConfig.name);
                    if (!firstParam) return '';
                    return `
                        <div class="p-2 font-sans">
                            <div class="font-extrabold text-slate-800 border-b border-slate-100 pb-1 mb-1.5 text-xs">${firstParam.name}</div>
                            <div class="flex justify-between items-center text-[11px] py-0.5">
                                <span class="text-slate-400 font-bold">${firstParam.seriesName}:</span>
                                <span class="font-black text-slate-700 ml-2">${activeKpiConfig.format(firstParam.value)}</span>
                            </div>
                        </div>
                    `;
                },
                backgroundColor: 'rgba(255, 255, 255, 0.98)',
                borderRadius: 8,
                borderWidth: 0,
                shadowColor: 'rgba(0, 0, 0, 0.05)',
                shadowBlur: 10
            },
            legend: {
                data: [activeKpiConfig.name],
                top: '2%',
                right: '5%',
                textStyle: { color: '#475569', fontSize: 10, fontWeight: 'bold' }
            },
            grid: { left: '5%', right: '5%', top: '15%', bottom: displayZoom ? 120 : 80, containLabel: false },
            dataZoom: !displayZoom ? [] : [{ type: 'inside', start: 0, end: 35 }, { type: 'slider', start: 0, end: 35, bottom: '2%', height: 20 }],
            xAxis: {
                type: 'category',
                data: sortedStates.map(s => s.name),
                axisTick: { show: false },
                axisLine: { show: false },
                axisLabel: { interval: 0, rotate: 45, color: '#475569', fontSize: 9, fontWeight: 'bold' }
            },
            yAxis: {
                type: 'value',
                name: activeKpiConfig.unit,
                axisLine: { show: false },
                axisTick: { show: false },
                splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } },
                axisLabel: { color: '#64748b', fontSize: 10 }
            },
            series: [
                {
                    name: activeKpiConfig.name,
                    type: 'bar',
                    barWidth: sortedStates.length > 15 ? '50%' : '25%',
                    itemStyle: {
                        color: activeKpiConfig.sentiment === 'higher-is-better' ? '#009688' : '#f57c00',
                        borderRadius: [4, 4, 0, 0]
                    },
                    data: sortedStates.map(s => s.value)
                },
                {
                    name: 'Trend',
                    type: 'line',
                    smooth: true,
                    symbolSize: 6,
                    lineStyle: { width: 2.5, color: '#6366f1' },
                    itemStyle: { color: '#6366f1' },
                    data: sortedStates.map(s => s.value)
                }
            ]
        };
    };

    if (loading) {
        return <LoadingScreen message="Initializing Map Analysis..." subtext="Accessing patient geospatial records." />;
    }

    const dropdownLabel = selectedStates.length === allAvailableStates.length
        ? 'All States Selected'
        : `${selectedStates.length} States Selected`;

    return (
        <div className="animate-fade-in w-full h-full flex flex-col bg-slate-50">
            {/* Header */}
            <header className="bg-white border-b border-slate-200 py-4 px-6 md:px-8 sticky top-0 z-20 w-full">
                <div className="max-w-[1600px] mx-auto w-full flex flex-col sm:flex-row sm:items-center justify-between gap-4">
                    <div>
                        <h1 className="text-xl font-bold text-slate-800 tracking-tight flex items-center gap-2">
                            <LayoutGrid className="text-teal-600" size={20} />
                            Geographic Performance Analysis
                        </h1>
                        <p className="text-slate-400 text-xs font-semibold mt-0.5">Nationwide hospital KPI and growth rate distribution.</p>
                    </div>

                    {/* Controls Row */}
                    <div className="flex items-center gap-4 flex-wrap">
                        {/* 1. Drodown Checkbox for State Selection */}
                        <div className="relative flex items-center gap-2" ref={dropdownRef}>
                            <span className="text-slate-400 font-bold text-xs">Filter Region:</span>
                            <button
                                onClick={() => setDropdownOpen(!dropdownOpen)}
                                className="bg-white border border-slate-200 text-slate-700 text-xs font-bold rounded-xl py-2 px-4 flex items-center gap-2 shadow-sm hover:bg-slate-50 transition-colors outline-none cursor-pointer"
                            >
                                <span>{dropdownLabel}</span>
                                <ChevronDown size={14} className={`text-slate-400 transition-transform ${dropdownOpen ? 'rotate-180' : ''}`} />
                            </button>

                            {dropdownOpen && (
                                <div className="absolute right-0 top-full mt-2 w-64 bg-white border border-slate-200 rounded-xl shadow-xl z-30 p-3 space-y-2 max-h-[300px] flex flex-col">
                                    <div className="flex justify-between items-center border-b border-slate-100 pb-2">
                                        <span className="text-[10px] font-black text-slate-400 uppercase">States (Max 10 for compare)</span>
                                        <button 
                                            onClick={handleClearAll} 
                                            className="text-[9px] font-black text-rose-500 hover:underline cursor-pointer"
                                        >
                                            Clear All
                                        </button>
                                    </div>
                                    <div className="flex gap-2">
                                        <button 
                                            onClick={handleSelectAll} 
                                            className="text-[9px] font-black text-teal-600 bg-teal-50 border border-teal-100 px-2 py-1 rounded hover:bg-teal-100 transition-colors cursor-pointer text-center flex-1"
                                        >
                                            Select 10
                                        </button>
                                        <button 
                                            onClick={handleSelectAll50} 
                                            className="text-[9px] font-black text-indigo-600 bg-indigo-50 border border-indigo-100 px-2 py-1 rounded hover:bg-indigo-100 transition-colors cursor-pointer text-center flex-1"
                                        >
                                            Select All (50)
                                        </button>
                                    </div>

                                    {/* Checklist Scroll Portion */}
                                    <div className="flex-1 overflow-y-auto space-y-1.5 pr-1 custom-scrollbar">
                                        {allAvailableStates.map(stateName => {
                                            const isChecked = selectedStates.includes(stateName);
                                            return (
                                                <div
                                                    key={stateName}
                                                    onClick={() => handleStateToggle(stateName)}
                                                    className="flex items-center justify-between px-2 py-1.5 rounded-lg hover:bg-slate-50 cursor-pointer text-left text-xs font-bold text-slate-600"
                                                >
                                                    <span className="truncate">{stateName}</span>
                                                    {isChecked ? (
                                                        <Check size={14} className="text-teal-600 font-extrabold" />
                                                    ) : (
                                                        <div className="w-3.5 h-3.5 border border-slate-300 rounded" />
                                                    )}
                                                </div>
                                            );
                                        })}
                                    </div>
                                </div>
                            )}
                        </div>

                        {/* 2. Single Dropdown for KPI Selection */}
                        <div className="flex items-center gap-2">
                            <label htmlFor="kpi-select" className="text-slate-400 font-bold text-xs whitespace-nowrap">
                                Metric:
                            </label>
                            <select
                                id="kpi-select"
                                value={activeKpiId}
                                onChange={(e) => {
                                    setActiveKpiId(e.target.value);
                                }}
                                className="bg-slate-50 border border-slate-200 text-slate-700 text-xs font-bold rounded-xl py-2 px-4 focus:ring-2 focus:ring-teal-500 focus:border-teal-500 cursor-pointer outline-none transition-all shadow-sm"
                            >
                                {KPI_GROUPS.map((group) => (
                                    <optgroup key={group.label} label={group.label}>
                                        {group.kpis.map((kpi) => (
                                            <option key={kpi.id} value={kpi.id}>
                                                {kpi.name}
                                            </option>
                                        ))}
                                    </optgroup>
                                ))}
                            </select>
                        </div>
                    </div>
                </div>
            </header>

            {/* Main Content Area */}
            <div className="max-w-[1600px] mx-auto w-full px-6 md:px-8 py-6 space-y-6 flex-1">
                
                {/* 1. Large USA Map - full width */}
                <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 flex flex-col h-[580px] relative">
                    <div className="flex items-center gap-1.5 text-slate-400 text-[10px] font-bold uppercase tracking-wider mb-2 border-b border-slate-100 pb-2">
                        <Info size={12} />
                        Interactive Heatmap View (Use scroll wheel to zoom, drag to pan map)
                    </div>

                    {kpiLoading ? (
                        <div className="flex-1 flex flex-col justify-center items-center gap-2">
                            <Loader2 className="animate-spin text-teal-600" size={32} />
                            <span className="text-slate-400 font-bold text-xs">Loading metric data...</span>
                        </div>
                    ) : mapJson ? (
                        <div className="flex-1 w-full relative">
                            <ReactECharts
                                option={getMapOption()}
                                style={{ height: '100%', width: '100%' }}
                            />
                        </div>
                    ) : (
                        <div className="flex-1 flex justify-center items-center">
                            <span className="text-red-500 font-bold text-xs">Failed to load USA map asset.</span>
                        </div>
                    )}
                </div>

                {/* 2. Large Bar + Trendline Chart - full width below the map */}
                <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6 flex flex-col h-[520px]">
                    <div className="text-slate-400 text-[10px] font-bold uppercase tracking-wider mb-2 border-b border-slate-100 pb-2">
                        State-by-State Metric Breakdown
                    </div>

                    {kpiLoading ? (
                        <div className="flex-1 flex justify-center items-center">
                            <Loader2 className="animate-spin text-teal-600" size={24} />
                        </div>
                    ) : sortedStates.length === 0 ? (
                        <div className="flex-1 flex flex-col justify-center items-center gap-2">
                            <span className="text-slate-400 font-bold text-xs">No states selected.</span>
                            <button 
                                onClick={handleSelectAll50} 
                                className="text-[10px] font-black text-teal-600 bg-teal-50 border border-teal-100 px-3 py-1.5 rounded-lg hover:bg-teal-100 transition-colors cursor-pointer"
                            >
                                Select All States
                            </button>
                        </div>
                    ) : (
                        <div className="flex-1 w-full h-[440px]">
                            <ReactECharts
                                option={getBarOption()}
                                style={{ height: '100%', width: '100%' }}
                            />
                        </div>
                    )}
                </div>

            </div>
        </div>
    );
};

export default GeographicDashboard;
