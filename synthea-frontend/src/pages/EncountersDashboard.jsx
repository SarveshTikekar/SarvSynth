
import React, { useState, useEffect, useMemo } from "react";
import { encountersDashboard } from "@/api/api";
import {
    Activity, CheckCircle, GitMerge, Clock as ClockIcon, Hospital,
    Stethoscope, BarChart3, TrendingUp, Search, X, Info, Filter, Users, Database, AlertTriangle, DollarSign, BriefcaseMedical, UserCheck, ShieldCheck
} from "lucide-react";
import KPICard from "@/components/KPICard";
import MetricsCard from "@/components/MetricsCard";
import AdvancedChartCard from "@/components/AdvancedChartCard";
import LoadingScreen from "@/components/LoadingScreen";
import {
    BarChart, Bar, PieChart, Pie, Cell,
    XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
    LineChart, Line, ScatterChart, Scatter, ZAxis, AreaChart, Area, ComposedChart
} from "recharts";
import ReactECharts from 'echarts-for-react';

// --- Colors & Gradients ---
const PIE_COLORS = ["#14b8a6", "#f43f5e", "#8b5cf6", "#f59e0b", "#3b82f6", "#64748b"];

const EncountersDashboard = () => {
    const [data, setData] = useState({ kpis: {}, metrics: {}, advanced_metrics: {} });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

    useEffect(() => {
        const fetchData = async () => {
            try {
                const result = await encountersDashboard();
                if (result?.encounters_dashboard) {
                    setData({
                        kpis: result.encounters_dashboard.kpis || {},
                        metrics: result.encounters_dashboard.metrics || {},
                        advanced_metrics: result.encounters_dashboard.advanced_metrics || {}
                    });
                } else {
                    setError("Failed to load encounters data.");
                }
            } catch (err) {
                console.error(err);
                setError("Error fetching dashboard data.");
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, []);

    const truncateLabel = (str, max = 15) => {
        if (!str) return "";
        return str.length > max ? `${str.substring(0, max)}...` : str;
    };

    // --- Memoized Data (Basic Metrics - 6 Total) ---
    // Metrics 1, 2, 6 are already [{name, value}] from the new backend output
    const topCauses = useMemo(() => data.metrics.top_10_causes || [], [data]);
    const encountersByType = useMemo(() => data.metrics.encounters_by_type || [], [data]);
    const topPractitioners = useMemo(() => data.metrics.top_10_practitioners || [], [data]);

    const mostExpensiveCauses = useMemo(() => {
        const result = data.metrics.most_expensive_causes || [];
        return result.map(item => ({ ...item, name: truncateLabel(item.name, 25) }))
    }, [data]);

    const coverageOOP = useMemo(() => data.metrics.coverage_vs_oop_by_type || [], [data]);
    const feeDivergence = useMemo(() => data.metrics.fee_divergence_by_type || [], [data]);

    // --- Memoized Data (Advanced Metrics - 4 Total) ---
    const costTrajectory = useMemo(() => data.advanced_metrics.uncovered_cost_trajectory || [], [data]);
    const readmissionTimeline = useMemo(() => data.advanced_metrics.readmission_timeline || [], [data]);
    const durationDist = useMemo(() => data.advanced_metrics.duration_distribution_by_type || [], [data]);

    const anomalyIndex = useMemo(() => {
        let list = data.advanced_metrics.high_cost_anomaly_index || [];
        return list.sort((a, b) => b.value - a.value).slice(0, 10);
    }, [data])


    // --- KPI Configuration (8 Total) ---
    const kpiData = [
        {
            title: "Total Volume (30d)",
            value: data.kpis.total_visit_volume || 0,
            prevWeek: data.kpis.historical_comparisons?.total_visit_volume?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.total_visit_volume?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.total_visit_volume?.prevYear,
            icon: Users, iconBg: "bg-teal-50", iconColor: "text-teal-600", sentiment: "neutral",
            infoText: "Total number of encounters across all hospitals"
        },
        {
            title: "Unique Pts (30d)",
            value: data.kpis.unique_patients_seen || 0,
            prevWeek: data.kpis.historical_comparisons?.unique_patients_seen?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.unique_patients_seen?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.unique_patients_seen?.prevYear,
            icon: UserCheck, iconBg: "bg-emerald-50", iconColor: "text-emerald-600", sentiment: "neutral",
            infoText: "Number of distinct patients seen in 30 days"
        },
        {
            title: "Total Revenue",
            value: data.kpis.total_revenue_generated || 0,
            prevWeek: data.kpis.historical_comparisons?.total_revenue_generated?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.total_revenue_generated?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.total_revenue_generated?.prevYear,
            icon: DollarSign, iconBg: "bg-purple-50", iconColor: "text-purple-600", sentiment: "higher-is-better",
            infoText: "Sum of total fees across all encounters"
        },
        {
            title: "Avg Duration",
            value: data.kpis.average_encounter_duration_hours || 0,
            prevWeek: data.kpis.historical_comparisons?.average_encounter_duration_hours?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.average_encounter_duration_hours?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.average_encounter_duration_hours?.prevYear,
            icon: ClockIcon, iconBg: "bg-amber-50", iconColor: "text-amber-600",
            infoText: "Average time spent per encounter in hours"
        },
        {
            title: "Avg Practitioner Load",
            value: data.kpis.average_practitioner_load || 0,
            prevWeek: data.kpis.historical_comparisons?.average_practitioner_load?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.average_practitioner_load?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.average_practitioner_load?.prevYear,
            icon: Stethoscope, iconBg: "bg-indigo-50", iconColor: "text-indigo-600",
            infoText: "Average number of encounters handled per practitioner"
        },
        {
            title: "Avg Base Fee",
            value: data.kpis.average_base_fee || 0,
            prevWeek: data.kpis.historical_comparisons?.average_base_fee?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.average_base_fee?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.average_base_fee?.prevYear,
            icon: Activity, iconBg: "bg-slate-100", iconColor: "text-slate-600", sentiment: "neutral",
            infoText: "Average base cost before addons"
        },
        {
            title: "Total Covered (Ins)",
            value: data.kpis.total_covered_amount || 0,
            prevWeek: data.kpis.historical_comparisons?.total_covered_amount?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.total_covered_amount?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.total_covered_amount?.prevYear,
            icon: ShieldCheck, iconBg: "bg-blue-50", iconColor: "text-blue-600", sentiment: "higher-is-better",
            infoText: "Total amount paid by insurers"
        },
        {
            title: "Patient Out-of-Pocket",
            value: data.kpis.average_patient_out_of_pocket || 0,
            prevWeek: data.kpis.historical_comparisons?.average_patient_out_of_pocket?.prevWeek,
            prevMonth: data.kpis.historical_comparisons?.average_patient_out_of_pocket?.prevMonth,
            prevYear: data.kpis.historical_comparisons?.average_patient_out_of_pocket?.prevYear,
            icon: AlertTriangle, iconBg: "bg-rose-50", iconColor: "text-rose-600",
            infoText: "Average financial burden falling on patients per encounter"
        },
    ];

    if (error) {
        return (
            <div className="flex min-h-screen w-full bg-slate-50 items-center justify-center flex-col gap-4">
                <AlertTriangle size={48} className="text-rose-600" />
                <h2 className="text-xl font-bold text-slate-800">{error}</h2>
            </div>
        );
    }

    if (loading) {
        return <LoadingScreen message="Loading Encounter Records..." subtext="Please wait while we gather the information." />;
    }

    return (
        <div className="animate-fade-in w-full">
            <header className="bg-white/80 backdrop-blur-md border-b border-slate-200 py-6 px-4 md:px-6 lg:px-8 sticky top-0 z-20 w-full">
                <div className="max-w-[1600px] mx-auto w-full">
                    <div className="flex items-center gap-3 mb-2">
                        <div className="p-2 bg-purple-50 rounded-lg">
                            <BriefcaseMedical size={24} className="text-purple-600" />
                        </div>
                        <h1 className="text-3xl font-black text-slate-900 tracking-tight">Encounters Overview</h1>
                    </div>
                    <p className="text-slate-500 font-medium ml-12">Key statistics and trends for clinical encounters and operations.</p>
                </div>
            </header>

            <div className="max-w-[1600px] mx-auto w-full px-4 md:px-6 lg:px-8 py-8 space-y-10 pb-10">
                {/* Section 1: 8 KPIs */}
                <KPICard kpis={kpiData} />

                {/* SECTION 2: 6 STANDARD GRAPHICAL METRICS */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">

                    <div className="lg:col-span-1">
                        <MetricsCard title="Encounter Types" metrics={[]} chartData={encountersByType} chartType="pie" infoText="Distribution of different patient encounter types (e.g., ambulatory, emergency, wellness) over the current period.">
                            <ReactECharts
                                option={{
                                    tooltip: { trigger: 'item', backgroundColor: 'rgba(255, 255, 255, 0.95)', borderRadius: 8, textStyle: { color: '#334155' } },
                                    legend: { bottom: 0, left: 'center', icon: 'circle', itemWidth: 8, itemHeight: 8, textStyle: { fontSize: 10, color: '#64748b' }, itemGap: 10 },
                                    color: PIE_COLORS,
                                    series: [
                                        {
                                            type: 'pie',
                                            center: ['50%', '42%'],
                                            radius: ['45%', '70%'],
                                            avoidLabelOverlap: false,
                                            itemStyle: { borderRadius: 8, borderColor: '#fff', borderWidth: 3 },
                                            label: { show: false },
                                            data: encountersByType
                                        }
                                    ]
                                }}
                                style={{ height: '300px', width: '100%' }}
                                opts={{ renderer: 'svg' }}
                            />
                        </MetricsCard>
                    </div>

                    <div className="lg:col-span-2">
                        <MetricsCard title="Coverage vs OOP by Type" metrics={[]} chartData={coverageOOP} chartType="bar" infoText="Comparison between amounts covered by insurance vs out-of-pocket costs paid by patients, categorized by encounter type.">
                            <ReactECharts
                                option={{
                                    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'rgba(255, 255, 255, 0.95)', borderRadius: 12, padding: 12, textStyle: { color: '#0f172a' }, valueFormatter: (val) => `$${Number(val).toFixed(2)}` },
                                    legend: { bottom: 0, icon: 'circle', itemWidth: 10, itemHeight: 10, textStyle: { color: '#64748b' } },
                                    grid: { left: '3%', right: '4%', bottom: '15%', top: '5%', containLabel: true },
                                    xAxis: [{ type: 'category', data: coverageOOP.map(d => truncateLabel(d.name, 15)), axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: '#64748b', fontSize: 11, fontWeight: 'bold' } }],
                                    yAxis: [{ type: 'value', axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: '#64748b', fontSize: 11 }, splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } } }],
                                    series: [
                                        { name: 'Covered Amount', type: 'bar', stack: 'total', itemStyle: { color: '#3b82f6', borderRadius: [0, 0, 4, 4] }, barWidth: '40%', data: coverageOOP.map(d => d.covered) },
                                        { name: 'Out-of-Pocket', type: 'bar', stack: 'total', itemStyle: { color: '#f43f5e', borderRadius: [4, 4, 0, 0] }, barWidth: '40%', data: coverageOOP.map(d => d.oop) }
                                    ]
                                }}
                                style={{ height: '300px', width: '100%' }}
                                opts={{ renderer: 'svg' }}
                            />
                        </MetricsCard>
                    </div>

                    <div className="lg:col-span-1">
                        <MetricsCard title="Top 10 Encounter Causes" metrics={[]} chartData={topCauses} chartType="bar" infoText="The most frequent medical conditions or reasons patients visited the hospital during the selected timeframe.">
                            <ResponsiveContainer width="100%" height="100%" minHeight={350}>
                                <BarChart layout="vertical" data={topCauses} margin={{ left: 20, right: 30, top: 10, bottom: 10 }}>
                                    <CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
                                    <XAxis type="number" hide />
                                    <YAxis dataKey="name" type="category" width={180} tickFormatter={(val) => truncateLabel(val, 25)} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
                                    <Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} labelFormatter={(label, payload) => payload?.[0]?.payload?.name || label} />
                                    <Bar dataKey="value" fill="#f43f5e" radius={[0, 4, 4, 0]} barSize={16} />
                                </BarChart>
                            </ResponsiveContainer>
                        </MetricsCard>
                    </div>

                    <div className="lg:col-span-2">
                        <MetricsCard title="Most Expensive Causes" metrics={[]} chartData={mostExpensiveCauses} chartType="bar" infoText="The conditions or encounter reasons that incur the highest average total fees.">
                            <ReactECharts
                                option={{
                                    tooltip: { trigger: 'axis', axisPointer: { type: 'shadow' }, backgroundColor: 'rgba(255, 255, 255, 0.95)', borderRadius: 12, valueFormatter: (val) => `$${Number(val).toFixed(2)}` },
                                    grid: { left: '3%', right: '10%', bottom: '3%', top: '3%', containLabel: true },
                                    xAxis: { type: 'value', show: false },
                                    yAxis: { type: 'category', data: mostExpensiveCauses.map(d => truncateLabel(d.name, 25)), axisLine: { show: false }, axisTick: { show: false }, axisLabel: { color: '#64748b', fontSize: 10, fontWeight: 'bold' }, inverse: true },
                                    series: [
                                        {
                                            name: 'Avg Cost',
                                            type: 'bar',
                                            data: mostExpensiveCauses.map(d => d.value),
                                            itemStyle: { color: '#8b5cf6', borderRadius: [0, 8, 8, 0] },
                                            barWidth: 16,
                                            label: { show: true, position: 'right', formatter: (params) => `$${Number(params.value).toLocaleString()}`, color: '#64748b', fontSize: 10, fontWeight: 'bold' }
                                        }
                                    ]
                                }}
                                style={{ height: '350px', width: '100%' }}
                                opts={{ renderer: 'svg' }}
                            />
                        </MetricsCard>
                    </div>

                    <div className="lg:col-span-2">
                        <MetricsCard title="Fee Divergence by Encounter Type" metrics={[]} chartData={feeDivergence} chartType="bar" infoText="Difference between the standard base fee and the actual total fee charged, broken down by encounter type.">
                            <ResponsiveContainer width="100%" height="100%" minHeight={300}>
                                <BarChart data={feeDivergence} margin={{ top: 20, right: 30, left: 0, bottom: 5 }}>
                                    <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                    <XAxis dataKey="name" tickFormatter={(val) => truncateLabel(val, 15)} tick={{ fontSize: 11, fill: '#64748b', fontWeight: 600 }} axisLine={false} tickLine={false} interval={0} />
                                    <YAxis tick={{ fontSize: 11, fill: '#64748b' }} axisLine={false} tickLine={false} />
                                    <Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} formatter={(value) => `$${Number(value).toFixed(2)} `} />
                                    <Legend iconType="circle" wrapperStyle={{ fontSize: '12px', paddingTop: '10px' }} />
                                    <Bar dataKey="base" name="Base Fee" fill="#94a3b8" radius={[4, 4, 0, 0]} barSize={20} />
                                    <Bar dataKey="total" name="Total Fee" fill="#f59e0b" radius={[4, 4, 0, 0]} barSize={20} />
                                </BarChart>
                            </ResponsiveContainer>
                        </MetricsCard>
                    </div>

                    <div className="lg:col-span-1">
                        <MetricsCard title="Top 10 Practitioners" metrics={[]} chartData={topPractitioners} chartType="bar" infoText="The medical practitioners with the highest volume of patient encounters.">
                            <ResponsiveContainer width="100%" height="100%" minHeight={300}>
                                <BarChart layout="vertical" data={topPractitioners} margin={{ left: 0, right: 30, top: 10, bottom: 10 }}>
                                    <CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
                                    <XAxis type="number" hide />
                                    <YAxis dataKey="name" type="category" width={80} tickFormatter={(val) => `MD - ${truncateLabel(val, 4)} `} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
                                    <Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} labelFormatter={(label, payload) => `Practitioner ${payload?.[0]?.payload?.name || label} `} />
                                    <Bar dataKey="value" name="Encounters" fill="#14b8a6" radius={[0, 4, 4, 0]} barSize={12} />
                                </BarChart>
                            </ResponsiveContainer>
                        </MetricsCard>
                    </div>

                </div>

                {/* SECTION 3: 4 GRAPHICAL ADVANCED METRICS */}
                <div className="pt-10 border-t border-slate-200">
                    <h2 className="text-2xl font-black text-slate-800 tracking-tight mb-8 flex items-center gap-3">
                        <BarChart3 className="text-purple-600" /> Advanced Interaction Metrics
                    </h2>

                    <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
                        {/* 1. Patient Burden Trajectory */}
                        <div className="bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100" style={{ overflow: 'visible' }}>
                            <div className="flex justify-between items-start mb-6">
                                <div className="flex items-start gap-2" style={{ overflow: 'visible' }}>
                                    <div>
                                        <h3 className="font-bold text-slate-800 flex items-center gap-2">
                                            <TrendingUp size={18} className="text-rose-500" /> Patient Burden Trajectory
                                        </h3>
                                        <p className="text-sm text-slate-500 mt-1">Average Monthly Out-Of-Pocket Cost ($)</p>
                                    </div>
                                    <div className="relative group/tooltip flex items-center mt-1" style={{ overflow: 'visible' }}>
                                        <Info size={16} className="text-slate-400 hover:text-teal-600 transition-colors cursor-help" />
                                        <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-max max-w-sm bg-slate-800 text-white text-xs rounded-xl p-3 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-xl pointer-events-none normal-case tracking-normal font-normal">
                                            <div className="absolute left-1/2 -translate-x-1/2 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-800"></div>
                                            Tracks how average patient out-of-pocket costs are trending over time to identify growing financial burdens.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="h-[300px] w-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <LineChart data={costTrajectory} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
                                        <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} tickFormatter={(val) => `$${val}`} width={55} />
                                        <Tooltip contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)' }} formatter={(value) => [`$${Number(value).toFixed(2)}`, 'Avg OOP']} />
                                        <Line type="monotone" dataKey="value" stroke="#f43f5e" strokeWidth={3} dot={{ r: 4, fill: "#f43f5e", strokeWidth: 2, stroke: "#fff" }} activeDot={{ r: 6 }} />
                                    </LineChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        {/* 2. Readmission & Retention Timeline */}
                        <div className="bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100" style={{ overflow: 'visible' }}>
                            <div className="flex justify-between items-start mb-6">
                                <div className="flex items-start gap-2" style={{ overflow: 'visible' }}>
                                    <div>
                                        <h3 className="font-bold text-slate-800 flex items-center gap-2">
                                            <Hospital size={18} className="text-amber-500" /> Readmission & Retention Timeline
                                        </h3>
                                        <p className="text-sm text-slate-500 mt-1">Unique Patients vs Repeat Visits (Last 12 Months)</p>
                                    </div>
                                    <div className="relative group/tooltip flex items-center mt-1" style={{ overflow: 'visible' }}>
                                        <Info size={16} className="text-slate-400 hover:text-teal-600 transition-colors cursor-help" />
                                        <div className="absolute left-0 top-full mt-2 w-72 bg-slate-800 text-white text-xs rounded-xl p-3 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-xl pointer-events-none">
                                            <div className="absolute left-4 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-800"></div>
                                            Compares the number of distinct patients returning for multiple encounters versus entirely new patients each month.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="h-[300px] w-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <AreaChart data={readmissionTimeline} margin={{ top: 5, right: 20, left: 10, bottom: 5 }}>
                                        <defs>
                                            <linearGradient id="colorUnique" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#14b8a6" stopOpacity={0.8} />
                                                <stop offset="95%" stopColor="#14b8a6" stopOpacity={0} />
                                            </linearGradient>
                                            <linearGradient id="colorRepeat" x1="0" y1="0" x2="0" y2="1">
                                                <stop offset="5%" stopColor="#f59e0b" stopOpacity={0.8} />
                                                <stop offset="95%" stopColor="#f59e0b" stopOpacity={0} />
                                            </linearGradient>
                                        </defs>
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <XAxis dataKey="name" tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
                                        <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} width={40} />
                                        <Tooltip contentStyle={{ borderRadius: '12px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)' }} />
                                        <Legend iconType="circle" wrapperStyle={{ fontSize: '12px', paddingTop: '8px' }} />
                                        <Area type="monotone" dataKey="unique_patients" name="Unique Patients" stroke="#14b8a6" fillOpacity={1} fill="url(#colorUnique)" strokeWidth={2} />
                                        <Area type="monotone" dataKey="repeat_patients" name="Repeat Visitors" stroke="#f59e0b" fillOpacity={1} fill="url(#colorRepeat)" strokeWidth={2} />
                                    </AreaChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        {/* 3. Duration Distribution */}
                        <div className="bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100" style={{ overflow: 'visible' }}>
                            <div className="flex justify-between items-start mb-6">
                                <div className="flex items-start gap-2" style={{ overflow: 'visible' }}>
                                    <div>
                                        <h3 className="font-bold text-slate-800 flex items-center gap-2">
                                            <ClockIcon size={18} className="text-indigo-500" /> Duration Distribution
                                        </h3>
                                        <p className="text-sm text-slate-500 mt-1">Average Time in Hours by Encounter Type</p>
                                    </div>
                                    <div className="relative group/tooltip flex items-center mt-1" style={{ overflow: 'visible' }}>
                                        <Info size={16} className="text-slate-400 hover:text-teal-600 transition-colors cursor-help" />
                                        <div className="absolute left-0 top-full mt-2 w-72 bg-slate-800 text-white text-xs rounded-xl p-3 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-xl pointer-events-none">
                                            <div className="absolute left-4 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-800"></div>
                                            Analyzes how long different types of encounters typically take from start to finish.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="h-[300px] w-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <BarChart data={durationDist} margin={{ top: 5, right: 20, left: 10, bottom: 20 }}>
                                        <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                        <XAxis dataKey="name" tickFormatter={(val) => truncateLabel(val, 12)} tick={{ fontSize: 10, fill: '#64748b', fontWeight: 600 }} axisLine={false} tickLine={false} interval={0} angle={-20} textAnchor="end" />
                                        <YAxis tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} tickFormatter={(val) => `${val}h`} width={40} />
                                        <Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} formatter={(value) => [`${Number(value).toFixed(2)} hrs`, 'Avg Duration']} />
                                        <Bar dataKey="value" fill="#8b5cf6" radius={[4, 4, 0, 0]} barSize={36}>
                                            {durationDist.map((entry, index) => (
                                                <Cell key={`cell-dur-${index}`} fill={['#6366f1', '#8b5cf6', '#d946ef', '#ec4899', '#f43f5e'][index % 5]} />
                                            ))}
                                        </Bar>
                                    </BarChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                        {/* 4. High-Cost Anomaly Index */}
                        <div className="bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100" style={{ overflow: 'visible' }}>
                            <div className="flex justify-between items-start mb-6">
                                <div className="flex items-start gap-2" style={{ overflow: 'visible' }}>
                                    <div>
                                        <h3 className="font-bold text-slate-800 flex items-center gap-2">
                                            <AlertTriangle size={18} className="text-rose-500" /> High-Cost Anomaly Flags
                                        </h3>
                                        <p className="text-sm text-slate-500 mt-1">Encounter causes exceeding 2 Std Dev in cost (Top 10)</p>
                                    </div>
                                    <div className="relative group/tooltip flex items-center mt-1" style={{ overflow: 'visible' }}>
                                        <Info size={16} className="text-slate-400 hover:text-teal-600 transition-colors cursor-help" />
                                        <div className="absolute left-0 top-full mt-2 w-72 bg-slate-800 text-white text-xs rounded-xl p-3 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-xl pointer-events-none">
                                            <div className="absolute left-4 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-800"></div>
                                            Identifies specific encounter reasons that lead to unusually high treatment costs compared to the average encounter.
                                        </div>
                                    </div>
                                </div>
                            </div>
                            <div className="h-[300px] w-full">
                                <ResponsiveContainer width="100%" height="100%">
                                    <ComposedChart layout="vertical" data={anomalyIndex} margin={{ top: 5, right: 20, bottom: 5, left: 0 }}>
                                        <CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
                                        <XAxis type="number" tick={{ fontSize: 11, fill: '#94a3b8' }} axisLine={false} tickLine={false} hide />
                                        <YAxis dataKey="name" type="category" width={150} tickFormatter={(val) => truncateLabel(val, 22)} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
                                        <Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} labelFormatter={(label, payload) => payload?.[0]?.payload?.name || label} formatter={(value) => [value, 'Anomalies Detected']} />
                                        <Bar dataKey="value" barSize={14} fill="#ffe4e6" radius={[0, 4, 4, 0]} />
                                        <Scatter dataKey="value" fill="#e11d48" />
                                    </ComposedChart>
                                </ResponsiveContainer>
                            </div>
                        </div>

                    </div>
                </div>
            </div>
        </div>
    );
};

export default EncountersDashboard;
