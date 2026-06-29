import React, { useState, useEffect, useMemo } from 'react';
import { patientDashboard, conditionsDashboard, encountersDashboard } from "@/api/api";
import { Users, Activity, Clock as ClockIcon, ShieldCheck, TrendingUp, AlertCircle, HeartPulse, Database, Stethoscope, Banknote, Shield, Wallet, BarChart3 } from 'lucide-react';
import KPICard from "@/components/KPICard";
import { Link } from 'react-router-dom';
import ReactECharts from 'echarts-for-react';
import LoadingScreen from "@/components/LoadingScreen";

const MainDashboard = () => {
    const [stats, setStats] = useState({
        patients: {},
        conditions: {},
        encounters: {},
        patientTrends: {},
        conditionMetrics: {}
    });
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        const fetchData = async () => {
            try {
                // Parallel fetching for "Executive View"
                const [patientData, conditionData, encounterData] = await Promise.all([
                    patientDashboard(),
                    conditionsDashboard(),
                    encountersDashboard()
                ]);

                setStats({
                    patients: patientData?.kpis || {},
                    conditions: conditionData?.conditions_dashboard?.kpis || {},
                    encounters: encounterData?.encounters_dashboard?.kpis || {},
                    patientTrends: patientData?.metrics || {},
                    conditionMetrics: conditionData?.conditions_dashboard?.metrics || {}
                });
            } catch (err) {
                console.error("Failed to load aggregate data", err);
            } finally {
                setLoading(false);
            }
        };
        fetchData();
    }, []);

    // Construct Composite KPIs
    // Construct Composite KPIs
    const kpiData = [
        {
            title: "Total Population",
            value: stats.patients.total_patients || 0,
            prevWeek: stats.patients.historical_comparisons?.total_patients?.prevWeek,
            prevMonth: stats.patients.historical_comparisons?.total_patients?.prevMonth,
            prevYear: stats.patients.historical_comparisons?.total_patients?.prevYear,
            icon: Users, iconBg: "bg-blue-50", iconColor: "text-blue-600",
        },
        {
            title: "Active Condition Burden",
            value: stats.conditions.current_active_burden || 0,
            prevWeek: stats.conditions.historical_comparisons?.current_active_burden?.prevWeek,
            prevMonth: stats.conditions.historical_comparisons?.current_active_burden?.prevMonth,
            prevYear: stats.conditions.historical_comparisons?.current_active_burden?.prevYear,
            icon: Activity, iconBg: "bg-rose-50", iconColor: "text-rose-600",
        },
        {
            title: "Avg Cure Time",
            value: stats.conditions.average_time_to_cure || 0,
            prevWeek: stats.conditions.historical_comparisons?.average_time_to_cure?.prevWeek,
            prevMonth: stats.conditions.historical_comparisons?.average_time_to_cure?.prevMonth,
            prevYear: stats.conditions.historical_comparisons?.average_time_to_cure?.prevYear,
            icon: ClockIcon, iconBg: "bg-amber-50", iconColor: "text-amber-600",
            sentiment: "lower-is-better"
        },
        {
            title: "Global Recovery Rate",
            value: stats.conditions.global_recovery_rate || 0,
            prevWeek: stats.conditions.historical_comparisons?.global_recovery_rate?.prevWeek,
            prevMonth: stats.conditions.historical_comparisons?.global_recovery_rate?.prevMonth,
            prevYear: stats.conditions.historical_comparisons?.global_recovery_rate?.prevYear,
            icon: ShieldCheck, iconBg: "bg-teal-50", iconColor: "text-teal-600",
            sentiment: "higher-is-better"
        },
        {
            title: "Total Revenue",
            value: stats.encounters?.total_revenue_generated || 0,
            prevWeek: stats.encounters?.historical_comparisons?.total_revenue_generated?.prevWeek,
            prevMonth: stats.encounters?.historical_comparisons?.total_revenue_generated?.prevMonth,
            prevYear: stats.encounters?.historical_comparisons?.total_revenue_generated?.prevYear,
            icon: Banknote, iconBg: "bg-emerald-50", iconColor: "text-emerald-600",
        },
        {
            title: "Encounters (30d)",
            value: stats.encounters?.unique_patients_seen || 0,
            prevWeek: stats.encounters?.historical_comparisons?.unique_patients_seen?.prevWeek,
            prevMonth: stats.encounters?.historical_comparisons?.unique_patients_seen?.prevMonth,
            prevYear: stats.encounters?.historical_comparisons?.unique_patients_seen?.prevYear,
            icon: Stethoscope, iconBg: "bg-blue-50", iconColor: "text-blue-600",
        },
        {
            title: "Insurer Covered",
            value: stats.encounters?.total_covered_amount || 0,
            prevWeek: stats.encounters?.historical_comparisons?.total_covered_amount?.prevWeek,
            prevMonth: stats.encounters?.historical_comparisons?.total_covered_amount?.prevMonth,
            prevYear: stats.encounters?.historical_comparisons?.total_covered_amount?.prevYear,
            icon: Shield, iconBg: "bg-indigo-50", iconColor: "text-indigo-600",
            sentiment: "higher-is-better"
        },
        {
            title: "Avg Out-of-Pocket",
            value: stats.encounters?.average_patient_out_of_pocket || 0,
            prevWeek: stats.encounters?.historical_comparisons?.average_patient_out_of_pocket?.prevWeek,
            prevMonth: stats.encounters?.historical_comparisons?.average_patient_out_of_pocket?.prevMonth,
            prevYear: stats.encounters?.historical_comparisons?.average_patient_out_of_pocket?.prevYear,
            icon: Wallet, iconBg: "bg-rose-50", iconColor: "text-rose-600",
            sentiment: "lower-is-better"
        }
    ];

    // Mock Trend Data for "System Health" visualization if real trend data isn't easily available in this format
    const systemHealthData = [
        { name: 'Jan', load: 65, efficiency: 80 },
        { name: 'Feb', load: 59, efficiency: 82 },
        { name: 'Mar', load: 80, efficiency: 75 },
        { name: 'Apr', load: 81, efficiency: 78 },
        { name: 'May', load: 56, efficiency: 85 },
        { name: 'Jun', load: 55, efficiency: 88 },
        { name: 'Jul', load: 40, efficiency: 90 },
    ];

    if (loading) {
        return <LoadingScreen message="Loading System Overview..." subtext="Please wait while we gather the information." />;
    }

    return (
        <div className="animate-fade-in w-full">
            <div className="max-w-[1600px] mx-auto w-full px-4 md:px-6 lg:px-8 py-8 space-y-8">
                {/* KPI Grid */}
                <KPICard kpis={kpiData} />

                {/* Quick Actions / Navigation Grid */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                    {/* Card 1: Patient Highlights */}
                    <div className="bg-gradient-to-br from-blue-600 to-indigo-700 rounded-2xl p-6 text-white shadow-xl shadow-blue-200 overflow-hidden relative group">
                        <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity transform group-hover:scale-110 duration-500">
                            <Users size={120} />
                        </div>
                        <div className="relative z-10">
                            <h3 className="text-blue-100 font-bold text-sm uppercase tracking-wider mb-2">Demographics</h3>
                            <p className="text-3xl font-black mb-1">Patient Analytics</p>
                            <p className="text-blue-100/80 text-sm mb-6 max-w-[80%]">Deep dive into population health, economic factors, and mortality trends.</p>
                            <Link to="/patient_dashboard" className="inline-flex items-center gap-2 bg-white/20 hover:bg-white/30 backdrop-blur-md px-4 py-2 rounded-lg text-sm font-bold transition-colors">
                                View Dashboard <TrendingUp size={16} />
                            </Link>
                        </div>
                    </div>

                    {/* Card 2: Clinical Highlights */}
                    <div className="bg-gradient-to-br from-teal-500 to-emerald-600 rounded-2xl p-6 text-white shadow-xl shadow-teal-200 overflow-hidden relative group">
                        <div className="absolute top-0 right-0 p-8 opacity-10 group-hover:opacity-20 transition-opacity transform group-hover:scale-110 duration-500">
                            <Activity size={120} />
                        </div>
                        <div className="relative z-10">
                            <h3 className="text-teal-100 font-bold text-sm uppercase tracking-wider mb-2">Epidemiology</h3>
                            <p className="text-3xl font-black mb-1">Conditions & Pathology</p>
                            <p className="text-teal-100/80 text-sm mb-6 max-w-[80%]">Track disease outbreaks, comorbidity patterns, and treatment efficiency.</p>
                            <Link to="/conditions_dashboard" className="inline-flex items-center gap-2 bg-white/20 hover:bg-white/30 backdrop-blur-md px-4 py-2 rounded-lg text-sm font-bold transition-colors">
                                Analyze Trends <Activity size={16} />
                            </Link>
                        </div>
                    </div>

                    {/* Card 3: System Status (Visual Filler) */}
                    <div className="bg-white rounded-2xl p-6 shadow-sm border border-slate-200 relative overflow-hidden">
                        <div className="flex justify-between items-start mb-4">
                            <div>
                                <h3 className="text-slate-500 font-bold text-xs uppercase tracking-wider mb-1">System Health</h3>
                                <p className="text-2xl font-black text-slate-800">Operational</p>
                            </div>
                            <div className="w-2 h-2 rounded-full bg-emerald-500 shadow-[0_0_8px_rgba(16,185,129,0.6)] animate-pulse"></div>
                        </div>
                        {/* Tiny Sparkline Area Chart */}
                        <div className="h-24 w-full">
                            <ReactECharts
                                option={{
                                    tooltip: { show: false },
                                    grid: { left: 0, right: 0, top: 5, bottom: 5 },
                                    xAxis: {
                                        type: 'category',
                                        data: systemHealthData.map(d => d.name),
                                        show: false
                                    },
                                    yAxis: {
                                        type: 'value',
                                        show: false
                                    },
                                    series: [
                                        {
                                            name: 'Efficiency',
                                            type: 'line',
                                            smooth: true,
                                            showSymbol: false,
                                            data: systemHealthData.map(d => d.efficiency),
                                            itemStyle: { color: '#10b981' },
                                            lineStyle: { width: 2 },
                                            areaStyle: {
                                                color: {
                                                    type: 'linear',
                                                    x: 0, y: 0, x2: 0, y2: 1,
                                                    colorStops: [
                                                        { offset: 0, color: 'rgba(16, 185, 129, 0.25)' },
                                                        { offset: 1, color: 'rgba(16, 185, 129, 0)' }
                                                    ]
                                                }
                                            }
                                        }
                                    ]
                                }}
                                style={{ height: '100%', width: '100%' }}
                                opts={{ renderer: 'svg' }}
                            />
                        </div>
                        <div className="flex gap-2 mt-2">
                            <div className="text-xs font-bold text-slate-400 bg-slate-50 px-2 py-1 rounded">ETL: Idle</div>
                            <div className="text-xs font-bold text-slate-400 bg-slate-50 px-2 py-1 rounded">API: Active</div>
                        </div>
                    </div>
                </div>

                {/* Quick Stats Row */}
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-8">
                    {/* Recent Alerts (Mock for now, but stylized) */}
                    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
                        <h3 className="font-bold text-slate-800 text-lg mb-4 flex items-center gap-2">
                            <AlertCircle className="text-amber-500" size={20} /> System Alerts
                        </h3>
                        <div className="space-y-3">
                            <div className="flex items-center justify-between p-3 bg-amber-50 rounded-xl border border-amber-100">
                                <div className="flex items-center gap-3">
                                    <div className="w-2 h-2 rounded-full bg-amber-500"></div>
                                    <span className="text-sm font-bold text-slate-700">High Readmission Rate Detected</span>
                                </div>
                                <span className="text-xs text-slate-400 font-mono">10m ago</span>
                            </div>
                            <div className="flex items-center justify-between p-3 bg-blue-50 rounded-xl border border-blue-100">
                                <div className="flex items-center gap-3">
                                    <div className="w-2 h-2 rounded-full bg-blue-500"></div>
                                    <span className="text-sm font-bold text-slate-700">New Patient Data Ingested</span>
                                </div>
                                <span className="text-xs text-slate-400 font-mono">1h ago</span>
                            </div>
                        </div>
                    </div>

                    {/* Aggregate Efficiency Chart */}
                    <div className="bg-white rounded-2xl border border-slate-200 shadow-sm p-6">
                        <h3 className="font-bold text-slate-800 text-lg mb-4 flex items-center gap-2">
                            <BarChart3 className="text-purple-500" size={20} /> Throughput Metrics
                        </h3>
                        <div className="h-40 w-full">
                            <ReactECharts
                                option={{
                                    tooltip: {
                                        trigger: 'axis',
                                        backgroundColor: 'rgba(255, 255, 255, 0.95)',
                                        borderRadius: 12,
                                        borderWidth: 0,
                                        shadowColor: 'rgba(0, 0, 0, 0.05)',
                                        shadowBlur: 10,
                                        textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
                                    },
                                    grid: { left: '3%', right: '3%', bottom: '5%', top: '10%', containLabel: true },
                                    xAxis: {
                                        type: 'category',
                                        data: ['Admit', 'Treat', 'Discharge', 'Bill'],
                                        axisLine: { show: false },
                                        axisTick: { show: false },
                                        axisLabel: { color: '#64748b', fontSize: 11, fontWeight: 'bold' }
                                    },
                                    yAxis: {
                                        type: 'value',
                                        axisLine: { show: false },
                                        axisTick: { show: false },
                                        axisLabel: { color: '#94a3b8', fontSize: 11 },
                                        splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
                                    },
                                    series: [
                                        {
                                            name: 'Throughput',
                                            type: 'bar',
                                            barWidth: '40%',
                                            data: [
                                                { value: 80, itemStyle: { color: '#3b82f6', borderRadius: [4, 4, 0, 0] } },
                                                { value: 65, itemStyle: { color: '#8b5cf6', borderRadius: [4, 4, 0, 0] } },
                                                { value: 90, itemStyle: { color: '#10b981', borderRadius: [4, 4, 0, 0] } },
                                                { value: 100, itemStyle: { color: '#f59e0b', borderRadius: [4, 4, 0, 0] } }
                                            ]
                                        }
                                    ]
                                }}
                                style={{ height: '100%', width: '100%' }}
                                opts={{ renderer: 'svg' }}
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MainDashboard;
