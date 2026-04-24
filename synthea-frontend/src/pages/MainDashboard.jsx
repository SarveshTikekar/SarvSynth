import React, { useState, useEffect } from 'react';
import { patientDashboard, conditionsDashboard, encountersDashboard } from "@/api/api";
import { Users, Activity, Clock, ShieldCheck, TrendingUp, AlertCircle, HeartPulse, Database, Stethoscope, Banknote, Shield, Wallet, BarChart3 } from 'lucide-react';
import KPICard from "@/components/KPICard";
import { Link } from 'react-router-dom';
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, BarChart, Bar, Cell } from 'recharts';
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
            icon: Clock, iconBg: "bg-amber-50", iconColor: "text-amber-600",
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
            {/* Header */}
            <header className="bg-white/80 backdrop-blur-md border-b border-slate-200 py-6 px-4 md:px-6 lg:px-8 sticky top-0 z-20 w-full">
                <div className="flex justify-between items-center max-w-[1600px] mx-auto w-full">
                    <div>
                        <div className="flex items-center gap-3 mb-2">
                            <div className="p-2 bg-rose-50 rounded-lg">
                                <HeartPulse size={24} className="text-rose-500" />
                            </div>
                            <h1 className="text-3xl font-black text-slate-900 tracking-tight">
                                Executive Overview
                            </h1>
                        </div>
                        <p className="text-slate-500 font-medium ml-12">Aggregate statistics and metrics across all departments.</p>
                    </div>
                    <div className="hidden md:flex gap-3">
                        <Link to="/data_generation" className="px-4 py-2 bg-slate-900 text-white rounded-lg text-sm font-bold hover:bg-slate-800 transition-colors shadow-lg shadow-slate-200">
                            System Controls
                        </Link>
                    </div>
                </div>
            </header>

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
                            <ResponsiveContainer width="100%" height="100%">
                                <AreaChart data={systemHealthData}>
                                    <defs>
                                        <linearGradient id="colorEfficiency" x1="0" y1="0" x2="0" y2="1">
                                            <stop offset="5%" stopColor="#10b981" stopOpacity={0.3} />
                                            <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                        </linearGradient>
                                    </defs>
                                    <Area type="monotone" dataKey="efficiency" stroke="#10b981" strokeWidth={2} fill="url(#colorEfficiency)" />
                                </AreaChart>
                            </ResponsiveContainer>
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
                            <ResponsiveContainer width="100%" height="100%">
                                <BarChart data={[{ name: 'Admit', val: 80 }, { name: 'Treat', val: 65 }, { name: 'Discharge', val: 90 }, { name: 'Bill', val: 100 }]}>
                                    <Bar dataKey="val" radius={[4, 4, 0, 0]}>
                                        {
                                            [0, 1, 2, 3].map((entry, index) => (
                                                <Cell key={`cell-${index}`} fill={['#3b82f6', '#8b5cf6', '#10b981', '#f59e0b'][index]} />
                                            ))
                                        }
                                    </Bar>
                                    <XAxis dataKey="name" axisLine={false} tickLine={false} tick={{ fontSize: 12 }} />
                                    <Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '8px' }} />
                                </BarChart>
                            </ResponsiveContainer>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MainDashboard;
