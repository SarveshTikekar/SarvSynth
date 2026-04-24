import React, { useState, useEffect, useMemo } from "react";
import { conditionsDashboard } from "@/api/api";
import {
	Activity, CheckCircle, GitMerge, Clock, Hospital,
	Stethoscope, BarChart3, TrendingUp, Search, X, Info, Filter, Users, Database, AlertTriangle, FileText, Fingerprint, RefreshCcw
} from "lucide-react";
import KPICard from "@/components/KPICard";
import MetricsCard from "@/components/MetricsCard";
import AdvancedChartCard from "@/components/AdvancedChartCard";
import LoadingScreen from "@/components/LoadingScreen";
import {
	BarChart, Bar, PieChart, Pie, Cell,
	XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer,
	LineChart, Line
} from "recharts";

// --- Colors & Gradients ---
const COLORS = {
	teal: ["#14b8a6", "#0d9488"],
	purple: ["#8b5cf6", "#7c3aed"],
	rose: ["#f43f5e", "#e11d48"],
	amber: ["#f59e0b", "#d97706"],
	blue: ["#3b82f6", "#2563eb"],
	slate: ["#64748b", "#475569"]
};

const PIE_COLORS = ["#14b8a6", "#f43f5e", "#8b5cf6", "#f59e0b", "#3b82f6"];

const ConditionsDashboard = () => {
	const [data, setData] = useState({ kpis: {}, metrics: {}, advanced_metrics: {} });
	const [loading, setLoading] = useState(true);
	const [error, setError] = useState("");

	// --- Interactivity State ---
	const [incidenceSearch, setIncidenceSearch] = useState("");
	const [selectedIncidence, setSelectedIncidence] = useState(null);
	const [recurrenceSort, setRecurrenceSort] = useState("desc");

	// Graph State
	const [graphLimit, setGraphLimit] = useState(50);
	const [hoveredNode, setHoveredNode] = useState(null);
	const [hoveredLink, setHoveredLink] = useState(null);
	const [seed, setSeed] = useState(1);

	useEffect(() => {
		const fetchData = async () => {
			try {
				const result = await conditionsDashboard();
				if (result?.conditions_dashboard) {
					setData({
						kpis: result.conditions_dashboard.kpis || {},
						metrics: result.conditions_dashboard.metrics || {},
						advanced_metrics: result.conditions_dashboard.advanced_metrics || {}
					});
				} else {
					setError("Failed to load conditions data.");
				}
			} catch (err) {
				setError("Error fetching dashboard data.");
			} finally {
				setLoading(false);
			}
		};
		fetchData();
	}, []);

	// --- Data Transformation Helpers ---
	const transformList = (list) => {
		if (!list) return [];
		return list.flatMap(item => {
			return Object.keys(item).map(key => ({ name: key, value: item[key] }));
		});
	};

	const truncateLabel = (str, max = 5) => {
		if (!str) return "";
		return str.length > max ? `(${str.substring(0, max)}...)` : str;
	};

	// --- Memoized Data (Basic Metrics - 6 Total) ---
	const topDisorders = useMemo(() => transformList(data.metrics.top_disorder_conditions), [data]);
	const recurring = useMemo(() => transformList(data.metrics.top_10_recurring_disorders), [data]);
	const clinicalGravity = useMemo(() => transformList(data.metrics.clinical_gravity).slice(0, 10), [data]);
	const chronicVsAcute = useMemo(() => transformList(data.metrics.chronic_vs_acute), [data]);

	// Metric 3: Disease Resolution Efficiency (Avg Time to Cure)
	const resolutionEfficiency = useMemo(() => {
		// List of tuples [concept, frequency, time]
		const raw = data.metrics.disease_resolution_efficiency || [];
		return raw.slice(0, 10).map(item => ({
			name: item[0],
			frequency: item[1],
			value: item[2] // avg time
		}));
	}, [data]);

	// Metric 5: Comorbidity Pattern (Conditions per patient skew)
	const comorbidityDistribution = useMemo(() => {
		const list = transformList(data.metrics.commorbidity_pattern);
		// Format name to "X Conds"
		return list.map(item => ({
			name: `${item.name} Conds`,
			value: item.value
		}));
	}, [data]);

	// --- Memoized Data (Advanced Metrics - 5 Total) ---
	const ageBurden = useMemo(() => transformList(data.advanced_metrics.age_based_burden), [data]);

	// Advanced 1: Incidence Velocity
	const incidenceData = useMemo(() => {
		if (!data.advanced_metrics.incidence_velocity) return { list: [], chartData: [] };
		const allConditions = Object.keys(data.advanced_metrics.incidence_velocity);
		if (allConditions.length > 0 && !selectedIncidence) setSelectedIncidence(allConditions[0]);
		const filteredList = allConditions.filter(c => c.toLowerCase().includes(incidenceSearch.toLowerCase()));

		let chartData = [];
		if (selectedIncidence && data.advanced_metrics.incidence_velocity[selectedIncidence]) {
			const monthMap = { "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6, "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12 };
			const rawData = data.advanced_metrics.incidence_velocity[selectedIncidence];
			chartData = Object.keys(rawData).map(m => ({ month: m, value: rawData[m] })).sort((a, b) => monthMap[a.month] - monthMap[b.month]);
		}
		return { list: filteredList, chartData };
	}, [data, selectedIncidence, incidenceSearch]);

	// Advanced 2: Recurrence Gap
	const recurrenceGapData = useMemo(() => {
		let list = transformList(data.advanced_metrics.average_condition_recurrence_gap);
		if (!list || list.length === 0) return [];
		if (recurrenceSort === "desc") list.sort((a, b) => b.value - a.value);
		else list.sort((a, b) => a.value - b.value);
		return list.slice(0, 5);
	}, [data, recurrenceSort]);

	// Advanced 3: Comorbidity Pairs
	const comorbidityPairs = useMemo(() => {
		const raw = data.advanced_metrics.commordity_cooccurence || [];
		return raw.slice(0, 15).map(item => ({
			name: `${item[0].substring(0, 8)}.. + ${item[1].substring(0, 8)}..`,
			fullName: `${item[0]} + ${item[1]}`,
			value: item[2]
		})).sort((a, b) => b.value - a.value);
	}, [data]);

	// Advanced 4: Network Graph (Randomized)
	const networkGraph = useMemo(() => {
		const raw = data.advanced_metrics.disease_transition_patterns || [];
		const topTransitions = raw.slice(0, Math.max(5, Math.min(graphLimit, 200)));

		const uniqueNodes = Array.from(new Set(topTransitions.flatMap(t => [t[0], t[1]])));

		const width = 1200;
		const height = 600;
		const padding = 50;

		const nodes = uniqueNodes.map((node, i) => {
			const hashX = (i * 137 + seed * 997) % 1000 / 1000;
			const hashY = (i * 263 + seed * 881) % 1000 / 1000;
			return {
				id: node,
				x: padding + hashX * (width - 2 * padding),
				y: padding + hashY * (height - 2 * padding),
				truncated: truncateLabel(node, 6)
			};
		});

		const links = topTransitions.map((t, idx) => {
			const source = nodes.find(n => n.id === t[0]);
			const target = nodes.find(n => n.id === t[1]);
			return { id: `link-${idx}`, source, target, prob: t[3], count: t[2] };
		});

		return { nodes, links, width, height };
	}, [data, graphLimit, seed]);


	// --- KPI Configuration ---
	const kpiData = [
		{
			title: "Active Burden",
			value: data.kpis.current_active_burden || 0,
			prevWeek: data.kpis.historical_comparisons?.current_active_burden?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.current_active_burden?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.current_active_burden?.prevYear,
			icon: Activity, iconBg: "bg-rose-50", iconColor: "text-rose-600",
			infoText: "Current count of active untreated conditions across all patients."
		},
		{
			title: "Recovery Rate",
			value: data.kpis.global_recovery_rate || 0,
			prevWeek: data.kpis.historical_comparisons?.global_recovery_rate?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.global_recovery_rate?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.global_recovery_rate?.prevYear,
			icon: CheckCircle, iconBg: "bg-teal-50", iconColor: "text-teal-600", sentiment: "higher-is-better",
			infoText: "Percentage of diagnosed conditions that have been fully resolved."
		},
		{
			title: "Avg Complexity",
			value: data.kpis.patient_complexity_score || 0,
			prevWeek: data.kpis.historical_comparisons?.patient_complexity_score?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.patient_complexity_score?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.patient_complexity_score?.prevYear,
			icon: GitMerge, iconBg: "bg-purple-50", iconColor: "text-purple-600",
			infoText: "Average number of co-occurring conditions per patient."
		},
		{
			title: "Avg Time to Cure",
			value: data.kpis.average_time_to_cure || 0,
			prevWeek: data.kpis.historical_comparisons?.average_time_to_cure?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.average_time_to_cure?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.average_time_to_cure?.prevYear,
			icon: Clock, iconBg: "bg-amber-50", iconColor: "text-amber-600", sentiment: "lower-is-better",
			infoText: "Average days elapsed between condition diagnosis and resolution."
		},
		{
			title: "Admissions (30d)",
			value: data.kpis.admission_rate_last_30_days || 0,
			prevWeek: data.kpis.historical_comparisons?.admission_rate_last_30_days?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.admission_rate_last_30_days?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.admission_rate_last_30_days?.prevYear,
			icon: Hospital, iconBg: "bg-blue-50", iconColor: "text-blue-600",
			infoText: "Total number of hospital admissions related to conditions in the last 30 days."
		},
		{
			title: "Total Diagnoses",
			value: data.kpis.total_diagnoses || 0,
			prevWeek: data.kpis.historical_comparisons?.total_diagnoses?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.total_diagnoses?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.total_diagnoses?.prevYear,
			icon: FileText, iconBg: "bg-indigo-50", iconColor: "text-indigo-600",
			infoText: "Total count of clinical disorder diagnoses recorded."
		},
		{
			title: "Unique Conditions",
			value: data.kpis.unique_conditions || 0,
			prevWeek: data.kpis.historical_comparisons?.unique_conditions?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.unique_conditions?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.unique_conditions?.prevYear,
			icon: Fingerprint, iconBg: "bg-emerald-50", iconColor: "text-emerald-600",
			infoText: "Count of entirely distinct medical conditions observed in the system."
		},
		{
			title: "Chronic Burden",
			value: data.kpis.chronic_condition_burden || 0,
			prevWeek: data.kpis.historical_comparisons?.chronic_condition_burden?.prevWeek,
			prevMonth: data.kpis.historical_comparisons?.chronic_condition_burden?.prevMonth,
			prevYear: data.kpis.historical_comparisons?.chronic_condition_burden?.prevYear,
			icon: RefreshCcw, iconBg: "bg-orange-50", iconColor: "text-orange-600",
			infoText: "Count of currently active, ongoing conditions that have persisted for more than 90 days."
		},
	];

	if (loading) {
		return <LoadingScreen message="Loading Pathology Records..." subtext="Please wait while we gather the information." />;
	}

	return (
		<div className="animate-fade-in w-full">
			<header className="bg-white/80 backdrop-blur-md border-b border-slate-200 py-6 px-4 md:px-6 lg:px-8 sticky top-0 z-20 w-full">
				<div className="max-w-[1600px] mx-auto w-full">
					<div className="flex items-center gap-3 mb-2">
						<div className="p-2 bg-teal-50 rounded-lg">
							<Stethoscope size={24} className="text-teal-600" />
						</div>
						<h1 className="text-3xl font-black text-slate-900 tracking-tight">Conditions Overview</h1>
					</div>
					<p className="text-slate-500 font-medium ml-12">Key statistics and trends for patient pathologies.</p>
				</div>
			</header>

			<div className="max-w-[1600px] mx-auto w-full px-4 md:px-6 lg:px-8 py-8 space-y-10 pb-10">

				<KPICard kpis={kpiData} />

				{/* SECTION 1: STANDARD METRICS (6 Metrics, 2-Column Grid) */}
				<div className="grid grid-cols-1 md:grid-cols-2 gap-8">
					{/* Row 1 */}
					<MetricsCard title="Top 10 Active Disorders" metrics={[]} chartData={topDisorders} chartType="bar">
						<ResponsiveContainer width="100%" height="100%" minHeight={450}>
							<BarChart layout="vertical" data={topDisorders} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
								<CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
								<XAxis type="number" hide />
								<YAxis dataKey="name" type="category" width={180} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
								<Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '12px', border: 'none' }} />
								<Bar dataKey="value" fill="#14b8a6" radius={[0, 4, 4, 0]} barSize={18} />
							</BarChart>
						</ResponsiveContainer>
					</MetricsCard>

					<MetricsCard title="Top 10 Recurring" metrics={[]} chartData={recurring} chartType="bar">
						<ResponsiveContainer width="100%" height="100%" minHeight={450}>
							<BarChart layout="vertical" data={recurring} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
								<CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
								<XAxis type="number" hide />
								<YAxis dataKey="name" type="category" width={180} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
								<Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '12px', border: 'none' }} />
								<Bar dataKey="value" fill="#f43f5e" radius={[0, 4, 4, 0]} barSize={18} />
							</BarChart>
						</ResponsiveContainer>
					</MetricsCard>

					{/* Row 2 */}
					<MetricsCard title="Clinical Gravity (Severity)" metrics={[]} chartData={clinicalGravity} chartType="bar">
						<ResponsiveContainer width="100%" height="100%" minHeight={450}>
							<BarChart layout="vertical" data={clinicalGravity} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
								<CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
								<XAxis type="number" hide />
								<YAxis dataKey="name" type="category" width={180} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
								<Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '12px', border: 'none' }} />
								<Bar dataKey="value" fill="#8b5cf6" radius={[0, 4, 4, 0]} barSize={18} />
							</BarChart>
						</ResponsiveContainer>
					</MetricsCard>

					<MetricsCard title="Treatment Efficiency (Avg Cure Time)" metrics={[]} chartData={resolutionEfficiency} chartType="bar">
						<ResponsiveContainer width="100%" height="100%" minHeight={450}>
							<BarChart layout="vertical" data={resolutionEfficiency} margin={{ left: 10, right: 30, top: 10, bottom: 10 }}>
								<CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} stroke="#f1f5f9" />
								<XAxis type="number" hide />
								<YAxis dataKey="name" type="category" width={180} tick={{ fontSize: 10, fontWeight: 600, fill: '#64748b' }} tickLine={false} axisLine={false} interval={0} />
								<Tooltip cursor={{ fill: 'transparent' }} contentStyle={{ borderRadius: '12px', border: 'none' }} />
								<Bar dataKey="value" fill="#d97706" radius={[0, 4, 4, 0]} barSize={18} name="Avg Days to Cure" />
							</BarChart>
						</ResponsiveContainer>
					</MetricsCard>

					{/* Row 3 */}
					<MetricsCard title="Clinical Course" metrics={[{ label: "Total", value: chronicVsAcute.reduce((a, c) => a + c.value, 0) }]} chartData={chronicVsAcute} chartType="pie">
						<ResponsiveContainer width="100%" height="100%" minHeight={300}>
							<PieChart>
								<Pie data={chronicVsAcute} cx="50%" cy="50%" innerRadius={70} outerRadius={90} paddingAngle={5} dataKey="value">
									{chronicVsAcute.map((entry, index) => <Cell key={`cell-${index}`} fill={PIE_COLORS[index % 5]} />)}
								</Pie>
								<Legend verticalAlign="bottom" height={36} iconType="circle" />
								<Tooltip contentStyle={{ borderRadius: '12px', border: 'none' }} />
							</PieChart>
						</ResponsiveContainer>
					</MetricsCard>

					<MetricsCard title="Comorbidity Distribution" metrics={[]} chartData={comorbidityDistribution} chartType="bar">
						<ResponsiveContainer width="100%" height="100%" minHeight={300}>
							<BarChart data={comorbidityDistribution}>
								<CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
								<XAxis dataKey="name" tick={{ fontSize: 10, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
								<YAxis tick={{ fontSize: 10, fill: '#94a3b8' }} axisLine={false} tickLine={false} />
								<Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px', border: 'none' }} />
								<Bar dataKey="value" fill="#6366f1" radius={[4, 4, 0, 0]} barSize={40} />
							</BarChart>
						</ResponsiveContainer>
					</MetricsCard>
				</div>


				{/* SECTION 2: ADVANCED ANALYSIS */}
				<div className="pt-10 border-t border-slate-200">
					<h2 className="text-2xl font-black text-slate-800 tracking-tight mb-8 flex items-center gap-3">
						<BarChart3 className="text-teal-600" /> Advanced Analysis
					</h2>

					{/* Row 1: Incidence, Recurrence */}
					<div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
						<div className="bg-white rounded-2xl p-6 shadow-xl shadow-slate-200/50 border border-slate-100">
							<div className="flex items-center justify-between mb-6">
								<h3 className="font-bold text-slate-800">Incidence Velocity</h3>
								<div className="relative">
									<Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
									<input
										type="text"
										placeholder="Search..."
										value={incidenceSearch}
										onChange={(e) => setIncidenceSearch(e.target.value)}
										className="pl-9 pr-4 py-2 bg-slate-50 border border-slate-200 rounded-lg text-sm w-48"
									/>
								</div>
							</div>
							<div className="grid grid-cols-3 gap-6">
								<div className="col-span-1 border-r border-slate-100 pr-4 max-h-[300px] overflow-y-auto custom-scrollbar">
									<div className="space-y-1">
										{incidenceData.list.map(cond => (
											<button
												key={cond}
												onClick={() => setSelectedIncidence(cond)}
												className={`w-full text-left px-3 py-2 text-xs font-bold rounded-lg ${selectedIncidence === cond ? 'bg-teal-500 text-white' : 'hover:bg-slate-100'}`}
											>
												<span className="truncate">{cond}</span>
											</button>
										))}
									</div>
								</div>
								<div className="col-span-2 h-[300px]">
									{selectedIncidence && (
										<ResponsiveContainer width="100%" height="100%">
											<LineChart data={incidenceData.chartData}>
												<CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
												<XAxis dataKey="month" tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
												<YAxis tick={{ fontSize: 12 }} axisLine={false} tickLine={false} />
												<Tooltip contentStyle={{ borderRadius: '12px', border: 'none' }} />
												<Line type="monotone" dataKey="value" stroke="#14b8a6" strokeWidth={3} dot={{ r: 4, fill: "#14b8a6" }} />
											</LineChart>
										</ResponsiveContainer>
									)}
								</div>
							</div>
						</div>

						<AdvancedChartCard title="Recurrence Gap" subtitle="Relapse Interval" icon={Clock}>
							<div className="h-[300px] w-full">
								<ResponsiveContainer width="100%" height="100%">
									<BarChart data={recurrenceGapData} layout="vertical" margin={{ left: 40 }}>
										<CartesianGrid strokeDasharray="3 3" horizontal={true} vertical={false} />
										<XAxis type="number" hide />
										<YAxis dataKey="name" type="category" width={90} tick={{ fontSize: 10 }} axisLine={false} />
										<Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px' }} />
										<Bar dataKey="value" fill="#8b5cf6" radius={[0, 4, 4, 0]} barSize={20} />
									</BarChart>
								</ResponsiveContainer>
							</div>
						</AdvancedChartCard>
					</div>

					{/* Row 2: Age Burden, Comorbidity Pairs */}
					<div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
						<MetricsCard title="Age-Based Disease Burden" metrics={[]} chartData={ageBurden} chartType="bar">
							<ResponsiveContainer width="100%" height="100%" minHeight={300}>
								<BarChart data={ageBurden}>
									<CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
									<XAxis dataKey="name" tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
									<YAxis tick={{ fontSize: 10 }} axisLine={false} tickLine={false} />
									<Tooltip cursor={{ fill: '#f8fafc' }} contentStyle={{ borderRadius: '12px' }} />
									<Bar dataKey="value" fill="#f43f5e" radius={[4, 4, 0, 0]} barSize={40} />
								</BarChart>
							</ResponsiveContainer>
						</MetricsCard>

						<div className="bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100 overflow-hidden">
							<div className="flex items-center gap-3 mb-6">
								<Activity size={20} className="text-rose-600" />
								<h3 className="font-bold text-slate-800">Top Co-morbidities</h3>
							</div>
							<div className="h-[300px] w-full overflow-y-auto custom-scrollbar pr-2">
								{comorbidityPairs.map((pair, idx) => (
									<div key={idx} className="mb-4">
										<div className="flex justify-between text-xs mb-1">
											<span className="font-bold text-slate-700">{pair.name}</span>
											<span className="text-slate-500">{pair.value}</span>
										</div>
										<div className="w-full bg-slate-100 rounded-full h-2">
											<div className="bg-rose-500 h-2 rounded-full" style={{ width: `${(pair.value / comorbidityPairs[0].value) * 100}%` }}></div>
										</div>
									</div>
								))}
							</div>
						</div>
					</div>

					{/* Row 3: Full Width Interactive Graph */}
					<div className="w-full bg-white p-6 rounded-2xl shadow-xl shadow-slate-200/50 border border-slate-100 relative overflow-hidden">
						<div className="flex items-center justify-between mb-6 z-10 relative">
							<div className="flex items-center gap-3">
								<GitMerge size={20} className="text-amber-600" />
								<div>
									<h3 className="font-bold text-slate-800">Disease Pathways</h3>
									<p className="text-xs text-slate-500">Transition Network</p>
								</div>
							</div>
							<div className="flex items-center gap-2">
								<Users size={14} onClick={() => setSeed(s => s + 1)} className="cursor-pointer hover:text-amber-600" />
								<input
									type="number"
									min="5" max="200"
									value={graphLimit}
									onChange={(e) => setGraphLimit(Number(e.target.value))}
									className="pl-2 w-16 py-1 text-xs border border-slate-200 rounded-lg"
								/>
							</div>
						</div>

						<div className="w-full h-[600px] flex items-center justify-center overflow-auto">
							<svg width={networkGraph.width} height={networkGraph.height} viewBox={`0 0 ${networkGraph.width} ${networkGraph.height}`} className="overflow-visible">
								<defs>
									<marker id="arrowhead" markerWidth="10" markerHeight="7" refX="28" refY="3.5" orient="auto">
										<polygon points="0 0, 10 3.5, 0 7" fill="#94a3b8" />
									</marker>
								</defs>
								{networkGraph.links.map((link, i) => {
									const isHovered = hoveredLink === link.id;
									const opacity = hoveredNode ? (link.source.id === hoveredNode || link.target.id === hoveredNode ? 1 : 0.05) : (isHovered ? 1 : 0.3);
									const mx = (link.source.x + link.target.x) / 2;
									const my = (link.source.y + link.target.y) / 2;
									const d = `M ${link.source.x} ${link.source.y} L ${link.target.x} ${link.target.y}`;

									return (
										<g key={i} onMouseEnter={() => setHoveredLink(link.id)} onMouseLeave={() => setHoveredLink(null)} style={{ opacity }} className="transition-opacity duration-300">
											<path d={d} fill="none" stroke={link.prob > 0.5 ? "#f59e0b" : "#cbd5e1"} strokeWidth={isHovered ? 3 : 1} markerEnd="url(#arrowhead)" />
											{isHovered && (
												<g>
													<rect x={mx - 60} y={my - 20} width="120" height="40" rx="4" fill="rgba(0,0,0,0.85)" />
													<text x={mx} y={my} fill="white" fontSize="10" textAnchor="middle" dy="-4">Prob: {(link.prob * 100).toFixed(0)}%</text>
													<text x={mx} y={my} fill="#ccc" fontSize="9" textAnchor="middle" dy="8">Count: {link.count}</text>
												</g>
											)}
										</g>
									);
								})}
								{networkGraph.nodes.map((node, i) => {
									const isHovered = hoveredNode === node.id;
									return (
										<g key={i} onMouseEnter={() => setHoveredNode(node.id)} onMouseLeave={() => setHoveredNode(null)}
											style={{ opacity: hoveredNode && hoveredNode !== node.id && !networkGraph.links.some(l => (l.source.id === node.id && l.target.id === hoveredNode) || (l.target.id === node.id && l.source.id === hoveredNode)) ? 0.2 : 1 }}
											className="transition-opacity duration-300"
										>
											<circle cx={node.x} cy={node.y} r={isHovered ? 20 : 12} fill="white" stroke="#f59e0b" strokeWidth={2} />
											{isHovered && <text x={node.x} y={node.y - 30} textAnchor="middle" className="text-xs font-bold fill-slate-800 bg-white">{node.id}</text>}
											{!isHovered && <text x={node.x} y={node.y} dy="4" textAnchor="middle" className="text-[9px] font-bold fill-slate-500 pointer-events-none">{node.truncated}</text>}
										</g>
									)
								})}
							</svg>
						</div>
					</div>
				</div>
			</div>
		</div>
	);
};

export default ConditionsDashboard;
