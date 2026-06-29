import React, { useState, useEffect, useMemo } from "react";
import { allergyDashboard } from "@/api/api";
import {
	Activity, AlertTriangle, BarChart3, Clock as ClockIcon, 
	Database, GitMerge, Heart, ShieldAlert,
	Stethoscope, Users, Info, Calendar
} from "lucide-react";
import KPICard from "@/components/KPICard";
import MetricsCard from "@/components/MetricsCard";
import AdvancedChartCard from "@/components/AdvancedChartCard";
import LoadingScreen from "@/components/LoadingScreen";
import ReactECharts from 'echarts-for-react';

const PIE_COLORS = ["#14b8a6", "#8b5cf6", "#f43f5e", "#f59e0b", "#3b82f6"];

const AllergiesDashboard = () => {
	const [data, setData] = useState({ kpis: {}, metrics: {}, advanced_metrics: {} });
	const [loading, setLoading] = useState(true);

	useEffect(() => {
		const fetchData = async () => {
			try {
				const result = await allergyDashboard();
				if (result?.allergies_dashboard) {
					setData({
						kpis: result.allergies_dashboard.kpis || {},
						metrics: result.allergies_dashboard.metrics || {},
						advanced_metrics: result.allergies_dashboard.advanced_metrics || {}
					});
				}
			} catch (err) {
				console.error(err);
			} finally {
				setLoading(false);
			}
		};
		fetchData();
	}, []);

	// --- Helper to safely access historical trend values ---
	const getTrendVal = (kpiName, period = "last_month") => {
		if (!data.kpis?.historical_data?.[period]) return 0;
		return data.kpis.historical_data[period][kpiName] || 0;
	};

	// --- Construct KPI Arrays for the KPICard Component ---
	const coreKpis = useMemo(() => {
		return [
			{
				title: "Total Allergic Population",
				value: data.kpis?.total_allergic_population || 0,
				prevWeek: getTrendVal("total_allergic_population", "last_week"),
				prevMonth: getTrendVal("total_allergic_population", "last_month"),
				prevYear: getTrendVal("total_allergic_population", "last_year"),
				icon: Users,
				iconBg: "bg-teal-50",
				iconColor: "text-teal-600",
				infoText: "Total number of patients with at least one recorded active or inactive allergy."
			},
			{
				title: "Active Allergy Rate",
				value: data.kpis?.active_allergy_percentage || 0,
				prevWeek: getTrendVal("active_allergy_percentage", "last_week"),
				prevMonth: getTrendVal("active_allergy_percentage", "last_month"),
				prevYear: getTrendVal("active_allergy_percentage", "last_year"),
				icon: Activity,
				iconBg: "bg-purple-50",
				iconColor: "text-purple-600",
				infoText: "Percentage of diagnosed allergies that remain active (i.e. uncured/resolved)."
			},
			{
				title: "Severe Incident Rate",
				value: data.kpis?.severe_allergy_incidence_rate || 0,
				prevWeek: getTrendVal("severe_allergy_incidence_rate", "last_week"),
				prevMonth: getTrendVal("severe_allergy_incidence_rate", "last_month"),
				prevYear: getTrendVal("severe_allergy_incidence_rate", "last_year"),
				icon: AlertTriangle,
				iconBg: "bg-rose-50",
				iconColor: "text-rose-600",
				infoText: "Percentage of reactions categorized as clinically SEVERE at primary detection."
			},
			{
				title: "Allergic Patient Prevalence",
				value: data.kpis?.allergic_patient_rate || 0,
				prevWeek: getTrendVal("allergic_patient_rate", "last_week"),
				prevMonth: getTrendVal("allergic_patient_rate", "last_month"),
				prevYear: getTrendVal("allergic_patient_rate", "last_year"),
				icon: Database,
				iconBg: "bg-blue-50",
				iconColor: "text-blue-600",
				infoText: "Prevalence of allergic hypersensitivities across the entire registered patient roster."
			}
		];
	}, [data]);

	const clinicalKpis = useMemo(() => {
		return [
			{
				title: "Penicillin De-labeling Eligibility",
				value: data.kpis?.penicillin_allergy_delabeling_eligibility_rate || 0,
				prevWeek: getTrendVal("penicillin_allergy_delabeling_eligibility_rate", "last_week"),
				prevMonth: getTrendVal("penicillin_allergy_delabeling_eligibility_rate", "last_month"),
				prevYear: getTrendVal("penicillin_allergy_delabeling_eligibility_rate", "last_year"),
				icon: GitMerge,
				iconBg: "bg-amber-50",
				iconColor: "text-amber-600",
				infoText: "Percentage of penicillin-allergic patients who are eligible for de-labeling protocols due to non-severe historical symptoms."
			},
			{
				title: "30-Day Allergy Readmissions",
				value: data.kpis?.allergy_related_readmission_rate || 0,
				prevWeek: getTrendVal("allergy_related_readmission_rate", "last_week"),
				prevMonth: getTrendVal("allergy_related_readmission_rate", "last_month"),
				prevYear: getTrendVal("allergy_related_readmission_rate", "last_year"),
				icon: ClockIcon,
				iconBg: "bg-red-50",
				iconColor: "text-red-600",
				infoText: "Rate of 30-day clinical readmissions among patients with diagnosed hypersensitivities."
			},
			{
				title: "Poly-Allergen Burden Rate",
				value: data.kpis?.poly_allergen_patient_rate || 0,
				prevWeek: getTrendVal("poly_allergen_patient_rate", "last_week"),
				prevMonth: getTrendVal("poly_allergen_patient_rate", "last_month"),
				prevYear: getTrendVal("poly_allergen_patient_rate", "last_year"),
				icon: BarChart3,
				iconBg: "bg-slate-100",
				iconColor: "text-slate-600",
				infoText: "Percentage of the allergic population with co-diagnosed sensitivities to 2 or more distinct substances."
			},
			{
				title: "Drug Hypersensitivity Rate",
				value: data.kpis?.drug_hypersensitivity_rate || 0,
				prevWeek: getTrendVal("drug_hypersensitivity_rate", "last_week"),
				prevMonth: getTrendVal("drug_hypersensitivity_rate", "last_month"),
				prevYear: getTrendVal("drug_hypersensitivity_rate", "last_year"),
				icon: Stethoscope,
				iconBg: "bg-indigo-50",
				iconColor: "text-indigo-600",
				infoText: "Prevalence of medication-induced hypersensitivities as a proportion of total allergy records."
			}
		];
	}, [data]);

	// --- Data Processing for ECharts ---
	const topCausativeAgentsOption = useMemo(() => {
		const raw = Array.isArray(data.metrics?.top_10_causative_agents) ? data.metrics.top_10_causative_agents : [];
		const sorted = [...raw].sort((a, b) => (b.count || 0) - (a.count || 0));
		return {
			tooltip: {
				trigger: 'axis',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				shadowColor: 'rgba(0, 0, 0, 0.05)',
				shadowBlur: 10,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
			},
			grid: { left: '3%', right: '8%', bottom: '3%', top: '3%', containLabel: true },
			xAxis: { type: 'value', show: false },
			yAxis: {
				type: 'category',
				data: sorted.map(item => item?.agent || "Unknown").reverse(),
				axisLine: { show: false },
				axisTick: { show: false },
				axisLabel: { color: '#64748b', fontSize: 10, fontWeight: 'bold' }
			},
			series: [{
				name: 'Cases',
				type: 'bar',
				barWidth: 12,
				data: sorted.map(item => item?.count || 0).reverse(),
				itemStyle: {
					color: {
						type: 'linear',
						x: 0, y: 0, x2: 1, y2: 0,
						colorStops: [{ offset: 0, color: '#0d9488' }, { offset: 1, color: '#14b8a6' }]
					},
					borderRadius: [0, 6, 6, 0]
				}
			}]
		};
	}, [data]);

	const severityDistributionOption = useMemo(() => {
		const raw = Array.isArray(data.metrics?.severity_distribution) ? data.metrics.severity_distribution : [];
		const severityMap = { "MILD": "Mild", "MODERATE": "Moderate", "SEVERE": "Severe", "N/A": "Not Specified" };
		const formattedData = raw.map(item => ({
			name: severityMap[item?.severity] || item?.severity || "Unknown",
			value: item?.count || 0
		}));

		return {
			tooltip: {
				trigger: 'item',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif' }
			},
			legend: { orient: 'vertical', left: 'left', icon: 'circle', textStyle: { color: '#64748b', fontSize: 11 } },
			series: [{
				name: 'Severity',
				type: 'pie',
				radius: ['45%', '70%'],
				avoidLabelOverlap: false,
				itemStyle: { borderRadius: 8, borderColor: '#fff', borderWidth: 2 },
				label: { show: false },
				emphasis: { label: { show: true, fontSize: 12, fontWeight: 'bold' } },
				data: formattedData,
				color: ['#14b8a6', '#f59e0b', '#f43f5e', '#64748b']
			}]
		};
	}, [data]);

	const categoryDistributionOption = useMemo(() => {
		const raw = Array.isArray(data.metrics?.allergy_distribution_by_category) ? data.metrics.allergy_distribution_by_category : [];
		const formattedData = raw.map(item => ({
			name: item?.category ? (item.category.charAt(0).toUpperCase() + item.category.slice(1)) : "Unknown",
			value: item?.count || 0
		}));

		return {
			tooltip: {
				trigger: 'item',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif' }
			},
			legend: { orient: 'vertical', left: 'left', icon: 'circle', textStyle: { color: '#64748b', fontSize: 11 } },
			series: [{
				name: 'Allergen Category',
				type: 'pie',
				radius: ['0%', '70%'],
				itemStyle: { borderRadius: 6, borderColor: '#fff', borderWidth: 1 },
				label: { show: false },
				data: formattedData,
				color: PIE_COLORS
			}]
		};
	}, [data]);

	const discoveryTrendsOption = useMemo(() => {
		const raw = Array.isArray(data.metrics?.allergy_discovery_trends) ? data.metrics.allergy_discovery_trends : [];
		
		// Group trends by year and category
		const years = [...new Set(raw.map(item => item?.year).filter(Boolean))].sort();
		const categories = [...new Set(raw.map(item => item?.category).filter(Boolean))];

		const series = categories.map((cat, idx) => {
			const catData = years.map(y => {
				const match = raw.find(item => item?.year === y && item?.category === cat);
				return match ? (match.count || 0) : 0;
			});
			return {
				name: cat ? (cat.charAt(0).toUpperCase() + cat.slice(1)) : "Unknown",
				type: 'line',
				smooth: true,
				showSymbol: false,
				data: catData,
				lineStyle: { width: 3 }
			};
		});

		return {
			tooltip: {
				trigger: 'axis',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
			},
			legend: { bottom: 0, icon: 'circle', textStyle: { color: '#64748b' } },
			grid: { left: '3%', right: '4%', bottom: '12%', top: '8%', containLabel: true },
			xAxis: {
				type: 'category',
				boundaryGap: false,
				data: years,
				axisLine: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 }
			},
			yAxis: {
				type: 'value',
				axisLine: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 },
				splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
			},
			series: series,
			color: PIE_COLORS
		};
	}, [data]);

	// --- Advanced Metrics Options ---
	const ageOnsetOption = useMemo(() => {
		const raw = Array.isArray(data.advanced_metrics?.age_at_onset_distribution) ? data.advanced_metrics.age_at_onset_distribution : [];
		return {
			tooltip: {
				trigger: 'axis',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif' }
			},
			grid: { left: '3%', right: '4%', bottom: '5%', top: '5%', containLabel: true },
			xAxis: {
				type: 'category',
				data: raw.map(item => `${item?.age_group || "Unknown"} yrs`),
				axisLine: { show: false },
				axisTick: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 }
			},
			yAxis: {
				type: 'value',
				axisLine: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 },
				splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
			},
			series: [{
				name: 'Onset Count',
				type: 'bar',
				barWidth: 20,
				data: raw.map(item => item?.count || 0),
				itemStyle: {
					color: {
						type: 'linear',
						x: 0, y: 0, x2: 0, y2: 1,
						colorStops: [{ offset: 0, color: '#8b5cf6' }, { offset: 1, color: '#a78bfa' }]
					},
					borderRadius: [4, 4, 0, 0]
				}
			}]
		};
	}, [data]);

	const pollenSeasonalityOption = useMemo(() => {
		const raw = Array.isArray(data.advanced_metrics?.pollen_seasonality_acceleration) ? data.advanced_metrics.pollen_seasonality_acceleration : [];
		return {
			tooltip: {
				trigger: 'axis',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif' }
			},
			legend: { bottom: 0, textStyle: { color: '#64748b' } },
			grid: { left: '3%', right: '4%', bottom: '15%', top: '10%', containLabel: true },
			xAxis: {
				type: 'category',
				data: raw.map(item => item?.month || "Unknown"),
				axisLine: { show: false },
				axisTick: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 }
			},
			yAxis: [
				{
					type: 'value',
					name: 'New Detections',
					nameTextStyle: { color: '#94a3b8', fontSize: 10 },
					axisLine: { show: false },
					axisLabel: { color: '#94a3b8', fontSize: 10 },
					splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
				},
				{
					type: 'value',
					name: 'MoM Growth (%)',
					nameTextStyle: { color: '#94a3b8', fontSize: 10 },
					axisLine: { show: false },
					axisLabel: { color: '#94a3b8', fontSize: 10, formatter: '{value}%' },
					splitLine: { show: false }
				}
			],
			series: [
				{
					name: 'Detections',
					type: 'bar',
					barWidth: 16,
					data: raw.map(item => item?.count || 0),
					itemStyle: { color: '#3b82f6', borderRadius: [4, 4, 0, 0] }
				},
				{
					name: 'Growth Rate',
					type: 'line',
					yAxisIndex: 1,
					smooth: true,
					data: raw.map(item => item?.mom_growth_rate || 0),
					lineStyle: { width: 3 },
					itemStyle: { color: '#f43f5e' }
				}
			]
		};
	}, [data]);

	const natureSeverityOption = useMemo(() => {
		const raw = Array.isArray(data.advanced_metrics?.allergen_nature_severity_matrix) ? data.advanced_metrics.allergen_nature_severity_matrix : [];
		const natures = [...new Set(raw.map(item => item?.allergen_nature).filter(Boolean))];
		const severities = ["MILD", "MODERATE", "SEVERE", "N/A"];
		const severityMap = { "MILD": "Mild", "MODERATE": "Moderate", "SEVERE": "Severe", "N/A": "Not Specified" };

		const series = severities.map((sev) => {
			const sevData = natures.map(nat => {
				const match = raw.find(item => item?.allergen_nature === nat && item?.severity === sev);
				return match ? (match.count || 0) : 0;
			});
			return {
				name: severityMap[sev],
				type: 'bar',
				stack: 'total',
				data: sevData,
				barWidth: 20
			};
		});

		return {
			tooltip: {
				trigger: 'axis',
				backgroundColor: 'rgba(255, 255, 255, 0.95)',
				borderRadius: 12,
				borderWidth: 0,
				textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif' }
			},
			legend: { bottom: 0, icon: 'circle', textStyle: { color: '#64748b' } },
			grid: { left: '3%', right: '4%', bottom: '15%', top: '8%', containLabel: true },
			xAxis: {
				type: 'category',
				data: natures.map(nat => nat ? (nat.charAt(0).toUpperCase() + nat.slice(1)) : "Unknown"),
				axisLine: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 }
			},
			yAxis: {
				type: 'value',
				axisLine: { show: false },
				axisLabel: { color: '#94a3b8', fontSize: 10 },
				splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
			},
			series: series,
			color: ['#14b8a6', '#f59e0b', '#f43f5e', '#64748b']
		};
	}, [data]);

	if (loading) {
		return <LoadingScreen message="Reconstructing Allergy Analytics Infrastructure..." />;
	}

	return (
		<div className="flex-1 overflow-y-auto bg-slate-50/50 p-6 space-y-6">
			{/* KPI Section 1: Simple Metrics */}
			<div className="space-y-3">
				<KPICard kpis={coreKpis} />
			</div>

			{/* KPI Section 2: Complex / Clinical Metrics */}
			<div className="space-y-3">
				<KPICard kpis={clinicalKpis} />
			</div>

			{/* Standard Metrics Row */}
			<div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
				<MetricsCard
					title="Top 10 Causative Allergenic Agents"
					metrics={[]}
					infoText="Leading substances and agents driving allergy records."
				>
					<div className="h-80">
						<ReactECharts option={topCausativeAgentsOption} style={{ height: '100%' }} />
					</div>
				</MetricsCard>

				<MetricsCard
					title="Severity & Category Profiles"
					metrics={[]}
					infoText="Hypersensitivity clinical severity and allergen source types."
				>
					<div className="flex flex-col gap-6 h-80 overflow-y-auto pr-1">
						<div className="flex-1 min-h-[140px] relative border-b border-slate-100 pb-2">
							<span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block mb-2">Reaction Severity</span>
							<ReactECharts option={severityDistributionOption} style={{ height: '120px' }} />
						</div>
						<div className="flex-1 min-h-[140px] relative">
							<span className="text-[10px] font-bold text-slate-400 uppercase tracking-wider block mb-2">Allergen Source Category</span>
							<ReactECharts option={categoryDistributionOption} style={{ height: '120px' }} />
						</div>
					</div>
				</MetricsCard>
			</div>

			{/* Discovery Trend Row */}
			<div className="grid grid-cols-1 gap-6">
				<MetricsCard
					title="Hypersensitivity Diagnosis Discovery Trends"
					metrics={[]}
					infoText="Longitudinal velocity of new allergy detections grouped by allergen category."
				>
					<div className="h-80">
						<ReactECharts option={discoveryTrendsOption} style={{ height: '100%' }} />
					</div>
				</MetricsCard>
			</div>

			{/* Advanced Analytics Grid Section */}
			<div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
				{/* Advanced 1: Cross Reactivity Matrix */}
				<AdvancedChartCard
					title="Cross-Reactivity & Co-occurrence Risk Index"
					description="Highly comorbid pairs of drug/substance allergies diagnosed in the same patient population."
					badgeText="Advanced Correlation"
				>
					<div className="h-80 overflow-y-auto pr-2">
						<table className="w-full text-left border-collapse">
							<thead>
								<tr className="border-b border-slate-100 text-[10px] font-bold text-slate-400 uppercase tracking-wider">
									<th className="pb-3 pl-2">Allergen A</th>
									<th className="pb-3">Allergen B</th>
									<th className="pb-3 text-right pr-2">Co-occurrences</th>
								</tr>
							</thead>
							<tbody className="divide-y divide-slate-100 text-xs font-semibold text-slate-600">
								{(data.advanced_metrics?.cross_reactivity_risk_matrix || []).slice(0, 10).map((row, idx) => (
									<tr key={idx} className="hover:bg-slate-50/50 transition-colors">
										<td className="py-2.5 pl-2 truncate max-w-[160px]" title={row?.allergy_A}>
											{row?.allergy_A}
										</td>
										<td className="py-2.5 truncate max-w-[160px]" title={row?.allergy_B}>
											{row?.allergy_B}
										</td>
										<td className="py-2.5 text-right pr-2 font-mono text-teal-600">
											{row?.co_occurrence_count}
										</td>
									</tr>
								))}
								{(!data.advanced_metrics?.cross_reactivity_risk_matrix || data.advanced_metrics.cross_reactivity_risk_matrix.length === 0) && (
									<tr>
										<td colSpan={3} className="py-8 text-center text-slate-400 font-medium">
											No co-occurring patterns detected.
										</td>
									</tr>
								)}
							</tbody>
						</table>
					</div>
				</AdvancedChartCard>

				{/* Advanced 2: Allergen Nature Severity Matrix */}
				<AdvancedChartCard
					title="Allergen Nature vs. Reaction Severity Profiling"
					description="Cross-tabulation showing severity level distribution across different allergen categories."
					badgeText="Stacked Profiling"
				>
					<div className="h-80">
						<ReactECharts option={natureSeverityOption} style={{ height: '100%' }} />
					</div>
				</AdvancedChartCard>

				{/* Advanced 3: Age at Onset Distribution */}
				<AdvancedChartCard
					title="Age-at-Onset Profile"
					description="Hypersensitivity onset counts distributed by age groups at the time of diagnosis."
					badgeText="Onset Demographics"
				>
					<div className="h-80">
						<ReactECharts option={ageOnsetOption} style={{ height: '100%' }} />
					</div>
				</AdvancedChartCard>

				{/* Advanced 4: Pollen Seasonality Acceleration */}
				<AdvancedChartCard
					title="Environmental Pollen Seasonality & Acceleration"
					description="Month-over-month rate of change in environmental/pollen allergy diagnosis counts."
					badgeText="Temporal Dynamics"
				>
					<div className="h-80">
						<ReactECharts option={pollenSeasonalityOption} style={{ height: '100%' }} />
					</div>
				</AdvancedChartCard>
			</div>
		</div>
	);
};

export default AllergiesDashboard;
