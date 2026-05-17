import React, { useState, useEffect, useMemo } from "react";
import { patientDashboard } from "@/api/api";
import { Users, Heart, DollarSign, TrendingUp, Users2, Activity, Globe, Scale, Clock as ClockIcon, GraduationCap } from "lucide-react";
import KPICard from "@/components/KPICard";
import MetricsCard from "@/components/MetricsCard";
import AdvancedChartCard from "@/components/AdvancedChartCard";
import LoadingScreen from "@/components/LoadingScreen";
import ReactECharts from 'echarts-for-react';

// --- Data Transformation Helpers for Advanced Metrics --- // 

// 1. Transform Survival Trend: [{"males": [...]}, {"females": [...]}] -> [{age: 5, male: 0.99, female: 0.99}, ...]
const transformSurvivalData = (raw) => {
  if (!raw || raw.length < 2) return [];
  const males = raw[0].males; // List of {age: val}
  const females = raw[1].females;

  // Merge into single array
  return males.map((mItem, idx) => {
    const age = Object.keys(mItem)[0];
    const mVal = mItem[age];
    const fVal = females[idx] ? females[idx][age] : 0;
    return {
      name: `${age}y`,
      Male: mVal * 100, // percentage for better viz
      Female: fVal * 100
    };
  });
};

// 2. Transform Demographic Entropy: [("City", Val, [...]), ...] -> [{name: "City", value: Val}]
const transformEntropyData = (raw) => {
  if (!raw) return [];
  return raw.slice(0, 10).map(item => ({ // Take top 10 cities
    name: item[0],
    value: item[1]
  }));
};

// 3. Transform Wealth Trajectory: [("0-5", Inc, Vel), ...] -> [{name: "0-5", Income: Inc, Velocity: Vel}]
const transformWealthData = (raw) => {
  if (!raw) return [];
  return raw.map(item => ({
    name: item[0],
    Income: item[1],
    Velocity: item[2]
  }));
};

// 4. Transform Mortality Hazard: { "White": [("Q0", [min, max], prob), ...], ... } -> [{name: "Q0", White: prob, Black: prob...}]
const transformMortalityData = (raw) => {
  if (!raw) return [];
  const races = Object.keys(raw);
  const quintiles = ["Q0", "Q1", "Q2", "Q3", "Q4", "Q5", "Q6", "Q7", "Q8", "Q9"];

  return quintiles.map(q => {
    const entry = { name: q };
    races.forEach(race => {
      const raceData = raw[race];
      if (Array.isArray(raceData)) {
        const qData = raceData.find(item => item[0] === q);
        entry[race] = qData ? qData[2] * 100 : 0; // percentage
      } else {
        entry[race] = 0;
      }
    });
    return entry;
  });
};

const PatientDashboard = () => {
  const [data, setData] = useState({ kpis: {}, metrics: {}, trends: {}, advanced: {} });
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const result = await patientDashboard();
        // Matching the Flask structure: { kpis: {}, metrics: {}, metric_trends: {}, advanced_metrics: {} }
        if (result) {
          setData({
            kpis: result.kpis || {},
            metrics: result.metrics || {},
            trends: result.metric_trends || {},
            advanced: result.advanced_metrics || {}
          });
        }
      } catch (err) {
        console.error("Error fetching dashboard data:", err);
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  // Memoize transformed data for charts
  const survivalData = useMemo(() => transformSurvivalData(data.advanced.actural_survival_trend), [data.advanced.actural_survival_trend]);
  const entropyData = useMemo(() => transformEntropyData(data.advanced.demographic_entropy), [data.advanced.demographic_entropy]);
  const wealthData = useMemo(() => transformWealthData(data.advanced.wealth_trajectory), [data.advanced.wealth_trajectory]);
  const mortalityData = useMemo(() => transformMortalityData(data.advanced.mortality_hazard_by_quintiles), [data.advanced.mortality_hazard_by_quintiles]);

  const survivalOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      borderRadius: 12,
      borderWidth: 0,
      shadowColor: 'rgba(0, 0, 0, 0.05)',
      shadowBlur: 10,
      textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
    },
    legend: {
      data: ['Male', 'Female'],
      icon: 'circle',
      bottom: 0,
      textStyle: { color: '#64748b', fontWeight: 'bold' }
    },
    grid: { left: '3%', right: '3%', bottom: '12%', top: '8%', containLabel: true },
    xAxis: {
      type: 'category',
      boundaryGap: false,
      data: survivalData.map(d => d.name),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10 }
    },
    yAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10, formatter: '{value}%' },
      splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
    },
    series: [
      {
        name: 'Male',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: survivalData.map(d => d.Male),
        itemStyle: { color: '#3b82f6' },
        lineStyle: { width: 3 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(59, 130, 246, 0.1)' },
              { offset: 1, color: 'rgba(59, 130, 246, 0)' }
            ]
          }
        }
      },
      {
        name: 'Female',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: survivalData.map(d => d.Female),
        itemStyle: { color: '#ec4899' },
        lineStyle: { width: 3 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(236, 72, 153, 0.1)' },
              { offset: 1, color: 'rgba(236, 72, 153, 0)' }
            ]
          }
        }
      }
    ]
  }), [survivalData]);

  const entropyOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      borderRadius: 12,
      borderWidth: 0,
      shadowColor: 'rgba(0, 0, 0, 0.05)',
      shadowBlur: 10,
      textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
    },
    grid: { left: '3%', right: '4%', bottom: '5%', top: '5%', containLabel: true },
    xAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10 },
      splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
    },
    yAxis: {
      type: 'category',
      data: entropyData.map(d => d.name),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#64748b', fontSize: 11, fontWeight: 'bold' }
    },
    series: [
      {
        name: 'Entropy Score',
        type: 'bar',
        barWidth: 16,
        data: entropyData.map(d => d.value),
        itemStyle: {
          borderRadius: [0, 4, 4, 0],
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 1, y2: 0,
            colorStops: [
              { offset: 0, color: '#8b5cf6' },
              { offset: 1, color: '#c084fc' }
            ]
          }
        }
      }
    ]
  }), [entropyData]);

  const wealthOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      borderRadius: 12,
      borderWidth: 0,
      shadowColor: 'rgba(0, 0, 0, 0.05)',
      shadowBlur: 10,
      textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
    },
    legend: {
      data: ['Income', 'Velocity'],
      bottom: 0,
      textStyle: { color: '#64748b', fontWeight: 'bold' }
    },
    grid: { left: '3%', right: '3%', bottom: '12%', top: '8%', containLabel: true },
    xAxis: {
      type: 'category',
      data: wealthData.map(d => d.name),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10 }
    },
    yAxis: [
      {
        type: 'value',
        name: 'Income',
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: '#14b8a6', fontSize: 10, formatter: '${value}' },
        splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
      },
      {
        type: 'value',
        name: 'Velocity',
        axisLine: { show: false },
        axisTick: { show: false },
        axisLabel: { color: '#f59e0b', fontSize: 10 },
        splitLine: { show: false }
      }
    ],
    series: [
      {
        name: 'Income',
        type: 'line',
        smooth: true,
        showSymbol: false,
        data: wealthData.map(d => d.Income),
        itemStyle: { color: '#14b8a6' },
        lineStyle: { width: 3 },
        areaStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: 'rgba(20, 184, 166, 0.15)' },
              { offset: 1, color: 'rgba(20, 184, 166, 0)' }
            ]
          }
        }
      },
      {
        name: 'Velocity',
        type: 'line',
        yAxisIndex: 1,
        smooth: true,
        data: wealthData.map(d => d.Velocity),
        itemStyle: { color: '#f59e0b' },
        lineStyle: { width: 3 },
        symbol: 'circle',
        symbolSize: 8
      }
    ]
  }), [wealthData]);

  const mortalityOption = useMemo(() => ({
    tooltip: {
      trigger: 'axis',
      backgroundColor: 'rgba(255, 255, 255, 0.95)',
      borderRadius: 12,
      borderWidth: 0,
      shadowColor: 'rgba(0, 0, 0, 0.05)',
      shadowBlur: 10,
      textStyle: { color: '#334155', fontFamily: 'Inter, sans-serif', fontSize: 11 }
    },
    legend: {
      data: ['White', 'Black', 'Asian', 'Native'],
      icon: 'circle',
      bottom: 0,
      textStyle: { color: '#64748b', fontWeight: 'bold' }
    },
    grid: { left: '3%', right: '3%', bottom: '12%', top: '8%', containLabel: true },
    xAxis: {
      type: 'category',
      data: mortalityData.map(d => d.name),
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10 }
    },
    yAxis: {
      type: 'value',
      axisLine: { show: false },
      axisTick: { show: false },
      axisLabel: { color: '#94a3b8', fontSize: 10, formatter: '{value}%' },
      splitLine: { lineStyle: { type: 'dashed', color: '#f1f5f9' } }
    },
    series: [
      {
        name: 'White',
        type: 'bar',
        stack: 'total',
        barWidth: 26,
        data: mortalityData.map(d => d.white),
        itemStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#94a3b8' },
              { offset: 1, color: '#cbd5e1' }
            ]
          }
        }
      },
      {
        name: 'Black',
        type: 'bar',
        stack: 'total',
        data: mortalityData.map(d => d.black),
        itemStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#0d9488' },
              { offset: 1, color: '#2dd4bf' }
            ]
          }
        }
      },
      {
        name: 'Asian',
        type: 'bar',
        stack: 'total',
        data: mortalityData.map(d => d.asian),
        itemStyle: {
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#0284c7' },
              { offset: 1, color: '#38bdf8' }
            ]
          }
        }
      },
      {
        name: 'Native',
        type: 'bar',
        stack: 'total',
        data: mortalityData.map(d => d.native),
        itemStyle: {
          borderRadius: [4, 4, 0, 0],
          color: {
            type: 'linear',
            x: 0, y: 0, x2: 0, y2: 1,
            colorStops: [
              { offset: 0, color: '#6366f1' },
              { offset: 1, color: '#818cf8' }
            ]
          }
        }
      }
    ]
  }), [mortalityData]);

  // 1. Map KPI Data (Injecting prevValues from new historical_data structure)
  // historical_data is grouped by period: { last_week: { total_patients: X, ... }, last_month: {...}, last_year: {...} }
  const hist = data.kpis.historical_data || {};
  const kpiData = [
    {
      title: "Total Patients",
      value: data.kpis.total_patients || 0,
      prevWeek: hist.last_week?.total_patients,
      prevMonth: hist.last_month?.total_patients,
      prevYear: hist.last_year?.total_patients,
      icon: Users,
      iconBg: "bg-blue-50",
      iconColor: "text-blue-600",
      infoText: "Total number of registered patients in the system."
    },
    {
      title: "Active Rate",
      value: data.kpis.active_patient_rate || 0,
      prevWeek: hist.last_week?.active_patient_rate,
      prevMonth: hist.last_month?.active_patient_rate,
      prevYear: hist.last_year?.active_patient_rate,
      sentiment: "higher-is-better",
      icon: Heart,
      iconBg: "bg-rose-50",
      iconColor: "text-rose-600",
      infoText: "Percentage of patients currently alive relative to total registered."
    },
    {
      title: "Gender Balance",
      value: data.kpis.gender_balance_ratio || 0,
      prevWeek: hist.last_week?.gender_balance,
      prevMonth: hist.last_month?.gender_balance,
      prevYear: hist.last_year?.gender_balance,
      icon: Users2,
      iconBg: "bg-purple-50",
      iconColor: "text-purple-600",
      infoText: "Ratio of male to female patients."
    },
    {
      title: "Mean Income",
      value: data.kpis.mean_family_income || 0,
      prevWeek: hist.last_week?.mean_income,
      prevMonth: hist.last_month?.mean_income,
      prevYear: hist.last_year?.mean_income,
      icon: DollarSign,
      iconBg: "bg-emerald-50",
      iconColor: "text-emerald-600",
      infoText: "Average household income of the patient population."
    },
    {
      title: "Median Income",
      value: data.kpis.median_family_income || 0,
      prevWeek: hist.last_week?.median_income,
      prevMonth: hist.last_month?.median_income,
      prevYear: hist.last_year?.median_income,
      icon: TrendingUp,
      iconBg: "bg-orange-50",
      iconColor: "text-orange-600",
      infoText: "Middle-value household income of the patient population."
    },
    {
      title: "Avg Patient Age",
      value: data.kpis.avg_patient_age || 0,
      prevWeek: hist.last_week?.avg_age,
      prevMonth: hist.last_month?.avg_age,
      prevYear: hist.last_year?.avg_age,
      icon: ClockIcon,
      iconBg: "bg-cyan-50",
      iconColor: "text-cyan-600",
      infoText: "Average chronological age of the active patient base."
    },
    {
      title: "Marriage Rate",
      value: data.kpis.married_rate || 0,
      prevWeek: hist.last_week?.married_rate,
      prevMonth: hist.last_month?.married_rate,
      prevYear: hist.last_year?.married_rate,
      icon: Activity,
      iconBg: "bg-pink-50",
      iconColor: "text-pink-600",
      infoText: "Percentage of patients whose marital status is recorded as married."
    },
    {
      title: "Higher Ed Rate",
      value: data.kpis.higher_education_rate || 0,
      prevWeek: hist.last_week?.higher_education_rate,
      prevMonth: hist.last_month?.higher_education_rate,
      prevYear: hist.last_year?.higher_education_rate,
      icon: GraduationCap,
      iconBg: "bg-sky-50",
      iconColor: "text-sky-600",
      infoText: "Percentage of patients holding a doctorate or similar higher education degrees."
    }
  ];

  if (loading) {
    return <LoadingScreen message="Loading Patient Records..." subtext="Please wait while we gather the information." />;
  }

  return (
    <div className="animate-fade-in w-full">
      <header className="bg-white/80 backdrop-blur-md border-b border-slate-200 py-6 px-4 md:px-6 lg:px-8 sticky top-0 z-20 w-full">
        <div className="max-w-[1600px] mx-auto w-full">
          <div className="flex items-center gap-3 mb-2">
            <div className="p-2 bg-blue-50 rounded-lg">
              <Users size={24} className="text-blue-600" />
            </div>
            <h1 className="text-3xl font-black text-slate-900 tracking-tight">Patient Overview</h1>
          </div>
          <p className="text-slate-500 font-medium ml-12">Key statistics and trends across the patient population.</p>
        </div>
      </header>

      <div className="max-w-[1600px] mx-auto w-full px-4 md:px-6 lg:px-8 py-8 space-y-10">
        {/* Section 1: KPI Grid */}
        <KPICard kpis={kpiData} />

        {/* Section 2: Trend Analysis (Standard Metrics) */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          <MetricsCard
            title="Economic Dependence"
            metrics={[
              { label: "Current Ratio", value: `${data.metrics.economic_dependence_ratio}%` },
            ]}
            chartData={data.trends.economic_dependence}
            chartType="bar"
            infoText="Ratio of the non-working age population (under 20 or over 64) to the working-age population (20-64)."
          />

          <MetricsCard
            title="Cultural Diversity"
            metrics={[
              { label: "Diversity Score", value: `${data.metrics.cultural_diversity_score}%` }
            ]}
            chartData={data.trends.cultural_diversity}
            chartType="line"
            infoText="Multi-ethnic representation score where 100% represents a perfectly balanced distribution across all racial groups."
          />

          <MetricsCard
            title="Mortality Analysis"
            metrics={[
              { label: "Current Rate", value: `${data.metrics.mortality_rate?.toFixed(2)}%` }
            ]}
            chartData={data.trends.mortality_rate}
            chartType="line"
            infoText="Longitudinal death rate within the selected population, tracking changes in survival rates over the last 10 years."
          />
        </div>

        {/* Section 3: Advanced Analysis */}
        <div className="pt-10 border-t border-slate-200">
          <h2 className="text-2xl font-black text-slate-800 tracking-tight mb-8 flex items-center gap-3">
            <Globe className="text-teal-600" /> Advanced Analysis
          </h2>

          <div className="grid grid-cols-1 xl:grid-cols-2 gap-8">

            {/* 1. Survival Trend */}
            <AdvancedChartCard
              title="Actual Survival Trend"
              subtitle="Male vs Female Survival Probability"
              icon={Activity}
              infoText="Probability of survival over time by gender, calculated using the Kaplan-Meier estimate on historical patient records."
            >
              <ReactECharts
                option={survivalOption}
                style={{ height: '300px', width: '100%' }}
                opts={{ renderer: 'svg' }}
              />
            </AdvancedChartCard>

            {/* 2. Demographic Entropy */}
            <AdvancedChartCard
              title="Demographic Entropy"
              subtitle="Top Cities by Population Diversity"
              icon={Users}
              infoText="Measure of racial and ethnic diversity within specific geographic regions, using the Shannon Entropy Index."
            >
              <ReactECharts
                option={entropyOption}
                style={{ height: '300px', width: '100%' }}
                opts={{ renderer: 'svg' }}
              />
            </AdvancedChartCard>

            {/* 3. Wealth Trajectory */}
            <AdvancedChartCard
              title="Wealth Trajectory"
              subtitle="Income vs Velocity by Age Group"
              icon={DollarSign}
              infoText="Correlation between age groups and average household income, showing the rate of wealth accumulation over a lifetime."
            >
              <ReactECharts
                option={wealthOption}
                style={{ height: '300px', width: '100%' }}
                opts={{ renderer: 'svg' }}
              />
            </AdvancedChartCard>

            {/* 4. Mortality Hazard */}
            <AdvancedChartCard
              title="Mortality Hazard"
              subtitle="Hazard Probability by Income Quintile"
              icon={Scale}
              infoText="Probability of mortality categorized by income quintiles and racial demographics, highlighting socioeconomic health disparities."
            >
              <ReactECharts
                option={mortalityOption}
                style={{ height: '300px', width: '100%' }}
                opts={{ renderer: 'svg' }}
              />
            </AdvancedChartCard>

          </div>
        </div>
      </div>
    </div>
  );
};

export default PatientDashboard;
