import React from "react";
import { Link } from "react-router-dom";
import { ArrowRight, Activity, Users, LayoutDashboard, TrendingUp, Layers, Zap, Database, Globe, ChevronRight } from "lucide-react";
import FeatureCard from "../components/FeatureCard";

const Homepage = () => {
  return (
    <div className="font-sans text-slate-900 overflow-hidden relative">

      {/* Background Decoration */}
      <div className="absolute top-0 left-0 w-full h-[600px] bg-gradient-to-b from-teal-50 to-white -z-10 pointer-events-none"></div>
      <div className="absolute top-0 right-0 w-[600px] h-[600px] bg-teal-200/20 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2 -z-10 animate-pulse-slow"></div>

      {/* Hero Section */}
      <div className="relative pt-20 pb-20 px-4 text-center">
        <div className="inline-flex items-center gap-2 px-4 py-2 bg-white border border-teal-100 rounded-full shadow-sm mb-8 animate-fade-in-up">
          <span className="relative flex h-3 w-3">
            <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-teal-400 opacity-75"></span>
            <span className="relative inline-flex rounded-full h-3 w-3 bg-teal-500"></span>
          </span>
          <span className="text-xs font-bold text-teal-800 uppercase tracking-wide">System Online v2.0</span>
        </div>

        <h1 className="text-5xl lg:text-7xl font-black text-slate-900 mb-8 tracking-tight leading-tight max-w-5xl mx-auto animate-fade-in-up delay-100">
          Next-Gen Hospital <br />
          <span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-600 to-blue-600">Management System</span>
        </h1>

        <p className="text-xl text-slate-500 font-medium max-w-2xl mx-auto mb-12 leading-relaxed animate-fade-in-up delay-200">
          A modern, ERP-style management system powered by synthetic FHIR patient data.
          Visualize epidemiological trends, track pathology, and optimize operations in real-time.
        </p>

        <div className="flex flex-col sm:flex-row gap-4 justify-center items-center animate-fade-in-up delay-300">
          <Link
            to="/dashboard"
            className="bg-slate-900 hover:bg-slate-800 text-white px-8 py-4 rounded-2xl font-bold text-lg shadow-xl shadow-slate-200 transition-all flex items-center gap-3 hover:-translate-y-1 hover:shadow-2xl"
          >
            <LayoutDashboard size={20} />
            Launch Dashboard
          </Link>
          <a
            href="https://github.com/SarveshTikekar/Synthea-Based-Hospital-Dashboard"
            target="_blank"
            rel="noreferrer"
            className="bg-white hover:bg-slate-50 text-slate-600 border-2 border-slate-100 px-8 py-4 rounded-2xl font-bold text-lg transition-all flex items-center gap-3 hover:border-teal-100 hover:text-teal-600 group"
          >
            <Database size={20} />
            View Repository
            <ChevronRight size={16} className="text-slate-300 group-hover:text-teal-500 transition-colors" />
          </a>
        </div>
      </div>

      {/* Features Stats Strip */}
      <div className="max-w-6xl mx-auto bg-white rounded-3xl shadow-xl shadow-slate-200/50 border border-slate-100 p-8 mb-24 grid grid-cols-2 md:grid-cols-4 gap-8 animate-fade-in-up delay-500">
        <div className="text-center">
          <p className="text-4xl font-black text-slate-900 mb-1">10k+</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Patients Simulated</p>
        </div>
        <div className="text-center border-l border-slate-100">
          <p className="text-4xl font-black text-teal-600 mb-1">50+</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Metrics Tracked</p>
        </div>
        <div className="text-center border-l border-slate-100 hidden md:block">
          <p className="text-4xl font-black text-blue-600 mb-1">ETL</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">PySpark Pipeline</p>
        </div>
        <div className="text-center border-l border-slate-100 hidden md:block">
          <p className="text-4xl font-black text-rose-500 mb-1">Real</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Time Analytics</p>
        </div>
      </div>

      {/* Features Grid */}
      <div className="max-w-7xl mx-auto px-4 pb-20">
        <div className="text-center mb-16">
          <h2 className="text-3xl font-black text-slate-900 mb-4">Comprehensive Insights</h2>
          <p className="text-slate-500">From individual patient intake to global population health trends.</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          <FeatureCard
            icon={Users}
            title="Patient Demographics"
            desc="Track population trends, survivor probabilities, and socioeconomic factors."
          />
          <FeatureCard
            icon={Activity}
            title="Pathology Engine"
            desc="Advanced condition tracking including comorbidity heatmaps and recurrence rates."
          />
          <FeatureCard
            icon={TrendingUp}
            title="Operational Metrics"
            desc="Monitor admission rates, length of stay, and resource utilization efficiency."
          />
          <FeatureCard
            icon={Layers}
            title="Scalable ETL"
            desc="Robust PySpark architecture handles massive datasets with ease."
          />
          <FeatureCard
            icon={Clock}
            title="High Performance"
            desc="Efficient backend architecture delivering aggregated metrics in milliseconds."
          />
          <FeatureCard
            icon={Globe}
            title="Geo-Spatial Awareness"
            desc="Analyze demographic entropy and diversity scores across regions."
          />
        </div>
      </div>
    </div>
  );
};

export default Homepage;
