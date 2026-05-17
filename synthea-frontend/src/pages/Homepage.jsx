import React from "react";
import { Link } from "react-router-dom";
import { ArrowRight, Activity, Users, LayoutDashboard, TrendingUp, Layers, Clock as ClockIcon, Database, Globe, ChevronRight } from "lucide-react";
import FeatureCard from "../components/FeatureCard";

const Homepage = () => {
  return (
    <div className="font-sans text-slate-900 overflow-hidden relative">

      {/* Background Decoration */}
      <div className="absolute top-0 left-0 w-full h-[600px] bg-gradient-to-b from-teal-50 to-white -z-10 pointer-events-none"></div>
      <div className="absolute top-0 right-0 w-[600px] h-[600px] bg-teal-200/20 rounded-full blur-3xl -translate-y-1/2 translate-x-1/2 -z-10 animate-pulse-slow"></div>

      {/* Hero Section */}
      <div className="relative pt-24 pb-20 px-4 text-center">

        <h1 className="text-5xl lg:text-7xl font-black text-slate-900 mb-8 tracking-tight leading-tight max-w-5xl mx-auto animate-fade-in-up delay-100">
          Hospital <br />
          <span className="text-transparent bg-clip-text bg-gradient-to-r from-teal-600 to-blue-600">Analytics System</span>
        </h1>

        <p className="text-xl text-slate-500 font-medium max-w-3xl mx-auto mb-12 leading-relaxed animate-fade-in-up delay-200">
          An enterprise-grade clinical intelligence and operational analytics platform powered by standardized synthetic FHIR patient data. 
          Conduct high-fidelity cohort exploration, monitor epidemiological progression, track pathological complexity, and optimize facility operations in real-time.
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
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Simulated Cohort</p>
        </div>
        <div className="text-center border-l border-slate-100">
          <p className="text-4xl font-black text-teal-600 mb-1">50+</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Clinical Indicators</p>
        </div>
        <div className="text-center border-l border-slate-100 hidden md:block">
          <p className="text-4xl font-black text-blue-600 mb-1">ETL</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Distributed PySpark</p>
        </div>
        <div className="text-center border-l border-slate-100 hidden md:block">
          <p className="text-4xl font-black text-rose-500 mb-1">Real</p>
          <p className="text-sm font-bold text-slate-400 uppercase tracking-wider">Time Ingestion</p>
        </div>
      </div>

      {/* Features Grid */}
      <div className="max-w-7xl mx-auto px-4 pb-20">
        <div className="text-center mb-16">
          <h2 className="text-3xl font-black text-slate-900 mb-4">Clinical & Operational Intelligence</h2>
          <p className="text-slate-500">Comprehensive diagnostic dashboards covering demographic trajectories, pathological trends, and facility throughput.</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8">
          <FeatureCard
            Icon={Users}
            title="Demographic Trajectories"
            desc="Analyze cohort migration, longitudinal survival probabilities, and multi-dimensional socioeconomic indicators."
          />
          <FeatureCard
            Icon={Activity}
            title="Pathological Complexity"
            desc="Evaluate disease prevalence, active clinical burdens, chronic progression, and comorbidity networks."
          />
          <FeatureCard
            Icon={TrendingUp}
            title="Operational Throughput"
            desc="Monitor facility utilization, clinical encounter durations, practitioner caseload distribution, and coverage patterns."
          />
          <FeatureCard
            Icon={Layers}
            title="Distributed Data Pipelines"
            desc="A highly optimized Apache Spark ETL architecture for high-throughput normalization of standard clinical records."
          />
          <FeatureCard
            Icon={ClockIcon}
            title="Low-Latency Queries"
            desc="Asynchronous microservices utilizing schema-anchored dependency injection for rapid executive reporting."
          />
          <FeatureCard
            Icon={Globe}
            title="Spatial Epidemiology"
            desc="Assess spatial demographic entropy, local cultural diversity indices, and regional health inequality markers."
          />
        </div>
      </div>
    </div>
  );
};

export default Homepage;
