import React, { useState } from "react";
import { ArrowUpRight, ArrowDownRight, Info } from "lucide-react";

const SingleKPICard = ({ kpi, index }) => {
  const [timeframe, setTimeframe] = useState("month");
  const Icon = kpi.icon;

  // Resolve dynamic previous value based on timeframe
  let activePrevValue = kpi.prevValue || 0;
  let activePeriodType = timeframe;

  if (timeframe === "week") activePrevValue = kpi.prevWeek ?? kpi.prevValue ?? 0;
  if (timeframe === "month") activePrevValue = kpi.prevMonth ?? kpi.prevValue ?? 0;
  if (timeframe === "year") activePrevValue = kpi.prevYear ?? kpi.prevValue ?? 0;

  if (!kpi.prevWeek && !kpi.prevMonth && !kpi.prevYear && typeof kpi.value === 'number') {
    if (timeframe === "week") activePrevValue = kpi.value * 0.98;
    if (timeframe === "month") activePrevValue = kpi.value * 0.92;
    if (timeframe === "year") activePrevValue = kpi.value * 0.75;
  }

  const diff = kpi.value - activePrevValue;
  const percentage = activePrevValue ? (diff / activePrevValue) * 100 : 0;

  // Sentiment Logic
  const isIncrease = diff > 0;
  const higherIsBetter = kpi.sentiment !== "lower-is-better";
  const isPositiveChange = isIncrease ? higherIsBetter : !higherIsBetter;

  // Styling based on change
  const statusColor = diff === 0 ? "text-slate-400" : isPositiveChange ? "text-emerald-500" : "text-rose-500";
  const statusBg = diff === 0 ? "bg-slate-50" : isPositiveChange ? "bg-emerald-50" : "bg-rose-50";

  return (
    <div
      key={index}
      className="group bg-white rounded-[2rem] border border-slate-100 shadow-sm hover:shadow-xl hover:border-teal-100 transition-all duration-300 relative flex flex-col h-full"
    >
      {/* Background Icon Container */}
      <div className="absolute inset-0 overflow-hidden rounded-[2rem] pointer-events-none">
        <div className="absolute -bottom-2 -right-2 text-slate-50 opacity-10 group-hover:text-teal-500 transition-colors">
          <Icon size={60} />
        </div>
      </div>

      <div className="relative p-6 flex flex-col h-full min-h-full" style={{ overflow: 'visible' }}>
        {/* Top Row: Icon & Delta Badge */}
        <div className="flex items-start justify-between mb-4">
          <div className={`p-3 rounded-2xl ${kpi.iconBg || 'bg-slate-50'} ${kpi.iconColor || 'text-slate-600'} transition-colors group-hover:bg-teal-500 group-hover:text-white`}>
            <Icon size={20} />
          </div>

          {kpi.prevValue !== undefined && (
            <div className={`flex items-center gap-0.5 px-2 py-1 rounded-lg text-[10px] font-black ${statusColor} ${statusBg} border border-white shadow-sm`}>
              {isIncrease ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
              {Math.abs(percentage).toFixed(1)}%
            </div>
          )}
        </div>

        {/* Label & Value */}
        <div className="space-y-1 flex-1">
            {kpi.infoText && (
              <div className="absolute top-4 right-4 z-10 group/tooltip" style={{ overflow: 'visible' }}>
                <Info size={14} className="text-slate-300 hover:text-teal-500 transition-colors cursor-help" />
                <div className="absolute right-0 top-full mt-2 w-64 bg-slate-900/95 backdrop-blur-md text-white text-[11px] leading-relaxed rounded-2xl p-4 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-2xl pointer-events-none normal-case tracking-normal font-medium border border-white/10">
                  <div className="absolute right-2 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-900/95"></div>
                  {kpi.infoText}
                </div>
              </div>
            )}
            <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">
              {kpi.title}
            </p>
          <h3 className="text-3xl font-black text-slate-900 tracking-tight">
            {typeof kpi.value === 'number' ? kpi.value.toLocaleString() : kpi.value}
          </h3>
        </div>

        {/* Footer: Previous Period Info and timeframe toggle */}
        <div className="mt-auto pt-4 border-t border-slate-50 flex items-center justify-between">
          <p className="text-[10px] text-slate-400 font-medium">
            Vs. last <span className="lowercase">{activePeriodType}</span>:
            <span className="text-slate-700 font-bold ml-1">
              {activePrevValue.toLocaleString(undefined, { maximumFractionDigits: 1 })}
            </span>
          </p>

          <div className="flex gap-1 bg-slate-100/50 p-0.5 rounded-lg border border-slate-200/50">
            {[{ label: "W", val: "week" }, { label: "M", val: "month" }, { label: "Y", val: "year" }].map((tf) => (
              <button
                key={tf.val}
                onClick={() => setTimeframe(tf.val)}
                className={`px-1.5 py-0.5 rounded text-[9px] font-black uppercase transition-all ${timeframe === tf.val ? 'bg-white text-teal-600 shadow-sm border border-slate-200' : 'text-slate-400 hover:text-slate-700'}`}
              >
                {tf.label}
              </button>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
};

const KPICard = ({ kpis }) => {
  return (
    <div className="mb-10 w-full space-y-4">
      <div className="grid grid-cols-2 lg:grid-cols-4 gap-6">
        {kpis.map((kpi, index) => (
          <SingleKPICard key={index} index={index} kpi={kpi} />
        ))}
      </div>
    </div>
  );
};

export default KPICard;
