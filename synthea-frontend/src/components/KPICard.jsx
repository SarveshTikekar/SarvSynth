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
      className="group bg-white rounded border border-slate-200 hover:border-slate-300 transition-all duration-200 relative flex flex-row items-stretch p-4 h-full"
    >
      {/* Left side: Main details (centralized value, title, comparison) */}
      <div className="flex-1 flex flex-col justify-between pr-4">
        <div>
          <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest mb-1">
            {kpi.title}
          </p>
          {/* Centralized main value, visible first */}
          <h3 className="text-3xl font-black text-slate-900 tracking-tight mb-2">
            {typeof kpi.value === 'number' ? kpi.value.toLocaleString() : kpi.value}
          </h3>
        </div>

        {/* Bottom vs comparison */}
        <div className="flex items-center gap-2 flex-wrap mt-auto">
          <p className="text-[9px] text-slate-400 font-semibold">
            Vs. last <span className="lowercase">{activePeriodType}</span>:
            <span className="text-slate-700 font-bold ml-1">
              {activePrevValue.toLocaleString(undefined, { maximumFractionDigits: 1 })}
            </span>
          </p>

          {kpi.prevValue !== undefined && (
            <div className={`flex items-center gap-0.5 px-1.5 py-0.5 rounded text-[9px] font-black ${statusColor} ${statusBg} border border-slate-100`}>
              {isIncrease ? <ArrowUpRight size={10} /> : <ArrowDownRight size={10} />}
              {Math.abs(percentage).toFixed(1)}%
            </div>
          )}
        </div>
      </div>

      {/* Right side: Icon and Vertical Timeframe Selection Stack */}
      <div className="flex flex-col justify-between items-end border-l border-slate-100 pl-4 w-12 shrink-0">
        {/* Tooltip Info icon */}
        {kpi.infoText && (
          <div className="absolute top-4 right-18 z-10 group/tooltip" style={{ overflow: 'visible' }}>
            <Info size={14} className="text-slate-300 hover:text-teal-500 transition-colors cursor-help" />
            <div className="absolute right-0 top-full mt-2 w-64 bg-slate-900/95 backdrop-blur-md text-white text-[11px] leading-relaxed rounded p-4 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-2xl pointer-events-none normal-case tracking-normal font-medium border border-white/10">
              <div className="absolute right-2 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-900/95"></div>
              {kpi.infoText}
            </div>
          </div>
        )}

        <div className={`p-2 rounded border border-slate-100 ${kpi.iconBg || 'bg-slate-50'} ${kpi.iconColor || 'text-slate-600'}`}>
          <Icon size={16} />
        </div>

        {/* Vertical Stack: W, M, Y */}
        <div className="flex flex-col gap-1 w-full mt-4">
          {[{ label: "W", val: "week" }, { label: "M", val: "month" }, { label: "Y", val: "year" }].map((tf) => (
            <button
              key={tf.val}
              onClick={() => setTimeframe(tf.val)}
              className={`w-full py-1 text-center rounded text-[9px] font-black uppercase transition-all border cursor-pointer ${
                timeframe === tf.val 
                  ? 'bg-teal-50 border-teal-200 text-teal-700 shadow-sm font-bold' 
                  : 'bg-white border-slate-200 text-slate-400 hover:text-slate-700 hover:bg-slate-50'
              }`}
            >
              {tf.label}
            </button>
          ))}
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
