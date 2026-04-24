import React, { useMemo } from "react";
import {
  AreaChart, Area, BarChart, Bar,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from "recharts";
import { BarChart3, TrendingUp, History, Info } from "lucide-react";

const MetricsCard = ({ title, metrics, chartData, chartType = "line", infoText = "", children = null }) => {

  // Data is now normalized as [{ name: "Year", value: val }] from the backend
  const formattedData = useMemo(() => {
    if (!chartData || !Array.isArray(chartData)) return [];
    
    // If the data is already in {name, value} format, just reverse it for chronological order
    if (chartData.length > 0 && chartData[0].name) {
        return [...chartData].reverse();
    }

    // Fallback for old format just in case
    return chartData.map(item => {
      const year = Object.keys(item)[0];
      return { name: year, value: item[year] };
    }).reverse();
  }, [chartData]); 

  return (
    <div className="bg-white rounded-[2.5rem] border border-slate-100 shadow-sm mb-8 flex flex-col h-full overflow-hidden">

      {/* Top Section: Header & Summary */}
      <div className="w-full p-6 border-b border-slate-100 bg-slate-50/50 flex flex-col sm:flex-row sm:items-center justify-between gap-6 rounded-t-[2.5rem]">
        <div className="flex items-center gap-3">
          <div className="p-2.5 bg-white rounded-xl shadow-sm">
            <BarChart3 size={22} className="text-teal-600" />
          </div>
          <div className="flex items-center gap-2">
            <h3 className="text-xl font-black text-slate-800 tracking-tight">{title}</h3>
            {infoText && (
              <div className="relative group/tooltip flex items-center">
                <Info size={16} className="text-slate-400 hover:text-teal-600 transition-colors cursor-help" />
                <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-max max-w-xs bg-slate-800 text-white text-xs rounded-xl p-3 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-xl pointer-events-none normal-case tracking-normal font-normal">
                  <div className="absolute left-1/2 -translate-x-1/2 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-800"></div>
                  {infoText}
                </div>
              </div>
            )}
          </div>
        </div>

        <div className="flex flex-wrap gap-3">
          {metrics.map((metric, index) => (
            <div key={index} className="bg-white px-4 py-2 rounded-2xl border border-slate-100 shadow-sm flex items-center gap-3 group hover:border-teal-200 transition-colors">
              <span className="text-xs font-bold text-slate-400 uppercase tracking-wider">{metric.label}</span>
              <span className="text-lg font-black text-slate-900 group-hover:text-teal-600 transition-colors">{metric.value}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Main Chart Area */}
      <div className="w-full p-6 relative flex-1 flex flex-col min-h-0">
        <div className="flex-1 w-full min-h-[300px]">
          {children ? children : (
            <ResponsiveContainer width="100%" height="100%">
              {formattedData.length > 0 ? (
                chartType === "line" ? (
                  <AreaChart data={formattedData}>
                    <defs>
                      <linearGradient id="colorMetric" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#14b8a6" stopOpacity={0.2} />
                        <stop offset="95%" stopColor="#14b8a6" stopOpacity={0} />
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="0" vertical={false} stroke="#f1f5f9" />
                    <XAxis
                      dataKey="name"
                      fontSize={11}
                      fontWeight={700}
                      axisLine={false}
                      tickLine={false}
                      tick={{ fill: '#94a3b8' }}
                      dy={10}
                    />
                    <YAxis
                      fontSize={11}
                      fontWeight={700}
                      axisLine={false}
                      tickLine={false}
                      tick={{ fill: '#94a3b8' }}
                    />
                    <Tooltip
                      contentStyle={{ borderRadius: '16px', border: 'none', boxShadow: '0 10px 15px -3px rgba(0,0,0,0.1)' }}
                      cursor={{ stroke: '#14b8a6', strokeWidth: 2, strokeDasharray: '4 4' }}
                    />
                    <Area
                      type="monotone"
                      dataKey="value"
                      stroke="#14b8a6"
                      strokeWidth={4}
                      fillOpacity={1}
                      fill="url(#colorMetric)"
                      animationDuration={1500}
                    />
                  </AreaChart>
                ) : (
                  <BarChart data={formattedData}>
                    <CartesianGrid strokeDasharray="0" vertical={false} stroke="#f1f5f9" />
                    <XAxis dataKey="name" fontSize={11} fontWeight={700} axisLine={false} tickLine={false} tick={{ fill: '#94a3b8' }} dy={10} />
                    <YAxis fontSize={11} fontWeight={700} axisLine={false} tickLine={false} tick={{ fill: '#94a3b8' }} />
                    <Tooltip cursor={{ fill: 'rgba(20, 184, 166, 0.05)' }} />
                    <Bar dataKey="value" fill="#14b8a6" radius={[6, 6, 0, 0]} barSize={40} animationDuration={1500} />
                  </BarChart>
                )
              ) : (
                <div className="flex flex-col items-center justify-center h-full text-slate-400 gap-2">
                  <History size={32} className="opacity-20" />
                  <p className="text-sm font-medium">No trend data available</p>
                </div>
              )}
            </ResponsiveContainer>
          )}
        </div>
      </div>
    </div>
  );
};

export default MetricsCard;
