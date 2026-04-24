import React from 'react';
import { ResponsiveContainer } from 'recharts';
import { Info } from "lucide-react";

const AdvancedChartCard = ({ title, subtitle, icon: Icon, children, rightElement = null, infoText = "" }) => {
    return (
        <div className="bg-white p-6 rounded-[2rem] border border-slate-100 shadow-sm hover:shadow-xl transition-all duration-300 flex flex-col h-full group relative">
            
            {infoText && (
                <div className="absolute top-6 right-6 z-10 group/tooltip">
                    <Info size={16} className="text-slate-300 hover:text-teal-600 transition-colors cursor-help" />
                    <div className="absolute right-0 top-full mt-2 w-72 bg-slate-900/95 backdrop-blur-md text-white text-[11px] leading-relaxed rounded-2xl p-4 opacity-0 invisible group-hover/tooltip:opacity-100 group-hover/tooltip:visible transition-all z-[200] shadow-2xl pointer-events-none normal-case tracking-normal font-medium border border-white/10">
                        <div className="absolute right-2 bottom-full w-0 h-0 border-l-8 border-r-8 border-b-8 border-transparent border-b-slate-900/95"></div>
                        {infoText}
                    </div>
                </div>
            )}

            <div className="flex items-start justify-between mb-6">
                <div className="flex items-center gap-3">
                    {Icon && (
                        <div className="p-2.5 bg-slate-50 rounded-xl text-teal-600 group-hover:bg-teal-50 group-hover:text-teal-700 transition-colors">
                            <Icon size={20} />
                        </div>
                    )}
                    <div>
                        <h3 className="text-lg font-bold text-slate-800 tracking-tight">{title}</h3>
                        {subtitle && <p className="text-[10px] font-black text-slate-400 uppercase tracking-widest">{subtitle}</p>}
                    </div>
                </div>
                {rightElement}
            </div>

            <div className="flex-1 w-full min-h-[300px]">
                <ResponsiveContainer width="100%" height="100%">
                    {children}
                </ResponsiveContainer>
            </div>
        </div>
    );
};

export default AdvancedChartCard;
