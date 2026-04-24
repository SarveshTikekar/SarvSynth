import React from 'react';
import { Activity, Shield } from 'lucide-react';

const LoadingScreen = ({ message = "Loading Records...", subtext = "Please wait while we gather the information." }) => {
  return (
    <div className="fixed inset-0 bg-slate-50/80 backdrop-blur-sm z-[9999] flex items-center justify-center animate-clinical-fade">
      <div className="max-w-md w-full px-6 text-center">
        <div className="relative mb-8 flex justify-center">
          {/* Animated rings */}
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="w-24 h-24 border-4 border-teal-100 rounded-full animate-ping opacity-20"></div>
          </div>
          <div className="absolute inset-0 flex items-center justify-center">
            <div className="w-20 h-20 border-4 border-teal-200 rounded-full animate-pulse opacity-40"></div>
          </div>
          
          {/* Main Logo/Icon Container */}
          <div className="relative w-16 h-16 bg-white rounded-3xl shadow-xl border border-teal-100 flex items-center justify-center z-10">
            <Activity className="text-teal-600 animate-spin-slow" size={32} />
          </div>
        </div>

        <div className="space-y-3">
          <h2 className="text-2xl font-black text-slate-800 tracking-tight font-signika uppercase tracking-widest">
            {message}
          </h2>
          <div className="flex items-center justify-center gap-2 text-slate-500 font-medium">
            <Shield size={16} className="text-teal-500" />
            <p>{subtext}</p>
          </div>
        </div>

        {/* Progress bar simulation */}
        <div className="mt-10 w-full bg-slate-200 h-1.5 rounded-full overflow-hidden">
          <div className="bg-teal-600 h-full animate-loading-bar w-1/2 rounded-full shadow-[0_0_10px_rgba(13,148,136,0.5)]"></div>
        </div>
      </div>
    </div>
  );
};

export default LoadingScreen;
