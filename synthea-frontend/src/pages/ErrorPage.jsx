import React from 'react';
import { useRouteError, Link } from 'react-router-dom';
import { AlertTriangle, Home, RefreshCcw } from 'lucide-react';

const ErrorPage = () => {
    const error = useRouteError();
    console.error(error);

    const referenceId = "ERR-5X29Z"; // Simple fixed ID or use something else if needed

    return (
        <div className="min-h-screen bg-slate-50 flex items-center justify-center p-4">
            <div className="max-w-md w-full bg-white rounded-3xl shadow-2xl shadow-slate-200 border border-slate-100 p-8 text-center animate-fade-in">
                <div className="w-20 h-20 bg-rose-50 rounded-2xl flex items-center justify-center mx-auto mb-6">
                    <AlertTriangle size={40} className="text-rose-500" />
                </div>
                
                <h1 className="text-2xl font-black text-slate-900 mb-2">Oops! Application Error</h1>
                <p className="text-slate-500 mb-8 font-medium">
                    {error.statusText || error.message || "An unexpected error occurred while rendering this page."}
                </p>

                <div className="space-y-3">
                    <button 
                        onClick={() => window.location.reload()}
                        className="w-full flex items-center justify-center gap-2 py-3 bg-teal-600 hover:bg-teal-700 text-white rounded-xl font-bold transition-all shadow-lg shadow-teal-100"
                    >
                        <RefreshCcw size={18} /> Reload Application
                    </button>
                    
                    <Link 
                        to="/"
                        className="w-full flex items-center justify-center gap-2 py-3 bg-white border border-slate-200 text-slate-600 hover:bg-slate-50 rounded-xl font-bold transition-all"
                    >
                        <Home size={18} /> Back to Home
                    </Link>
                </div>

                <div className="mt-8 pt-6 border-t border-slate-50">
                    <p className="text-[10px] font-mono text-slate-400 uppercase tracking-widest">
                        Reference ID: {referenceId}
                    </p>
                </div>
            </div>
        </div>
    );
};

export default ErrorPage;
