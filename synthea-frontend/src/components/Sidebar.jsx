import React, { useState } from 'react';
import { NavLink } from 'react-router-dom';
import {
    Home,
    LayoutDashboard,
    Users,
    Activity,
    AlertCircle,
    Database,
    Menu,
    X,
    ChevronLeft,
    ChevronRight,
    Stethoscope
} from 'lucide-react';
import { useLocation } from 'react-router-dom';

const Sidebar = ({ isOpen, setIsOpen, isMobile }) => {
    const location = useLocation();
    const [isCollapsed, setIsCollapsed] = useState(false);

    const toggleCollapse = () => {
        if (!isMobile) {
            setIsCollapsed(!isCollapsed);
        }
    };

    const navItems = [
        { name: 'Home', path: '/', icon: Home },
        { name: 'Main Dashboard', path: '/dashboard', icon: LayoutDashboard },
        { name: 'Patient View', path: '/patient_dashboard', icon: Users },
        { name: 'Conditions', path: '/conditions_dashboard', icon: Activity },
        { name: 'Encounters', path: '/encounters_dashboard', icon: Stethoscope },
        { name: 'Allergies', path: '/allergies_dashboard', icon: AlertCircle },
        { name: 'Data Generation', path: '/data_generation', icon: Database },
    ];

    const sidebarWidth = isCollapsed ? 'w-20' : 'w-64';
    const mobileClasses = isMobile
        ? `fixed inset-y-0 left-0 z-50 transform ${isOpen ? 'translate-x-0' : '-translate-x-full'} transition-transform duration-300 ease-in-out`
        : `sticky top-0 h-screen transition-all duration-300 ease-in-out ${sidebarWidth}`;

    return (
        <>
            {/* Overlay for mobile */}
            {isMobile && isOpen && (
                <div
                    className="fixed inset-0 bg-black/50 z-40"
                    onClick={() => setIsOpen(false)}
                />
            )}

            <aside className={`bg-white border-r border-slate-200 text-slate-700 flex flex-col shadow-sm ${mobileClasses}`}>
                {/* Header */}
                <div className="h-20 flex items-center justify-between px-6 border-b border-slate-100">
                    {!isCollapsed && (
                        <div className="flex items-center gap-3">
                            <div className="w-8 h-8 bg-teal-50 rounded-lg flex items-center justify-center">
                                <span className="font-bold text-teal-600">S</span>
                            </div>
                            <span className="font-bold text-lg tracking-tight text-slate-800">Sarv<span className="text-teal-700">Synth</span></span>
                        </div>
                    )}
                    {isCollapsed && (
                        <div className="w-full flex justify-center">
                            <div className="w-8 h-8 bg-teal-50 rounded-lg flex items-center justify-center">
                                <span className="font-bold text-teal-600">S</span>
                            </div>
                        </div>
                    )}

                    {isMobile && (
                        <button onClick={() => setIsOpen(false)} className="text-slate-400 hover:text-slate-600">
                            <X size={24} />
                        </button>
                    )}
                </div>

                {/* Navigation */}
                <nav className="flex-1 py-6 px-3 space-y-1 overflow-y-auto">
                    {navItems.map((item) => {
                        const isActive = location.pathname === item.path;
                        return (
                            <NavLink
                                key={item.path}
                                to={item.path}
                                onClick={() => isMobile && setIsOpen(false)}
                                className={`flex items-center gap-3 px-3 py-3 rounded-xl transition-all duration-200 group
                  ${isActive
                                        ? 'bg-teal-50 text-teal-600 shadow-sm border border-teal-100'
                                        : 'text-slate-500 hover:bg-slate-50 hover:text-slate-700 hover:pl-4'}
                `}
                                title={isCollapsed ? item.name : ''}
                            >
                                <item.icon size={20} className={`min-w-[20px] ${isActive ? 'text-teal-600' : 'text-slate-400 group-hover:text-slate-600'}`} />
                                {!isCollapsed && (
                                    <span className="font-medium truncate">{item.name}</span>
                                )}

                                {/* Active Indicator */}
                                {isActive && !isCollapsed && (
                                    <div className="ml-auto w-1.5 h-1.5 rounded-full bg-teal-500" />
                                )}
                            </NavLink>
                        );
                    })}
                </nav>

                {/* Footer / User Profile */}
                <div className="p-4 border-t border-slate-100">
                    <button className={`flex items-center gap-3 w-full p-2 rounded-xl hover:bg-slate-50 transition-colors ${isCollapsed ? 'justify-center' : ''}`}>
                        <div className="w-10 h-10 rounded-full bg-slate-100 border border-slate-200 flex items-center justify-center text-xs font-bold text-slate-600 shadow-sm">
                            DR
                        </div>
                        {!isCollapsed && (
                            <div className="text-left overflow-hidden">
                                <p className="text-sm font-bold text-slate-700 truncate">Dr. Sarvesh</p>
                                <p className="text-xs text-slate-500 truncate">System Administrator</p>
                            </div>
                        )}
                    </button>
                </div>

                {/* Collapse Toggle (Desktop Only) */}
                {!isMobile && (
                    <button
                        onClick={toggleCollapse}
                        className="absolute -right-3 top-24 bg-white hover:bg-slate-50 text-slate-400 hover:text-teal-600 p-1 rounded-full shadow-md border border-slate-200 transition-transform hover:scale-110 z-50"
                    >
                        {isCollapsed ? <ChevronRight size={14} /> : <ChevronLeft size={14} />}
                    </button>
                )}
            </aside>
        </>
    );
};

export default Sidebar;
