import React from "react";

const FeatureCard = ({ Icon, title, desc }) => {

	return (

		<div className="bg-slate-50/50 p-8 rounded-2xl border border-slate-100 hover:border-teal-500/30 hover:shadow-lg hover:bg-white transition-all duration-300 group h-full flex flex-col items-start text-left active:scale-[0.98]">

			<div className="w-12 h-12 bg-white rounded-xl border border-slate-200 flex items-center justify-center mb-6 text-teal-600 group-hover:scale-110 group-hover:border-teal-500/50 group-hover:text-teal-700 transition-all shadow-sm shrink-0">
				{Icon && <Icon size={24} />}
			</div>

			<h3 className="text-lg font-bold text-slate-900 mb-3 group-hover:text-teal-700 transition-colors w-full">
				{title}
			</h3>

			<p className="text-slate-500 leading-relaxed text-sm w-full">
				{desc}
			</p>
		</div>
	)
}

export default FeatureCard;
