from pydantic import BaseModel, Field
from typing import List, Dict, Tuple, Any

class encountersKPIS(BaseModel):
    total_visit_volume: int = Field(0, description="Total number of encounters across all hospitals")
    total_revenue_generated: float = Field(0.0, description="Sum of visting_total_fees across all encounters")
    average_encounter_duration_hours: float = Field(0.0, description="Average time spent per encounter in hours")
    average_patient_out_of_pocket: float = Field(0.0, description="Average financial burden falling on patients per encounter")
    average_base_fee: float = Field(0.0, description="Average base cost before addons")
    total_covered_amount: float = Field(0.0, description="Total amount paid by insurers")
    unique_patients_seen: int = Field(0, description="Number of distinct patients seen in 30 days")
    average_practitioner_load: float = Field(0.0, description="Average number of encounters handled per practitioner")

    historical_comparisons: Dict[str, Dict[str, float]] = Field(default_factory=dict, description="Nested dictionary for prevWeek, prevMonth, prevYear values for each metric")

class encountersMetrics(BaseModel):
    encounters_by_type: List[Dict[str, Any]] = Field(default_factory=list, description="Distribution of encounters segmented by encounter_type")
    top_10_causes: List[Dict[str, Any]] = Field(default_factory=list, description="Top 10 most frequent reasons for visits")
    coverage_vs_oop_by_type: List[Dict[str, Any]] = Field(default_factory=list, description="Average Out-of-Pocket and Covered amounts split by encounter type")
    top_10_practitioners: List[Dict[str, Any]] = Field(default_factory=list, description="Top 10 practitioners by volume")
    fee_divergence_by_type: List[Dict[str, Any]] = Field(default_factory=list, description="Average Base Fee vs Total Fee grouped by encounter type")
    most_expensive_causes: List[Dict[str, Any]] = Field(default_factory=list, description="Top 10 encounter causes with highest average total fee")

class encountersAdvancedMetrics(BaseModel):
    high_cost_anomaly_index: List[Dict[str, Any]] = Field(default_factory=list, description="Count of outlier encounters where total fees are > 2 std dev above mean per encounter reason")
    duration_distribution_by_type: List[Dict[str, Any]] = Field(default_factory=list, description="Average duration in hours per encounter type")
    readmission_timeline: List[Dict[str, Any]] = Field(default_factory=list, description="Monthly active patients vs repeat patients (Frequent Flyers)")
    uncovered_cost_trajectory: List[Dict[str, Any]] = Field(default_factory=list, description="Monthly average out-of-pocket costs to track burden trajectory")
