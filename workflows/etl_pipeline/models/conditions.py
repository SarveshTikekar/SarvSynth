from pydantic import BaseModel, Field
from typing import List, Dict, Tuple

class conditionKPIS(BaseModel):
    current_active_burden: int = 0
    global_recovery_rate: float = Field(0, description="Percentage of conditions that have been completely treated")
    patient_complexity_score: int = Field(0, description="Average number of conditions per patient")
    average_time_to_cure: int = Field(0, description="Average time (in days) taken to completely treat a condition")
    admission_rate_last_30_days: int = Field(0, description="Number of admissions in the last 30 days related to conditions")
    total_diagnoses: int = Field(0, description="Total count of all condition records in the history")
    unique_conditions: int = Field(0, description="Total count of distinct medical condition concepts seen")
    chronic_condition_burden: int = Field(0, description="Count of currently active conditions lasting longer than 90 days")

    historical_comparisons: Dict[str, Dict[str, float]] = Field(default_factory=dict, description="Nested dictionary for prevWeek, prevMonth, prevYear values for each metric")

class conditionMetrics(BaseModel):
    top_disorder_conditions: List[Dict[str, int]] = Field(default_factory=list, description="List of top 5 disorder conditions by prevalence with their counts")
    chronic_vs_acute: List[Dict[str, int]] = Field(default_factory=list, description="List containing active chronic and acute conditions")
    disease_resolution_efficiency: List[Tuple[str, int, int]] = Field(default_factory=list, description="List of tuples containing medical concept, count and average time to cure for top 5 conditions")
    top_10_recurring_disorders: List[Dict[str, int]] = Field(default_factory=list, description="List of top 10 recurring disorders with their counts")
    commorbidity_pattern: List[Dict[int, int]] = Field(default_factory=list, description="Frequency of patients with multiple co-occurring conditions")
    clinical_gravity: List[Dict[str, float]]

class conditionAdvancedMetrics(BaseModel):
    average_condition_recurrence_gap: List[Dict[str, int]] = Field(default_factory=list, description="Average time gap between recurrences for top recurring conditions")
    incidence_velocity: Dict[str, Dict[str, int]] = Field(default_factory=dict, description="Rate of new condition occurrences over time for top conditions")
    commordity_cooccurence: List[Tuple[str, str, int]] = Field(default_factory=list, description="List of tuples containing pairs of co-occurring conditions and their frequency")
    disease_transition_patterns: List[Tuple[str, str, int, float]] = Field(default_factory=list, description="List of tuples containing pairs of conditions where one condition often leads to another and their frequency") 
    age_based_burden: List[Dict[str, int]] = Field(default_factory=list, description="Distribution of condition count by age groups (Health Cliff)") 