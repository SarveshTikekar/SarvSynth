from pydantic import BaseModel, Field
from typing import List, Dict, Tuple, Any

class patientKPIS(BaseModel):
    total_patients: int = 0
    active_patient_rate: float = 0
    gender_balance_ratio: float = 0
    mean_family_income: int = 0
    median_family_income: int = 0
    avg_patient_age: float = Field(0.0, description="Average age of the current patient population")
    married_rate: float = Field(0.0, description="Percentage of patients who are married")
    higher_education_rate: float = Field(0.0, description="Percentage of patients with a doctorate or higher education level")

class patientMetrics(BaseModel):
    economic_dependence_ratio: int = 0
    cultural_diversity_score: int = 0 
    mortality_rate: float = 0
    age_wealth_correlation: float = 0
    income_inequality_index: float = 0

class patientAdvancedMetrics(BaseModel):
    actural_survival_trend: List[Dict[str, List[Dict[int, float]]]] = Field(default_factory=list)
    demographic_entropy: List[Tuple[str, float, List[Dict[str, int]]]] = Field(default_factory=list)
    wealth_trajectory: List[Tuple[str, int, int]] = Field(default_factory=list)
    mortality_hazard_by_quintiles: Dict[str, List[Tuple[str, List[int], float]]] = Field(default_factory=dict)
    
    # Pre-calculated trends for the API
    economic_dependence_trend: List[Dict[int, int]] = Field(default_factory=list)
    cultural_diversity_trend: List[Dict[int, int]] = Field(default_factory=list)
    mortality_rate_trend: List[Dict[int, float]] = Field(default_factory=list)
