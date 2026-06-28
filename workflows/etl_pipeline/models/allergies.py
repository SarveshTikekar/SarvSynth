from pydantic import BaseModel, Field
from typing import List, Dict, Tuple, Any

class allergyKPIS(BaseModel):
    # Simple KPIs
    total_allergic_population: int = Field(0, description="Total number of patients with at least one recorded allergy")
    active_allergy_percentage: float = Field(0.0, description="Percentage of allergies that are active (no cure date)")
    severe_allergy_incidence_rate: float = Field(0.0, description="Percentage of all allergies with severe status")
    allergic_patient_rate: float = Field(0.0, description="Percentage of total registered patient population with at least one allergy")

    # Complex KPIs
    penicillin_allergy_delabeling_eligibility_rate: float = Field(0.0, description="Percentage of penicillin-allergic patients eligible for de-labeling")
    allergy_related_readmission_rate: float = Field(0.0, description="Percentage of patients with allergy-related encounters readmitted within 30 days")
    poly_allergen_patient_rate: float = Field(0.0, description="Percentage of patients with 2+ recorded allergies among all allergic patients")
    drug_hypersensitivity_rate: float = Field(0.0, description="Percentage of patients with drug allergies among all allergic patients")
    
    # Original field from starter code
    allergy_risk_stratification: List[Tuple[str, int, float]] = Field(default_factory=list, description="List of tuples containing (allergen_nature, number_of_patients, percentage_of_allergic_population)")
    
    # Historical comparisons (like other metrics)
    historical_data: Dict[str, Dict[str, Any]] = Field(default_factory=dict, description="Historical data for each KPI to track trends over time")

class allergyMetrics(BaseModel):
    top_10_causative_agents: List[Dict[str, Any]] = Field(default_factory=list, description="List of top 10 causative agents with their respective counts")
    severity_distribution: List[Dict[str, Any]] = Field(default_factory=list, description="Distribution of allergy severities among patients") 
    allergy_discovery_trends: List[Dict[str, Any]] = Field(default_factory=list, description="Trends in allergy discoveries over time, categorized by allergen type")
    allergy_distribution_by_category: List[Dict[str, Any]] = Field(default_factory=list, description="Allergy count distribution by category (food, medication, environment)")

class allergyAdvancedMetrics(BaseModel):
    cross_reactivity_risk_matrix: List[Dict[str, Any]] = Field(default_factory=list, description="Matrix of co-occurring drug allergies")
    allergen_nature_severity_matrix: List[Dict[str, Any]] = Field(default_factory=list, description="Distribution of primary reaction severity across allergen natures")
    age_at_onset_distribution: List[Dict[str, Any]] = Field(default_factory=list, description="Allergy count distribution by patient age group at onset")
    pollen_seasonality_acceleration: List[Dict[str, Any]] = Field(default_factory=list, description="Month-over-month rate of change in environmental allergies")
