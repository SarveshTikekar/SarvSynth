from pydantic import BaseModel, Field
from typing import List, Dict, Tuple

class allergyKPIS(BaseModel):
    total_allergic_population: int = Field(..., description="Total number of patients with atleast one recorded allergy")
    severe_anaphylactic_risk_rate: float = Field(...)
    drug_hypersensitivity_rate: float = Field(..., description="Percentage of patients with allergic reactions due to drug hypersensitivity among all allergic patients")
    poly_allergen_patient_rate: float = Field(..., description="Percentage of patients with 2+ recorded allergies among all allergic patients")
    allergy_risk_stratification: List[Tuple[str, str, int]] = Field(..., description="List of tuples containing (allergen_class, number_of_patients, percentage_of_allergic_population)")

class allergyMetrics(BaseModel):
   top_10_causative_agents: List[Dict[str, int]] = Field(..., description="List of top 10 causative agents with their respective counts")
   severity_distribution: List[Dict[str, int] ] = Field(..., description="Distribution of allergy severities among patients") 
   allergy_discovery_trends: List[Dict[str, int]] = Field(..., description="Trends in allergy discoveries over time, categorized by allergen type")