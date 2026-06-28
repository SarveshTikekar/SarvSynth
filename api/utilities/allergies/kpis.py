import re
from datetime import datetime
from workflows.scripts.randomised_state_return import states as US_STATES

def parse_date(date_str):
    if not date_str:
        return None
    try:
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        return datetime.strptime(date_str, "%Y-%m-%d")
    except Exception:
        return None

def calculate_total_allergic_population(records, *args):
    result = {state: 0 for state in US_STATES}
    state_patients = {state: set() for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        if state in state_patients and uuid:
            state_patients[state].add(uuid)
    for state in US_STATES:
        result[state] = len(state_patients[state])
    return result

def calculate_active_allergy_percentage(records, *args):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_active = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_active[state] = 0
        state_total[state] += 1
        if not rec.get("allergy_cure_date"):
            state_active[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        active = state_active.get(state, 0)
        if total > 0:
            result[state] = round((active / total) * 100, 2)
        else:
            result[state] = 0.0
    return result

def calculate_severe_allergy_incidence_rate(records, *args):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_severe = {}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_severe[state] = 0
        state_total[state] += 1
        if rec.get("primary_symptom_severity") == "SEVERE":
            state_severe[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        severe = state_severe.get(state, 0)
        if total > 0:
            result[state] = round((severe / total) * 100, 2)
        else:
            result[state] = 0.0
    return result

def calculate_allergic_patient_rate(records, patient_records, *args):
    result = {state: 0.0 for state in US_STATES}
    # Total patient count per state
    state_total_patients = {state: 0 for state in US_STATES}
    for p in patient_records:
        state = p.get("geolocated_state")
        if state in state_total_patients:
            state_total_patients[state] += 1
            
    # Total allergic patient count per state
    state_allergic_patients = {state: set() for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        if state in state_allergic_patients and uuid:
            state_allergic_patients[state].add(uuid)
            
    for state in US_STATES:
        total_p = state_total_patients.get(state, 0)
        allergic_p = len(state_allergic_patients[state])
        if total_p > 0:
            result[state] = round((allergic_p / total_p) * 100, 2)
        else:
            result[state] = 0.0
    return result

def calculate_penicillin_allergy_delabeling_eligibility_rate(records, *args):
    result = {state: 0.0 for state in US_STATES}
    state_penicillin_total = {}
    state_penicillin_eligible = {}
    
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        if not state:
            continue
        desc = rec.get("allergy_description") or ""
        if re.search(r"^[pP]enicillin$", desc):
            if state not in state_penicillin_total:
                state_penicillin_total[state] = 0
                state_penicillin_eligible[state] = 0
            state_penicillin_total[state] += 1
            
            # criteria: severity != SEVERE and description != Anaphylaxis
            p_sev = rec.get("primary_symptom_severity")
            s_sev = rec.get("secondary_symptom_severity")
            p_desc = rec.get("primary_symptom_description")
            if p_sev != "SEVERE" and s_sev != "SEVERE" and p_desc != "Anaphylaxis":
                state_penicillin_eligible[state] += 1
                
    for state in US_STATES:
        tot = state_penicillin_total.get(state, 0)
        elig = state_penicillin_eligible.get(state, 0)
        if tot > 0:
            result[state] = round((elig / tot) * 100, 2)
        else:
            result[state] = 0.0
    return result

def calculate_allergy_related_readmission_rate(records, patient_records, encounters_records):
    result = {state: 0.0 for state in US_STATES}
    
    # State-level patient uuids
    state_allergic_patients = {state: set() for state in US_STATES}
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        if state in state_allergic_patients and uuid:
            state_allergic_patients[state].add(uuid)
            
    # For each state, analyze encounters for allergic patients
    for state in US_STATES:
        allergic_uuids = state_allergic_patients[state]
        if not allergic_uuids:
            result[state] = 0.0
            continue
            
        # Filter encounters of allergic patients in this state
        patient_visits = {} # uuid -> list of date objects
        for enc in encounters_records:
            uuid = enc.get("uuid")
            if uuid in allergic_uuids:
                visit_date = parse_date(enc.get("visit_start"))
                if visit_date:
                    if uuid not in patient_visits:
                        patient_visits[uuid] = []
                    patient_visits[uuid].append(visit_date)
                    
        readmit_count = 0
        for uuid, visits in patient_visits.items():
            if len(visits) >= 2:
                visits.sort()
                readmitted = False
                for i in range(1, len(visits)):
                    diff_days = (visits[i] - visits[i-1]).days
                    if 0 < diff_days <= 30:
                        readmitted = True
                        break
                if readmitted:
                    readmit_count += 1
                    
        total_allergic = len(allergic_uuids)
        if total_allergic > 0:
            result[state] = round((readmit_count / total_allergic) * 100, 2)
        else:
            result[state] = 0.0
            
    return result

def calculate_poly_allergen_patient_rate(records, *args):
    result = {state: 0.0 for state in US_STATES}
    state_patients = {state: {} for state in US_STATES} # state -> {uuid -> set(allergy_codes)}
    
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        code = rec.get("allergy_code")
        if state in state_patients and uuid and code:
            if uuid not in state_patients[state]:
                state_patients[state][uuid] = set()
            state_patients[state][uuid].add(code)
            
    for state in US_STATES:
        patient_allergies = state_patients[state]
        if not patient_allergies:
            result[state] = 0.0
            continue
            
        poly_count = sum(1 for codes in patient_allergies.values() if len(codes) >= 2)
        total_allergic = len(patient_allergies)
        if total_allergic > 0:
            result[state] = round((poly_count / total_allergic) * 100, 2)
        else:
            result[state] = 0.0
    return result

def calculate_drug_hypersensitivity_rate(records, *args):
    result = {state: 0.0 for state in US_STATES}
    state_patients = {state: set() for state in US_STATES}
    state_drug_patients = {state: set() for state in US_STATES}
    
    for rec in records:
        state = rec.get("patients", {}).get("geolocated_state")
        uuid = rec.get("uuid")
        if not state or not uuid:
            continue
        if state in state_patients:
            state_patients[state].add(uuid)
            category = rec.get("category")
            nature = rec.get("allergen_nature")
            if category == "medication" or nature == "drug":
                state_drug_patients[state].add(uuid)
                
    for state in US_STATES:
        tot = len(state_patients[state])
        drug_cnt = len(state_drug_patients[state])
        if tot > 0:
            result[state] = round((drug_cnt / tot) * 100, 2)
        else:
            result[state] = 0.0
    return result

async def get_state_allergy_kpis(kpi_id, supabase):
    try:
        response = await supabase.table("allergies").select(
            "allergy_cure_date, primary_symptom_severity, secondary_symptom_severity, primary_symptom_description, allergy_description, allergy_code, category, allergen_nature, uuid, patients(geolocated_state)"
        ).execute()
        records = response.data or []
    except Exception as e:
        print(f"Error querying allergies table asynchronously: {e}")
        records = []

    # Initial filter on geolocated_state
    valid_records = [r for r in records if r.get("patients") and r.get("patients", {}).get("geolocated_state")]

    # Fetch patients for total patient counts or rates if needed
    patient_records = []
    if kpi_id in ["allergic_patient_rate", "allergy_related_readmission_rate"]:
        try:
            p_resp = await supabase.table("patients").select("uuid, geolocated_state").execute()
            patient_records = p_resp.data or []
        except Exception as e:
            print(f"Error querying patients table: {e}")

    # Fetch encounters for readmission rate
    encounters_records = []
    if kpi_id == "allergy_related_readmission_rate":
        try:
            e_resp = await supabase.table("encounters").select("uuid, visit_start").execute()
            encounters_records = e_resp.data or []
        except Exception as e:
            print(f"Error querying encounters table: {e}")

    kpi_map = {
        "total_allergic_population": calculate_total_allergic_population,
        "active_allergy_percentage": calculate_active_allergy_percentage,
        "active_allergy_rate": calculate_active_allergy_percentage,
        "severe_allergy_incidence_rate": calculate_severe_allergy_incidence_rate,
        "severe_incident_rate": calculate_severe_allergy_incidence_rate,
        "allergic_patient_rate": calculate_allergic_patient_rate,
        "penicillin_allergy_delabeling_eligibility_rate": calculate_penicillin_allergy_delabeling_eligibility_rate,
        "allergy_related_readmission_rate": calculate_allergy_related_readmission_rate,
        "poly_allergen_patient_rate": calculate_poly_allergen_patient_rate,
        "drug_hypersensitivity_rate": calculate_drug_hypersensitivity_rate
    }

    calc_func = kpi_map.get(kpi_id)
    if calc_func:
        vals = calc_func(valid_records, patient_records, encounters_records)
    else:
        vals = {state: 0 for state in US_STATES}

    output = {}
    for state in US_STATES:
        output[state] = {
            "value": vals.get(state, 0),
            "trend": 0.0,
            "is_simulated": False
        }
    return output
