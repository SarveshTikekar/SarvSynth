from datetime import datetime
import statistics
from workflows.scripts.randomised_state_return import states as US_STATES

def calculate_age(birth_date_str):
    if not birth_date_str:
        return 0.0
    try:
        birth_date = datetime.strptime(birth_date_str, "%Y-%m-%d")
        today = datetime.today()
        delta = today - birth_date
        return delta.days / 365.25
    except Exception:
        return 0.0

def calculate_total_patients(records):
    result = {state: 0 for state in US_STATES}
    for rec in records:
        state = rec.get("geolocated_state")
        if state in result:
            result[state] += 1
    return result

def calculate_active_patient_rate(records):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_active = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_active[state] = 0
        state_total[state] += 1
        if rec.get("death_date") is None:
            state_active[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        active = state_active.get(state, 0)
        if total > 0:
            result[state] = int((active / total) * 100)
        else:
            result[state] = 0
    return result

def calculate_gender_balance_ratio(records):
    result = {state: 0 for state in US_STATES}
    state_m = {}
    state_f = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state:
            continue
        gender = rec.get("gender")
        if gender == "M":
            state_m[state] = state_m.get(state, 0) + 1
        elif gender == "F":
            state_f[state] = state_f.get(state, 0) + 1
            
    for state in US_STATES:
        m = state_m.get(state, 0)
        f = state_f.get(state, 0)
        result[state] = int((m / (f if f > 0 else 1)) * 100)
    return result

def calculate_mean_family_income(records):
    result = {state: 0 for state in US_STATES}
    state_incomes = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state or rec.get("death_date") is not None:
            continue
        income = rec.get("family_income")
        if income is not None:
            if state not in state_incomes:
                state_incomes[state] = []
            state_incomes[state].append(float(income))
            
    for state in US_STATES:
        incomes = state_incomes.get(state, [])
        if incomes:
            result[state] = int(sum(incomes) / len(incomes))
        else:
            result[state] = 0
    return result

def calculate_median_family_income(records):
    result = {state: 0 for state in US_STATES}
    state_incomes = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state or rec.get("death_date") is not None:
            continue
        income = rec.get("family_income")
        if income is not None:
            if state not in state_incomes:
                state_incomes[state] = []
            state_incomes[state].append(float(income))
            
    for state in US_STATES:
        incomes = state_incomes.get(state, [])
        if incomes:
            result[state] = int(statistics.median(incomes))
        else:
            result[state] = 0
    return result

def calculate_avg_patient_age(records):
    result = {state: 0.0 for state in US_STATES}
    state_sums = {}
    state_counts = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state or rec.get("death_date") is not None:
            continue
        birth_date = rec.get("birth_date")
        age = calculate_age(birth_date)
        if state not in state_sums:
            state_sums[state] = 0.0
            state_counts[state] = 0
        state_sums[state] += age
        state_counts[state] += 1
        
    for state in US_STATES:
        if state in state_counts and state_counts[state] > 0:
            result[state] = round(state_sums[state] / state_counts[state], 1)
        else:
            result[state] = 0.0
    return result

def calculate_married_rate(records):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_married = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_married[state] = 0
        state_total[state] += 1
        if rec.get("marital_status") == "M":
            state_married[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        married = state_married.get(state, 0)
        if total > 0:
            result[state] = round((married / total) * 100, 1)
        else:
            result[state] = 0.0
    return result

def calculate_higher_education_rate(records):
    result = {state: 0.0 for state in US_STATES}
    state_total = {}
    state_doc = {}
    for rec in records:
        state = rec.get("geolocated_state")
        if not state:
            continue
        if state not in state_total:
            state_total[state] = 0
            state_doc[state] = 0
        state_total[state] += 1
        doc = rec.get("doctorate")
        if doc and doc != "No doctorate":
            state_doc[state] += 1
            
    for state in US_STATES:
        total = state_total.get(state, 0)
        doc_cnt = state_doc.get(state, 0)
        if total > 0:
            result[state] = round((doc_cnt / total) * 100, 1)
        else:
            result[state] = 0.0
    return result

async def get_state_patient_kpis(kpi_id, supabase):
    try:
        response = await supabase.table("patients").select(
            "geolocated_state, birth_date, death_date, gender, family_income, marital_status, doctorate"
        ).execute()
        records = response.data or []
    except Exception as e:
        print(f"Error querying patients table asynchronously: {e}")
        records = []

    # Initial filter on geolocated_state
    valid_records = [r for r in records if r.get("geolocated_state")]

    # Normalize kpi_id mappings (support standard frontend names and database fields)
    kpi_map = {
        "total_patients": calculate_total_patients,
        "active_patient_rate": calculate_active_patient_rate,
        "gender_balance_ratio": calculate_gender_balance_ratio,
        "mean_family_income": calculate_mean_family_income,
        "median_family_income": calculate_median_family_income,
        "avg_patient_age": calculate_avg_patient_age,
        "married_rate": calculate_married_rate,
        "higher_education_rate": calculate_higher_education_rate
    }
    
    calc_func = kpi_map.get(kpi_id)
    if calc_func:
        vals = calc_func(valid_records)
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
