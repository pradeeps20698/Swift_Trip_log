import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import json
import psycopg2
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import os

# Refresh intervals (in seconds)
REFRESH_10_MIN = 600
REFRESH_15_MIN = 900
REFRESH_20_MIN = 1200

# Page configuration
st.set_page_config(
    page_title="Swift Trip Log Dashboard",
    page_icon="🚚",
    layout="wide",
    initial_sidebar_state="expanded"
)

# No full page auto-refresh - using @st.fragment with run_every for each section

# Custom CSS
st.markdown("""
<style>
    /* Hide the blur/dim effect when app is rerunning */
    [data-testid="stAppViewBlockContainer"] {
        opacity: 1 !important;
    }
    .stApp > div:first-child {
        opacity: 1 !important;
    }
    [data-testid="stStatusWidget"] {
        display: none !important;
    }
    .stSpinner {
        display: none !important;
    }

    .metric-card-blue {
        background: linear-gradient(135deg, #1e3a5f 0%, #2d5a87 100%);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-card-red {
        background: linear-gradient(135deg, #8b2635 0%, #c73e4d 100%);
        padding: 1.5rem;
        border-radius: 10px;
        text-align: center;
        margin: 0.5rem 0;
    }
    .metric-label {
        font-size: 0.9rem;
        color: #b0b0b0;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #ffffff;
    }
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    .stTabs [data-baseweb="tab"] {
        background-color: rgba(28, 131, 225, 0.1);
        border-radius: 4px;
        padding: 8px 16px;
        color: #FFFFFF;
    }
    .stTabs [aria-selected="true"] {
        background-color: rgba(28, 131, 225, 0.4);
    }
</style>
""", unsafe_allow_html=True)


def get_db_connection():
    """Create database connection"""
    try:
        conn = psycopg2.connect(
            host=st.secrets["Host"],
            database=st.secrets["database_name"],
            user=st.secrets["UserName"],
            password=st.secrets["Password"],
            port=st.secrets["Port"],
            connect_timeout=10
        )
        return conn
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes for fresher data
def load_triplog_data():
    """Load trip log data directly from PostgreSQL database"""
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()

        query = """
            SELECT * FROM swift_trip_log
            WHERE loading_date IS NOT NULL
              AND loading_date::date <= CURRENT_DATE
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Rename columns to match dashboard expected format
        column_mapping = {
            'loading_date': 'LoadingDate',
            'freight': 'Freight',
            'car_qty': 'CarQty',
            'vehicle_no': 'VehicleNo',
            'driver_name': 'DriverName',
            'route': 'Route',
            'trip_status': 'TripStatus',
            'new_party_name': 'NewPartyName',
            'party': 'Party',
            'office': 'Office',
            'lr_date': 'LRDate',
            'lr_nos': 'LRNos',
            'lr_freight': 'LRFreight',
            'material': 'Material',
            'distance': 'Distance',
            'settlement_date': 'SettlementDate',
            'settlement_no': 'SettlementNo',
            'unloading_date': 'UnloadingDate',
            'expected_date': 'ExpectedDate',
            'reporting_date': 'ReportingDate',
            'created_date': 'CreatedDate',
            'created_at': 'CreatedAt',
            'created_by': 'CreatedBy',
            'id': 'ID',
            'tlhs_no': 'TLHSNo',
            'driver_code': 'DriverCode',
            'second_driver_name': 'SecondDriverName',
            'second_driver_code': 'SecondDriverCode',
            'driver_phone_no': 'DriverPhoneNo',
            'guarantor': 'Guarantor',
            'onward_route': 'OnwardRoute',
            'tl_cash_advance': 'TLCashAdvance',
            'tl_diesel_advance': 'TLDieselAdvance',
            'e_toll': 'EToll',
            'fuel_qty': 'FuelQty',
            'fuel_qty_budget': 'FuelQtyBudget',
            'onward_trip_fuel_budget_qty': 'OnwardTripFuelBudgetQty',
            'carry_forward_fuel_qty': 'CarryForwardFuelQty',
            'required_fuel_qty': 'RequiredFuelQty',
            'trip_exp_budget': 'TripExpBudget',
            'report_unloading_date': 'ReportUnloadingDate'
        }
        df = df.rename(columns=column_mapping)
        return df
    except Exception as e:
        st.error(f"Error loading triplog data from database: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_vendor_data():
    """Load vendor data from cn_data table (where tl_no is NULL = vendor trips)"""
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()

        query = """
            SELECT billing_party, cn_date, qty, basic_freight, route, origin, vehicle_no
            FROM cn_data
            WHERE ((billing_party = 'R.sai Logistics India Pvt. Ltd.' AND (tl_no IS NULL OR tl_no = ''))
               OR (billing_party != 'R.sai Logistics India Pvt. Ltd.' AND vehicle_type = 'Hire Vehicle'))
               AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
               AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
               AND (is_active = true OR is_active = 'Yes')
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Rename columns
        df = df.rename(columns={
            'billing_party': 'BillingParty',
            'cn_date': 'CNDate',
            'qty': 'CarQty',
            'basic_freight': 'Freight',
            'route': 'Route',
            'origin': 'Origin',
            'vehicle_no': 'VehicleNo'
        })

        # Convert date
        df['CNDate'] = pd.to_datetime(df['CNDate'], errors='coerce')
        df['CNDateOnly'] = df['CNDate'].dt.date

        return df
    except Exception as e:
        st.error(f"Error loading vendor data from database: {e}")
        return pd.DataFrame()


def load_cn_data():
    """Load ALL cn_data for use in fragments (cn_aging, unbilled_cn, etc.)"""
    try:
        conn = get_db_connection()
        if conn is None:
            return pd.DataFrame()

        # Load all cn_data with columns needed by various fragments
        query = """
            SELECT cn_no, cn_date, billing_party, origin, route, vehicle_no,
                   qty, basic_freight, tl_no, branch, bill_no, pod_receipt_no,
                   eta, vehicle_type
            FROM cn_data
            WHERE (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
              AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
              AND (is_active = true OR is_active = 'Yes')
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        # Convert date columns
        df['cn_date'] = pd.to_datetime(df['cn_date'], errors='coerce')

        return df
    except Exception as e:
        st.error(f"Error loading cn_data from database: {e}")
        return pd.DataFrame()


@st.cache_data(ttl=3600)  # Cache for 60 minutes - auto refresh when vehicles change
def load_vehicles_by_type(vehicle_type):
    """Load vehicle numbers from swift_vehicles by vehicle_type"""
    try:
        conn = get_db_connection()
        if conn is None:
            return []

        query = f"""
            SELECT vehicle_no FROM swift_vehicles
            WHERE vehicle_type = '{vehicle_type}' AND (is_active = true OR is_active = 'Yes' OR is_active = 'Y')
        """
        df = pd.read_sql_query(query, conn)
        conn.close()

        if df.empty:
            return []
        return df['vehicle_no'].tolist()
    except Exception as e:
        st.error(f"Error loading vehicles by type: {e}")
        return []


def get_vendor_client_mapping(billing_party, origin=None):
    """Map vendor billing_party to client display name for summary"""
    if pd.isna(billing_party) or billing_party == "":
        return None

    # Special handling for Mahindra - split by origin
    if billing_party == 'MAHINDRA LOGISTICS LTD.':
        origin_str = str(origin).strip() if origin and not pd.isna(origin) else ''
        origin_upper = origin_str.upper()
        if 'CHAKAN' in origin_upper:
            return 'Mahindra Logistics Ltd - Chakan'
        elif 'NASHIK' in origin_upper:
            return 'Mahindra Logistics Ltd - Nashik'
        elif 'HARIDWAR' in origin_upper:
            return 'Mahindra Logistics Ltd - Haridwar'
        elif 'ZAHEERABAD' in origin_upper:
            return 'Mahindra Logistics Ltd - Zaheerabad'
        else:
            # Chennai, Pune, Jeypore, etc. go to main MAHINDRA LOGISTICS LTD
            return 'MAHINDRA LOGISTICS LTD'

    # Direct mappings: billing_party -> DisplayParty name for dashboard
    # Map to same name to show as separate row, or map to existing client name to merge
    vendor_mappings = {
        # R.sai uses tl_no IS NULL logic
        'R.sai Logistics India Pvt. Ltd.': 'R.sai Logistics India Pvt. Ltd.',
        # All below use vehicle_type = 'Hire Vehicle' logic
        'Kwick Living Private Limited': 'Kwick Living Private Limited',
        'SKODA AUTO VolkswagenIndia Pvt. Ltd - Pune': 'SKODA AUTO VolkswagenIndia Pvt. Ltd - Pune',
        'Glovis India Pvt Ltd - KIA': 'Glovis India Pvt Ltd - KIA',
        'Glovis India Pvt Ltd - Hyundai': 'Glovis India Pvt Ltd - Hyundai',
        'Honda Cars India Ltd - Tapukera': 'Honda Cars India Ltd - Tapukera',
        'Honda Cars India Ltd - Noida': 'Honda Noida',
        'Tata Motors Passenger Vehicles Limited - Pune': 'Tata Motors Pvt Ltd - Pune',
        'Tata Motors Passenger Vehicles Limited - Sanand': 'Tata Motors Pvt Ltd - Sanand',
        'Tata Passenger Electric Mobility Limited - Pune': 'Tata Motors Pvt Ltd - Pune',
        'JSW MG Motor India Private Limited': 'JSW MG Motor India Private Limited',
        'VALUEDRIVE TECHNOLOGIES PRIVATE LIMITED(SPINNY) BLR': 'VALUEDRIVE TECHNOLOGIES PRIVATE LIMITED(SPINNY)',
        'M/S Mohan Logistics Private  Limited': 'M/S Mohan Logistics Private Limited',
        'SAI AUTO COMPONENTS PVT.LTD': 'SAI AUTO COMPONENTS PVT.LTD',
        'John Deere india Private Limited': 'John Deere India Private Limited',
        'Glovis India Pvt Ltd - Pune': 'Glovis India Pvt Ltd - Pune',
        'shiv ansh logistics': 'Market Load',
        'Delhi Hubli Cargo Logistics Pvt. Ltd.': 'Market Load',
    }

    # If billing_party matches a mapping, return it; otherwise default to Market Load
    return vendor_mappings.get(billing_party, 'Market Load')


def load_targets():
    """Load target SOB data from database"""
    try:
        conn = get_db_connection()
        if conn is None:
            return {}
        cursor = conn.cursor()
        cursor.execute("SELECT party_name, target_value FROM sob_targets")
        rows = cursor.fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        return {}


def save_target(party_name, target_value):
    """Save single target SOB to database"""
    try:
        conn = get_db_connection()
        if conn is None:
            st.error("Database connection failed")
            return False
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sob_targets (party_name, target_value, updated_at)
            VALUES (%s, %s, CURRENT_TIMESTAMP)
            ON CONFLICT (party_name) DO UPDATE SET target_value = %s, updated_at = CURRENT_TIMESTAMP
        """, (party_name, target_value, target_value))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error saving target: {e}")
        return False


def load_excluded_trips():
    """Load excluded trip numbers from database"""
    try:
        conn = get_db_connection()
        if conn is None:
            return []
        cursor = conn.cursor()
        cursor.execute("SELECT trip_no FROM excluded_pending_trips ORDER BY excluded_at DESC")
        rows = cursor.fetchall()
        conn.close()
        return [row[0] for row in rows]
    except Exception as e:
        return []


def add_excluded_trip(trip_no):
    """Add a trip to the exclusion list"""
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO excluded_pending_trips (trip_no)
            VALUES (%s)
            ON CONFLICT (trip_no) DO NOTHING
        """, (trip_no.strip(),))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error adding excluded trip: {e}")
        return False


def remove_excluded_trip(trip_no):
    """Remove a trip from the exclusion list"""
    try:
        conn = get_db_connection()
        if conn is None:
            return False
        cursor = conn.cursor()
        cursor.execute("DELETE FROM excluded_pending_trips WHERE trip_no = %s", (trip_no,))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        st.error(f"Error removing excluded trip: {e}")
        return False


def normalize_party_name(party_name):
    """Map party name variations to a single standardized name for display"""
    if pd.isna(party_name) or party_name == "":
        return party_name

    party_upper = str(party_name).upper().strip()

    # Note: NISAN and BMW mappings are now handled in database trigger
    # They will show as separate entries: "Mahindra Logistics Ltd - NISAN" and "Mahindra Logistics Ltd - BMW"

    # Market Load mappings
    market_load_parties = [
        "ALL INDIA TRPT.",
        "BALAJI CARGO CARRIER (INDIA)",
        "DOOR TO DOOR RELATIONS LOGISTICS",
        "GOLDEN TRANSPORT",
        "M.Y. TRANSPORT COMPANY PRIVATE LIMITED",
        "RAMESH CAR CARRIER",
        "SHRI OM LOGISTICS",
        "YATI TRANS LOGISTICS LLP"
    ]
    if party_upper in [p.upper() for p in market_load_parties]:
        return "Market Load"

    # VALUEDRIVE mappings
    if party_upper == "VALUEDRIVE TECHNOLOGIES PRIVATE LIMITED(SPINNY) UP":
        return "VALUEDRIVE TECHNOLOGIES PRIVATE LIMITED(SPINNY)"

    # John Deere mappings
    if "JOHN DEERE" in party_upper:
        return "John Deere India Private Limited"

    return party_name


def get_client_category(party_name):
    """Categorize clients into groups based on NewPartyName"""
    if pd.isna(party_name) or party_name == "":
        return "Other"
    party_upper = str(party_name).upper()
    if "HONDA" in party_upper or "TAPUKERA" in party_upper:
        return "Honda"
    elif "MAHINDRA" in party_upper or "M & M" in party_upper or "M&M" in party_upper or "MSTC" in party_upper or "TRAIN LOAD" in party_upper or "NISAN" in party_upper:
        return "M & M"
    elif "TOYOTA" in party_upper or "TRANSYSTEM" in party_upper or "DC MOVEMENT" in party_upper:
        return "Toyota"
    elif "SKODA" in party_upper or "VOLKSWAGEN" in party_upper:
        return "Skoda"
    elif "GLOVIS" in party_upper:
        return "Glovis"
    elif "TATA" in party_upper:
        return "Tata"
    elif "JOHN DEERE" in party_upper:
        return "John Deere"
    elif "MARKET LOAD" in party_upper:
        return "Market Load"
    elif "VALUEDRIVE" in party_upper or "SPINNY" in party_upper:
        return "Spinny"
    elif "JSW" in party_upper or "MG MOTOR" in party_upper:
        return "JSW MG"
    elif "R.SAI" in party_upper or "RSAI" in party_upper:
        return "R.sai"
    elif "MOHAN LOGISTICS" in party_upper:
        return "Mohan Logistics"
    elif "SAI AUTO" in party_upper:
        return "SAI Auto"
    elif "KWICK" in party_upper:
        return "Kwick"
    else:
        return "Other"


def load_and_process_data():
    """Load and process all data from database"""
    df = load_triplog_data()
    vendor_df = load_vendor_data()

    if df.empty:
        return df, vendor_df

    # Convert date columns
    df['LoadingDate'] = pd.to_datetime(df['LoadingDate'], errors='coerce')
    df['CarQty'] = pd.to_numeric(df['CarQty'], errors='coerce').fillna(0)
    df['Freight'] = pd.to_numeric(df['Freight'], errors='coerce').fillna(0)
    df['LRFreight'] = pd.to_numeric(df['LRFreight'], errors='coerce').fillna(0)
    df['Distance'] = pd.to_numeric(df['Distance'], errors='coerce').fillna(0)

    # Use NewPartyName if available, otherwise fall back to Party
    if 'NewPartyName' in df.columns:
        df['DisplayParty'] = df['NewPartyName'].fillna(df['Party'])
    else:
        df['DisplayParty'] = df['Party']

    # Override DisplayParty for John Deere (Party column has correct name but NewPartyName is Market Load)
    if 'Party' in df.columns:
        john_deere_mask = df['Party'].str.contains('John Deere', case=False, na=False)
        df.loc[john_deere_mask, 'DisplayParty'] = 'John Deere India Private Limited'

    # Normalize party names (merge variations into single name)
    df['DisplayParty'] = df['DisplayParty'].apply(normalize_party_name)

    # Add client category based on DisplayParty
    df['category'] = df['DisplayParty'].apply(get_client_category)

    # Pre-compute LoadingDateOnly for faster filtering
    df['LoadingDateOnly'] = df['LoadingDate'].dt.date

    return df, vendor_df


def refresh_session_data():
    """Refresh session_state data from database - only if 10 minutes have passed"""
    # Only refresh if 10 minutes have passed since last refresh
    if 'last_data_refresh' in st.session_state:
        time_since_refresh = (datetime.now() - st.session_state.last_data_refresh).total_seconds()
        if time_since_refresh < 600:  # Less than 10 minutes - use stored data
            return st.session_state.df, st.session_state.vendor_df, st.session_state.cn_data, st.session_state.targets

    # 10 minutes passed OR first load - refresh from database
    df, vendor_df = load_and_process_data()
    cn_data = load_cn_data()
    st.session_state.df = df
    st.session_state.vendor_df = vendor_df
    st.session_state.cn_data = cn_data
    st.session_state.targets = load_targets()
    st.session_state.last_data_refresh = datetime.now()
    return df, vendor_df, cn_data, st.session_state.targets


def main():
    # Header
    st.markdown("<h1 style='text-align: center;'>🚚 Swift Trip Log Dashboard</h1>", unsafe_allow_html=True)

    # ========== AUTO-REFRESH EVERY 10 MINUTES ==========
    # Check if 10 minutes have passed since last data load
    if 'last_data_refresh' in st.session_state:
        time_since_refresh = (datetime.now() - st.session_state.last_data_refresh).total_seconds()
        if time_since_refresh > 600:  # 600 seconds = 10 minutes
            # Clear stored data to trigger reload
            for key in ['df', 'vendor_df', 'cn_data', 'targets']:
                if key in st.session_state:
                    del st.session_state[key]

    # ========== STORE ALL DATA IN SESSION_STATE ==========
    if 'df' not in st.session_state:
        with st.spinner("Loading data from database..."):
            df, vendor_df = load_and_process_data()
            cn_data = load_cn_data()
            st.session_state.df = df
            st.session_state.vendor_df = vendor_df
            st.session_state.cn_data = cn_data
            st.session_state.targets = load_targets()
            st.session_state.last_data_refresh = datetime.now()  # Track refresh time

    # Use data from session_state - INSTANT, no database call
    df = st.session_state.df
    vendor_df = st.session_state.vendor_df
    cn_data = st.session_state.cn_data
    targets = st.session_state.targets

    if df.empty:
        st.error("No data available. Please check the database connection.")
        return

    # Sidebar Filters
    st.sidebar.header("Filters")

    # Select Month
    st.sidebar.subheader("Select Month")
    # Filter valid dates: not null, not future dates, from Apr'25 onwards
    valid_dates = df['LoadingDate'].dropna()
    valid_dates = valid_dates[valid_dates <= pd.Timestamp.now()]
    valid_dates = valid_dates[valid_dates >= pd.Timestamp('2025-04-01')]  # Only show from Apr'25
    available_months = valid_dates.dt.to_period('M').unique()
    available_months = sorted([str(m) for m in available_months], reverse=True)

    if available_months:
        month_options = {pd.to_datetime(m).strftime("%b'%y"): m for m in available_months}
        selected_month_display = st.sidebar.selectbox("Month", list(month_options.keys()))
        selected_month_str = month_options[selected_month_display]
        selected_month = pd.to_datetime(selected_month_str)
    else:
        selected_month = datetime.now()

    # Filter by Party
    st.sidebar.subheader("Filter by Party")
    all_parties = ['All'] + sorted(df['DisplayParty'].dropna().unique().tolist())
    selected_party = st.sidebar.selectbox("Party", all_parties)

    # Refresh button - clears session_state to reload from database
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
        # Clear stored data so it reloads from database
        for key in ['df', 'vendor_df', 'cn_data', 'targets', 'last_data_refresh']:
            if key in st.session_state:
                del st.session_state[key]
        st.rerun()

    # Target SOB Update Section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Update Target SOB")

    with st.sidebar.expander("Set Target SOB", expanded=False):
        # Get unique parties for target setting
        unique_parties = sorted(df['DisplayParty'].dropna().unique().tolist())
        unique_parties = [p for p in unique_parties if p != '']

        # Select party to update
        target_party = st.selectbox("Select Party", [''] + unique_parties, key='target_party')

        if target_party:
            current_target = targets.get(target_party, 0)
            new_target = st.number_input("Target SOB", min_value=0, value=int(current_target), key='new_target')

            if st.button("💾 Save Target"):
                if save_target(target_party, new_target):
                    st.success(f"Target saved for {target_party}: {new_target}")
                    st.cache_data.clear()
                    # Reload targets in session_state
                    st.session_state.targets = load_targets()
                    st.rerun()

        # Show current targets
        st.markdown("**Current Targets:**")
        if targets:
            for party, target in sorted(targets.items()):
                st.text(f"{party[:20]}: {target}")
        else:
            st.text("No targets set")

    # Exclude Trips from Pending CN Section
    st.sidebar.markdown("---")
    st.sidebar.subheader("Pending CN Exclusions")

    with st.sidebar.expander("Manage Exclusions", expanded=False):
        # Add new exclusion
        new_trip_no = st.text_input("Trip No to Exclude", placeholder="e.g., T-196375", key='exclude_trip_input')

        if st.button("➕ Add Exclusion"):
            if new_trip_no:
                if add_excluded_trip(new_trip_no):
                    st.success(f"Added: {new_trip_no}")
                    st.cache_data.clear()
                    st.rerun()
            else:
                st.warning("Enter a trip number")

        # Show current exclusions
        st.markdown("**Excluded Trips:**")
        excluded_trips = load_excluded_trips()
        if excluded_trips:
            for trip in excluded_trips:
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.text(trip)
                with col2:
                    if st.button("❌", key=f"remove_{trip}"):
                        if remove_excluded_trip(trip):
                            st.cache_data.clear()
                            st.rerun()
        else:
            st.text("No exclusions")

    # Filter data for selected month
    month_start = selected_month.replace(day=1)
    month_end = (month_start + relativedelta(months=1)) - timedelta(days=1)

    # Full month data for summary boxes (all available data for the month)
    month_df = df[(df['LoadingDateOnly'] >= month_start.date()) & (df['LoadingDateOnly'] <= month_end.date())]

    if selected_party != 'All':
        month_df = month_df[month_df['DisplayParty'] == selected_party]

    # Month Summary Section (Full Month - Live Data) - Auto refresh every 10 minutes
    @st.fragment(run_every=REFRESH_10_MIN)
    def month_summary_fragment():
        # Auto-refresh: reload data from database and update session_state
        refresh_session_data()
        frag_df = st.session_state.df
        frag_vendor_df = st.session_state.vendor_df

        # Re-filter for selected month and party
        frag_month_df = frag_df[(frag_df['LoadingDateOnly'] >= month_start.date()) & (frag_df['LoadingDateOnly'] <= month_end.date())]
        if selected_party != 'All':
            frag_month_df = frag_month_df[frag_month_df['DisplayParty'] == selected_party]

        st.markdown(f"### Month Summary ({selected_month.strftime('%B %Y')})")

        # Calculate Own metrics
        loaded_trips = len(frag_month_df[(frag_month_df['TripStatus'] != 'Empty') & (frag_month_df['DisplayParty'] != '') & (frag_month_df['DisplayParty'].notna())])
        empty_trips = len(frag_month_df[(frag_month_df['TripStatus'] == 'Empty') | (frag_month_df['DisplayParty'] == '') | (frag_month_df['DisplayParty'].isna())])
        own_cars = int(frag_month_df['CarQty'].sum())
        own_freight = frag_month_df['Freight'].sum()

        # Calculate Vendor metrics for full month
        vendor_cars = 0
        vendor_freight = 0.0
        vendor_trips = 0
        if not frag_vendor_df.empty:
            vendor_month_df = frag_vendor_df[
                (frag_vendor_df['CNDate'] >= pd.Timestamp(month_start.date())) &
                (frag_vendor_df['CNDate'] < pd.Timestamp(month_end.date()) + pd.Timedelta(days=1))
            ].copy()
            if not vendor_month_df.empty:
                # Apply vendor mapping to filter only mapped vendors
                vendor_month_df['MappedParty'] = vendor_month_df.apply(
                    lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                )
                vendor_mapped = vendor_month_df[vendor_month_df['MappedParty'].notna()]
                vendor_cars = int(vendor_mapped['CarQty'].sum())
                vendor_freight = vendor_mapped['Freight'].sum()
                # Vendor trips = unique combinations of date + vehicle
                if not vendor_mapped.empty:
                    vendor_mapped['CNDateOnly'] = vendor_mapped['CNDate'].dt.date
                    vendor_trips = vendor_mapped.groupby(['CNDateOnly', 'VehicleNo']).ngroups

        # Total (Own + Vendor)
        total_cars = own_cars + vendor_cars
        total_trips = loaded_trips + vendor_trips
        total_freight = own_freight + vendor_freight
        total_freight_lakhs = total_freight / 100000

        # Display metrics in cards
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown(f"""
            <div class="metric-card-blue">
                <div class="metric-label">Loaded Trips</div>
                <div class="metric-value">{total_trips:,}</div>
                <div style="display: flex; gap: 8px; margin-top: 10px;">
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #60a5fa; font-size: 11px;">Own</div>
                        <div style="color: white; font-size: 16px; font-weight: bold;">{loaded_trips:,}</div>
                    </div>
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #fbbf24; font-size: 11px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 16px; font-weight: bold;">{vendor_trips:,}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col2:
            st.markdown(f"""
            <div class="metric-card-blue">
                <div class="metric-label">Empty Trips</div>
                <div class="metric-value">{empty_trips:,}</div>
                <div style="display: flex; gap: 8px; margin-top: 10px;">
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #60a5fa; font-size: 11px;">Own</div>
                        <div style="color: white; font-size: 16px; font-weight: bold;">{empty_trips:,}</div>
                    </div>
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #fbbf24; font-size: 11px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 16px; font-weight: bold;">0</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col3:
            st.markdown(f"""
            <div class="metric-card-blue">
                <div class="metric-label">Cars Lifted</div>
                <div class="metric-value">{total_cars:,}</div>
                <div style="display: flex; gap: 8px; margin-top: 10px;">
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #60a5fa; font-size: 11px;">Own</div>
                        <div style="color: white; font-size: 16px; font-weight: bold;">{own_cars:,}</div>
                    </div>
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #fbbf24; font-size: 11px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 16px; font-weight: bold;">{vendor_cars:,}</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

        with col4:
            st.markdown(f"""
            <div class="metric-card-red">
                <div class="metric-label">Total Freight</div>
                <div class="metric-value">₹{total_freight_lakhs:.2f}L</div>
                <div style="display: flex; gap: 8px; margin-top: 10px;">
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #60a5fa; font-size: 11px;">Own</div>
                        <div style="color: white; font-size: 16px; font-weight: bold;">₹{own_freight/100000:.2f}L</div>
                    </div>
                    <div style="flex: 1; background: rgba(0,0,0,0.2); padding: 6px; border-radius: 4px; text-align: center;">
                        <div style="color: #fbbf24; font-size: 11px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 16px; font-weight: bold;">₹{vendor_freight/100000:.2f}L</div>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

    # Call the month summary fragment
    month_summary_fragment()

    # Initialize session state for tab selection
    if 'selected_tab' not in st.session_state:
        st.session_state.selected_tab = 0

    # Tabs with key to maintain state
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["📊 Target vs Actual", "📅 Daily Loading Details", "🚚 Local/Pilot Loads", "🗺️ Zone View", "🔄 NSK/Ckn Round Trips", "💰 Trip Profitability", "📋 Pending CN - Triplogs"])

    with tab1:
        # Target vs Actual - Client-Wise Summary
        st.markdown("### Target vs Actual - Client-Wise Summary")

        @st.fragment(run_every=REFRESH_10_MIN)
        def target_vs_actual_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()
            frag_df = st.session_state.df
            frag_vendor_df = st.session_state.vendor_df

            month_display = selected_month.strftime("%b'%y")
            st.markdown(f"**Selected Month:** {month_display}")

            # Date selectors
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                select_month_start = st.date_input("Select Month", month_start.date())
            with col2:
                # Default to D-1 (yesterday)
                yesterday = (datetime.now() - timedelta(days=1)).date()
                till_date = st.date_input("Till Date", min(yesterday, month_end.date()))
            with col3:
                compare_month = st.date_input("Compare With Month", datetime(2025, 12, 1).date())
            with col4:
                compare_till_date = st.date_input("Compare Till Date", (pd.to_datetime(compare_month) + timedelta(days=till_date.day - 1)).date())

            # Filter data for current period (using date only for accurate comparison)
            current_df = frag_df[
                (frag_df['LoadingDateOnly'] >= select_month_start) &
                (frag_df['LoadingDateOnly'] <= till_date) &
                (frag_df['DisplayParty'] != '') &
                (frag_df['DisplayParty'].notna())
            ]

            # Filter data for comparison period
            compare_df = frag_df[
                (frag_df['LoadingDateOnly'] >= compare_month) &
                (frag_df['LoadingDateOnly'] <= compare_till_date) &
                (frag_df['DisplayParty'] != '') &
                (frag_df['DisplayParty'].notna())
            ]

            till_date_display = till_date.strftime("%dth %b'%y")
            st.markdown(f"#### Till {till_date_display} 📊")

            # Create client-wise summary (Own data from swift_trip_log)
            summary = current_df.groupby('DisplayParty').agg({
                'TLHSNo': 'count',
                'CarQty': 'sum',
                'Freight': 'sum'
            }).reset_index()
            summary.columns = ['Party', 'Trips', 'Own_Cars', 'Own_Freight']
            summary['category'] = summary['Party'].apply(get_client_category)

            # Process vendor data from cn_data
            summary['Vendor_Cars'] = 0
            summary['Vendor_Freight'] = 0.0

            if not frag_vendor_df.empty:
                # Filter vendor data for current period (convert to Timestamp for consistent comparison)
                vendor_current = frag_vendor_df[
                    (frag_vendor_df['CNDate'] >= pd.Timestamp(select_month_start)) &
                    (frag_vendor_df['CNDate'] < pd.Timestamp(till_date) + pd.Timedelta(days=1))
                ].copy()

                if not vendor_current.empty:
                    # Map vendor billing_party to client display name (with origin for Mahindra)
                    vendor_current['MappedParty'] = vendor_current.apply(
                        lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                    )

                    # Filter only mapped vendors and group
                    vendor_mapped = vendor_current[vendor_current['MappedParty'].notna()]
                    if not vendor_mapped.empty:
                        vendor_summary = vendor_mapped.groupby('MappedParty').agg({
                            'CarQty': 'sum',
                            'Freight': 'sum'
                        }).reset_index()
                        vendor_summary.columns = ['Party', 'Vendor_Cars', 'Vendor_Freight']

                        # Merge vendor data with summary (outer join to include vendor-only parties)
                        summary = summary.merge(vendor_summary, on='Party', how='outer', suffixes=('', '_v'))

                        # Fill missing values for vendor-only parties
                        summary['Trips'] = summary['Trips'].fillna(0)
                        summary['Own_Cars'] = summary['Own_Cars'].fillna(0)
                        summary['Own_Freight'] = summary['Own_Freight'].fillna(0)

                        # Merge vendor columns
                        summary['Vendor_Cars'] = summary['Vendor_Cars_v'].fillna(summary['Vendor_Cars']).fillna(0)
                        summary['Vendor_Freight'] = summary['Vendor_Freight_v'].fillna(summary['Vendor_Freight']).fillna(0)
                        summary = summary.drop(columns=['Vendor_Cars_v', 'Vendor_Freight_v'], errors='ignore')

                        # Add category for new vendor-only parties
                        summary['category'] = summary.apply(
                            lambda row: get_client_category(row['Party']) if pd.isna(row.get('category')) else row['category'],
                            axis=1
                        )

            # Calculate totals
            summary['Total_Cars'] = summary['Own_Cars'] + summary['Vendor_Cars']
            summary['Total_Freight'] = summary['Own_Freight'] + summary['Vendor_Freight']

            # Add comparison data
            if not compare_df.empty:
                compare_summary = compare_df.groupby('DisplayParty').agg({
                    'CarQty': 'sum',
                    'Freight': 'sum'
                }).reset_index()
                compare_summary.columns = ['Party', 'Compare_Cars', 'Compare_Freight']
                summary = summary.merge(compare_summary, on='Party', how='left')
                summary['Compare_Cars'] = summary['Compare_Cars'].fillna(0)
                summary['Compare_Freight'] = summary['Compare_Freight'].fillna(0)
            else:
                summary['Compare_Cars'] = 0
                summary['Compare_Freight'] = 0

            # Build final table with category totals
            category_order = ['Honda', 'M & M', 'Toyota', 'Skoda', 'Glovis', 'Tata', 'John Deere', 'Spinny', 'JSW MG', 'R.sai', 'Mohan Logistics', 'SAI Auto', 'Kwick', 'Market Load', 'Other']
            category_colors = {
                'Honda': '#ff6b35',
                'M & M': '#2e8b57',
                'Toyota': '#4169e1',
                'Skoda': '#8b5cf6',
                'Glovis': '#f59e0b',
                'Tata': '#06b6d4',
                'John Deere': '#22c55e',
                'Market Load': '#ec4899',
                'Other': '#6b7280',
                'Grand': '#1e40af'
            }

            final_rows = []
            grand_total = {'Own': 0, 'Vendor': 0, 'Total': 0, 'Own_F': 0, 'Vendor_F': 0, 'Total_F': 0, 'Cars_Comp': 0, 'Freight_Comp': 0, 'Target_SQR': 0}

            for category in category_order:
                cat_df = summary[summary['category'] == category].copy()
                if len(cat_df) > 0:
                    # Calculate category target sum from individual rows
                    category_target_sum = 0

                    # Add individual rows
                    for _, row in cat_df.iterrows():
                        target_sqr = targets.get(row['Party'], 0)
                        if target_sqr:
                            category_target_sum += target_sqr
                        final_rows.append({
                            'Client - Wise': row['Party'],
                            'Target SQR': target_sqr if target_sqr else '',
                            'Own': int(row['Own_Cars']),
                            'Vendor': int(row['Vendor_Cars']),
                            'Total': int(row['Total_Cars']),
                            'Own_F': row['Own_Freight'],
                            'Vendor_F': row['Vendor_Freight'],
                            'Total_F': row['Total_Freight'],
                            'Cars_Comp': int(row['Compare_Cars']),
                            'Freight_Comp': row['Compare_Freight'],
                            'is_total': False,
                            'category': category
                        })

                    # Add to grand total target (for all categories)
                    grand_total['Target_SQR'] += category_target_sum

                    # Add category total (only show when more than 1 party in category)
                    if len(cat_df) > 1:
                        final_rows.append({
                            'Client - Wise': f"{category} - Total",
                            'Target SQR': int(category_target_sum) if category_target_sum > 0 else '',
                            'Own': int(cat_df['Own_Cars'].sum()),
                            'Vendor': int(cat_df['Vendor_Cars'].sum()),
                            'Total': int(cat_df['Total_Cars'].sum()),
                            'Own_F': cat_df['Own_Freight'].sum(),
                            'Vendor_F': cat_df['Vendor_Freight'].sum(),
                            'Total_F': cat_df['Total_Freight'].sum(),
                            'Cars_Comp': int(cat_df['Compare_Cars'].sum()),
                            'Freight_Comp': cat_df['Compare_Freight'].sum(),
                            'is_total': True,
                            'category': category
                        })

                    # Accumulate grand total
                    grand_total['Own'] += int(cat_df['Own_Cars'].sum())
                    grand_total['Vendor'] += int(cat_df['Vendor_Cars'].sum())
                    grand_total['Total'] += int(cat_df['Total_Cars'].sum())
                    grand_total['Own_F'] += cat_df['Own_Freight'].sum()
                    grand_total['Vendor_F'] += cat_df['Vendor_Freight'].sum()
                    grand_total['Total_F'] += cat_df['Total_Freight'].sum()
                    grand_total['Cars_Comp'] += int(cat_df['Compare_Cars'].sum())
                    grand_total['Freight_Comp'] += cat_df['Compare_Freight'].sum()

            # Add Grand Total row
            final_rows.append({
                'Client - Wise': 'Grand Total',
                'Target SQR': int(grand_total['Target_SQR']) if grand_total['Target_SQR'] > 0 else '',
                'Own': grand_total['Own'],
                'Vendor': grand_total['Vendor'],
                'Total': grand_total['Total'],
                'Own_F': grand_total['Own_F'],
                'Vendor_F': grand_total['Vendor_F'],
                'Total_F': grand_total['Total_F'],
                'Cars_Comp': grand_total['Cars_Comp'],
                'Freight_Comp': grand_total['Freight_Comp'],
                'is_total': True,
                'category': 'Grand'
            })

            # Create display dataframe with row numbers
            display_data = []
            for idx, row in enumerate(final_rows, 1):
                display_data.append({
                    '': idx,
                    'Client - Wise': row['Client - Wise'],
                    'Target SQR': row['Target SQR'],
                    'Own': row['Own'],
                    'Vendor': row['Vendor'],
                    'Total': row['Total'],
                    'Own ': f"₹{row['Own_F']/100000:.2f}",
                    'Vendor ': f"₹{row['Vendor_F']/100000:.2f}",
                    'Total ': f"₹{row['Total_F']/100000:.2f}",
                    'Cars': row['Cars_Comp'],
                    'Freight': f"₹{row['Freight_Comp']/100000:.2f}"
                })

            display_df = pd.DataFrame(display_data)

            # Build HTML table with proper formatting
            compare_date = compare_till_date.strftime("%dth %b'%y")

            html_table = f"""
            <style>
                body {{
                    background-color: #0e1117;
                    color: #ffffff;
                }}
                .custom-table {{
                    width: 100%;
                    border-collapse: collapse;
                    font-size: 13px;
                    background-color: #0e1117;
                    color: #ffffff;
                }}
                .custom-table th {{
                    padding: 8px 12px;
                    text-align: center;
                    font-weight: bold;
                    color: #ffffff;
                    border: 1px solid #2d3748;
                    border-bottom: 2px solid #4a5568;
                }}
                .custom-table td {{
                    padding: 8px 10px;
                    border: 1px solid #2d3748;
                    color: #ffffff;
                }}
                .custom-table tr {{
                    border-bottom: 1px solid #4a5568;
                }}
                .custom-table tbody tr:hover {{
                    background-color: #1a202c;
                }}
                .summary-row {{
                    background-color: #1a365d !important;
                }}
                .col-divider {{
                    border-left: 2px solid #3b82f6 !important;
                }}
                .col-divider-right {{
                    border-right: 2px solid #3b82f6 !important;
                }}
                .header-group {{
                    background-color: #1e3a5f;
                    color: #ffffff;
                }}
                .header-sub {{
                    background-color: #16213e;
                    color: #ffffff;
                    font-size: 12px;
                }}
                .total-row {{
                    background-color: #d4a017 !important;
                    color: #000000 !important;
                    font-weight: bold;
                }}
                .total-row td {{
                    color: #000000 !important;
                }}
                .grand-total-row {{
                    background-color: #1e40af !important;
                    color: #ffffff !important;
                    font-weight: bold;
                }}
                .grand-total-row td {{
                    color: #ffffff !important;
                }}
                .data-row {{
                    color: #ffffff;
                }}
                .data-row:hover {{
                    background-color: #2d2d44;
                }}
                .text-left {{ text-align: left; }}
                .text-right {{ text-align: right; }}
                .text-center {{ text-align: center; }}
            </style>
            <div style="overflow-x: auto;">
            <table class="custom-table">
                <thead>
                    <tr>
                        <th class="header-group text-left col-divider-right" style="width: 25%;">Client - Wise</th>
                        <th class="header-group text-center col-divider-right" style="width: 8%;">Target SOB</th>
                        <th class="header-group text-center col-divider-right" colspan="3" style="background-color: #1e3a5f;">No. of Cars</th>
                        <th class="header-group text-center col-divider-right" colspan="3" style="background-color: #1e3a5f;">Freight (₹ Lakhs)</th>
                        <th class="header-group text-center" colspan="2" style="background-color: #0e4a6f;">Comparison (Till {compare_date})</th>
                    </tr>
                    <tr class="header-sub">
                        <th class="col-divider-right"></th>
                        <th class="col-divider-right"></th>
                        <th>Own</th>
                        <th>Vendor</th>
                        <th class="col-divider-right">Total</th>
                        <th>Own</th>
                        <th>Vendor</th>
                        <th class="col-divider-right">Total</th>
                        <th>Cars</th>
                        <th>Freight</th>
                    </tr>
                </thead>
                <tbody>
            """

            for row in final_rows:
                is_total = row['is_total']
                is_grand = row['category'] == 'Grand'

                if is_grand:
                    row_class = 'grand-total-row'
                elif is_total:
                    row_class = 'total-row'
                else:
                    row_class = 'data-row'

                html_table += f"""
                    <tr class="{row_class}">
                        <td class="text-left col-divider-right">{row['Client - Wise']}</td>
                        <td class="text-center col-divider-right">{row['Target SQR']}</td>
                        <td class="text-center">{row['Own']}</td>
                        <td class="text-center">{row['Vendor']}</td>
                        <td class="text-center col-divider-right">{row['Total']}</td>
                        <td class="text-right">₹{row['Own_F']/100000:.2f}</td>
                        <td class="text-right">₹{row['Vendor_F']/100000:.2f}</td>
                        <td class="text-right col-divider-right">₹{row['Total_F']/100000:.2f}</td>
                        <td class="text-center">{row['Cars_Comp']}</td>
                        <td class="text-right">₹{row['Freight_Comp']/100000:.2f}</td>
                    </tr>
                """

            # Calculate Avg Per Day and Shortfall
            days_in_period = (till_date - select_month_start).days + 1
            avg_per_day = grand_total['Total_F'] / days_in_period / 100000 if days_in_period > 0 else 0

            # Shortfall calculation (current period freight - comparison period freight)
            shortfall = (grand_total['Total_F'] - grand_total['Freight_Comp']) / 100000

            # Color based on positive/negative
            avg_color = "#22c55e" if avg_per_day >= 0 else "#ef4444"  # Green if positive, Red if negative
            shortfall_color = "#22c55e" if shortfall >= 0 else "#ef4444"  # Green if positive, Red if negative

            # Add Avg Per Day row
            html_table += f"""
                    <tr class="summary-row">
                        <td colspan="7" class="text-center" style="color: #ffffff; font-weight: bold; padding: 10px; background-color: #0f172a;">Avg Per Day >></td>
                        <td colspan="3" class="text-center" style="color: {avg_color}; font-weight: bold; padding: 10px; background-color: #0f172a;">₹{avg_per_day:.2f} L</td>
                    </tr>
                    <tr class="summary-row">
                        <td colspan="7" class="text-center" style="color: #ffffff; font-weight: bold; padding: 10px; background-color: #0f172a;">Shortfall from Till {compare_date} >></td>
                        <td colspan="3" class="text-center" style="color: {shortfall_color}; font-weight: bold; padding: 10px; background-color: #0f172a;">₹{shortfall:.2f} L</td>
                    </tr>
                </tbody>
            </table>
            </div>
            """

            # Calculate dynamic height based on number of rows + summary rows
            table_height = 100 + (len(final_rows) * 35) + 100
            components.html(html_table, height=table_height, scrolling=False)

        target_vs_actual_fragment()

    with tab2:
        st.markdown("### Daily Loading Details")

        @st.fragment(run_every=REFRESH_10_MIN)
        def daily_loading_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()
            frag_df = st.session_state.df

            # Date filter and summary in one row
            col_date, col_cars, col_trips_count, col_empty = st.columns([1, 1, 1, 1])
            with col_date:
                selected_date = st.date_input("Select Date", datetime.now().date(), key='daily_date_fragment')

            # Filter data for selected date - only loaded trips (with Party Name)
            daily_data = frag_df[
                (frag_df['LoadingDateOnly'] == selected_date) &
                (frag_df['DisplayParty'] != '') &
                (frag_df['DisplayParty'].notna()) &
                (frag_df['CarQty'] > 0)
            ]

            # Own totals
            own_cars_lifted = int(daily_data['CarQty'].sum())
            own_trips_count = len(daily_data)

            # Vendor totals for selected date
            frag_vendor_df = st.session_state.vendor_df
            vendor_cars_lifted = 0
            vendor_trips_count = 0
            if not frag_vendor_df.empty:
                vendor_daily_data = frag_vendor_df[
                    (frag_vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                    (frag_vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
                ].copy()
                if not vendor_daily_data.empty:
                    vendor_daily_data['MappedParty'] = vendor_daily_data.apply(
                        lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                    )
                    vendor_daily_data = vendor_daily_data[vendor_daily_data['MappedParty'].notna()]
                    vendor_cars_lifted = int(vendor_daily_data['CarQty'].sum())
                    # Trips = unique vehicle numbers
                    vendor_trips_count = vendor_daily_data['VehicleNo'].nunique()

            # Total (Own + Vendor)
            total_cars_lifted = own_cars_lifted + vendor_cars_lifted
            total_trips_count = own_trips_count + vendor_trips_count

            with col_cars:
                st.markdown(f"""
                <div style="background: linear-gradient(90deg, #22c55e, #16a34a); padding: 15px 25px; border-radius: 8px; text-align: center; margin-top: 25px;">
                    <span style="color: white; font-weight: bold; font-size: 20px;">Total Cars Lifted: {total_cars_lifted}</span>
                </div>
                <div style="display: flex; gap: 10px; margin-top: 8px;">
                    <div style="flex: 1; background: #1e3a5f; padding: 6px; border-radius: 6px; text-align: center;">
                        <div style="color: #9ca3af; font-size: 10px;">Own</div>
                        <div style="color: white; font-size: 13px; font-weight: bold;">{own_cars_lifted:,}</div>
                    </div>
                    <div style="flex: 1; background: #4a3728; padding: 6px; border-radius: 6px; text-align: center;">
                        <div style="color: #9ca3af; font-size: 10px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 13px; font-weight: bold;">{vendor_cars_lifted:,}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            with col_trips_count:
                st.markdown(f"""
                <div style="background: linear-gradient(90deg, #3b82f6, #2563eb); padding: 15px 25px; border-radius: 8px; text-align: center; margin-top: 25px;">
                    <span style="color: white; font-weight: bold; font-size: 20px;">Total Trips: {total_trips_count}</span>
                </div>
                <div style="display: flex; gap: 10px; margin-top: 8px;">
                    <div style="flex: 1; background: #1e3a5f; padding: 6px; border-radius: 6px; text-align: center;">
                        <div style="color: #9ca3af; font-size: 10px;">Own</div>
                        <div style="color: white; font-size: 13px; font-weight: bold;">{own_trips_count:,}</div>
                    </div>
                    <div style="flex: 1; background: #4a3728; padding: 6px; border-radius: 6px; text-align: center;">
                        <div style="color: #9ca3af; font-size: 10px;">Vendor</div>
                        <div style="color: #f59e0b; font-size: 13px; font-weight: bold;">{vendor_trips_count:,}</div>
                    </div>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("<br>", unsafe_allow_html=True)

            # Create two columns - Trip Details and Party-wise Summary
            col_left, col_right = st.columns([3, 1])

            with col_left:
                st.markdown(f"**Daily Loading Details {selected_date.strftime('%d-%b-%Y')}**")

                if len(daily_data) > 0:
                    # Prepare trip details table
                    trip_details = daily_data[['LoadingDate', 'VehicleNo', 'DisplayParty', 'Route', 'CarQty']].copy()

                    # Split Route into Origin and Destination
                    trip_details['Origin'] = trip_details['Route'].apply(lambda x: x.split(' - ')[0] if ' - ' in str(x) else str(x))
                    trip_details['Destination'] = trip_details['Route'].apply(lambda x: x.split(' - ')[1] if ' - ' in str(x) and len(x.split(' - ')) > 1 else '')

                    # Build HTML table
                    html_trips = """
                    <style>
                        .trips-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                        .trips-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #3b82f6; position: sticky; top: 0; z-index: 10; }
                        .trips-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                        .trips-table tr:nth-child(even) { background-color: #1a1f2e; }
                        .trips-table tr:nth-child(odd) { background-color: #0e1117; }
                        .trips-table tr:hover { background-color: #2d3748; }
                    </style>
                    <div style="max-height: 450px; overflow-y: auto;">
                    <table class="trips-table">
                        <thead>
                            <tr>
                                <th>S.No.</th>
                                <th>Date</th>
                                <th>Vehicle No</th>
                                <th>Party Name</th>
                                <th>Origin</th>
                                <th>Destination</th>
                                <th>Qty</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    total_qty = 0
                    for idx, (_, row) in enumerate(trip_details.iterrows(), 1):
                        date_str = row['LoadingDate'].strftime('%d/%m/%Y')
                        qty = int(row['CarQty'])
                        total_qty += qty
                        html_trips += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{date_str}</td>
                                <td>{row['VehicleNo']}</td>
                                <td>{row['DisplayParty']}</td>
                                <td>{row['Origin']}</td>
                                <td>{row['Destination']}</td>
                                <td style="text-align: center;">{qty}</td>
                            </tr>
                        """
                    # Add Grand Total row
                    html_trips += f"""
                            <tr style="background-color: #1e40af !important; font-weight: bold;">
                                <td colspan="6" style="text-align: right; color: white;">Grand Total</td>
                                <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_qty}</td>
                            </tr>
                        """
                    html_trips += "</tbody></table></div>"
                    components.html(html_trips, height=500, scrolling=True)
                else:
                    st.info("No own trips found for the selected date.")

                # Vendor Data Section (inside col_left)
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(f"**Vendor Loading Details {selected_date.strftime('%d-%b-%Y')}**")

                # Filter vendor data for selected date
                if not frag_vendor_df.empty:
                    vendor_daily = frag_vendor_df[
                        (frag_vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                        (frag_vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
                    ].copy()
                    if not vendor_daily.empty:
                        # Apply vendor mapping
                        vendor_daily['MappedParty'] = vendor_daily.apply(
                            lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                        )
                        vendor_daily = vendor_daily[vendor_daily['MappedParty'].notna()]

                    if not vendor_daily.empty:
                        # Build vendor HTML table
                        html_vendor = """
                        <style>
                            .vendor-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                            .vendor-table th { background-color: #7c3aed; color: white; padding: 10px; text-align: left; border: 1px solid #8b5cf6; position: sticky; top: 0; z-index: 10; }
                            .vendor-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                            .vendor-table tr:nth-child(even) { background-color: #1a1f2e; }
                            .vendor-table tr:nth-child(odd) { background-color: #0e1117; }
                            .vendor-table tr:hover { background-color: #2d3748; }
                        </style>
                        <div style="max-height: 300px; overflow-y: auto;">
                        <table class="vendor-table">
                            <thead>
                                <tr>
                                    <th>S.No.</th>
                                    <th>Date</th>
                                    <th>Vehicle No</th>
                                    <th>Mapped To</th>
                                    <th>Route</th>
                                    <th>Qty</th>
                                </tr>
                            </thead>
                            <tbody>
                        """
                        vendor_total_qty = 0
                        for idx, (_, row) in enumerate(vendor_daily.iterrows(), 1):
                            date_str = row['CNDate'].strftime('%d/%m/%Y') if pd.notna(row['CNDate']) else ''
                            vehicle_no = row['VehicleNo'] if pd.notna(row.get('VehicleNo')) else ''
                            qty = int(row['CarQty']) if pd.notna(row['CarQty']) else 0
                            vendor_total_qty += qty
                            html_vendor += f"""
                                <tr>
                                    <td>{idx}</td>
                                    <td>{date_str}</td>
                                    <td>{vehicle_no}</td>
                                    <td>{row['MappedParty']}</td>
                                    <td>{row['Route']}</td>
                                    <td style="text-align: center;">{qty}</td>
                                </tr>
                            """
                        html_vendor += f"""
                                <tr style="background-color: #7c3aed !important; font-weight: bold;">
                                    <td colspan="5" style="text-align: right; color: white;">Grand Total</td>
                                    <td style="text-align: center; color: #fbbf24; font-size: 15px;">{vendor_total_qty}</td>
                                </tr>
                            """
                        html_vendor += "</tbody></table></div>"
                        components.html(html_vendor, height=350, scrolling=True)
                    else:
                        st.info("No vendor trips found for the selected date.")
                else:
                    st.info("No vendor data available.")

            with col_right:
                st.markdown(f"**OEM's Wise Total Loads {selected_date.strftime('%d-%b-%Y')}**")

                if len(daily_data) > 0:
                    # Party-wise summary
                    party_summary = daily_data.groupby('DisplayParty').agg({
                        'TLHSNo': 'count',
                        'CarQty': 'sum'
                    }).reset_index()
                    party_summary.columns = ['Party Name', 'Trip Count', 'Qty']
                    party_summary = party_summary[party_summary['Party Name'] != '']
                    party_summary = party_summary.sort_values('Trip Count', ascending=False)

                    # Build HTML table
                    html_summary = """
                    <style>
                        .summary-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                        .summary-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #3b82f6; position: sticky; top: 0; z-index: 10; }
                        .summary-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                        .summary-table tr:nth-child(even) { background-color: #1a1f2e; }
                        .summary-table tr:nth-child(odd) { background-color: #0e1117; }
                        .summary-table tr:hover { background-color: #2d3748; }
                    </style>
                    <div style="max-height: 450px; overflow-y: auto;">
                    <table class="summary-table">
                        <thead>
                            <tr>
                                <th>#</th>
                                <th>Party Name</th>
                                <th>Trips</th>
                                <th>Qty</th>
                            </tr>
                        </thead>
                        <tbody>
                    """
                    total_trip_count = 0
                    total_qty = 0
                    for idx, (_, row) in enumerate(party_summary.iterrows(), 1):
                        trip_count = row['Trip Count']
                        qty = int(row['Qty'])
                        total_trip_count += trip_count
                        total_qty += qty
                        html_summary += f"""
                            <tr>
                                <td>{idx}</td>
                                <td>{row['Party Name']}</td>
                                <td style="text-align: center; font-weight: bold; color: #22c55e;">{trip_count}</td>
                                <td style="text-align: center; font-weight: bold; color: #a78bfa;">{qty}</td>
                            </tr>
                        """
                    # Add Grand Total row
                    html_summary += f"""
                            <tr style="background-color: #1e40af !important; font-weight: bold;">
                                <td colspan="2" style="text-align: right; color: white;">Grand Total</td>
                                <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_trip_count}</td>
                                <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_qty}</td>
                            </tr>
                        """
                    html_summary += "</tbody></table></div>"
                    components.html(html_summary, height=500, scrolling=True)
                else:
                    st.info("No data available.")

                # Vendor Summary - Mapped To wise
                st.markdown("<br>", unsafe_allow_html=True)
                st.markdown(f"**Vendor Summary {selected_date.strftime('%d-%b-%Y')}**")

                if not frag_vendor_df.empty:
                    vendor_daily_summary = frag_vendor_df[
                        (frag_vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                        (frag_vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
                    ].copy()
                    if not vendor_daily_summary.empty:
                        vendor_daily_summary['MappedParty'] = vendor_daily_summary.apply(
                            lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                        )
                        vendor_daily_summary = vendor_daily_summary[vendor_daily_summary['MappedParty'].notna()]

                    if not vendor_daily_summary.empty:
                        # Group by MappedParty - count unique vehicle numbers as trips and sum qty
                        # Trip = unique combination of Date + Vehicle No + Mapped To (date already filtered)
                        vendor_party_summary = vendor_daily_summary.groupby('MappedParty').agg({
                            'CarQty': 'sum',
                            'VehicleNo': 'nunique'  # Count of unique vehicles = No of Trips
                        }).reset_index()
                        vendor_party_summary.columns = ['Mapped To', 'Qty', 'Trips']
                        vendor_party_summary = vendor_party_summary.sort_values('Qty', ascending=False)

                        # Build HTML table
                        html_vendor_summary = """
                        <style>
                            .vendor-summary-table { width: 100%; border-collapse: collapse; font-size: 13px; }
                            .vendor-summary-table th { background-color: #7c3aed; color: white; padding: 10px; text-align: left; border: 1px solid #8b5cf6; position: sticky; top: 0; z-index: 10; }
                            .vendor-summary-table td { padding: 8px 10px; border: 1px solid #2d3748; color: white; }
                            .vendor-summary-table tr:nth-child(even) { background-color: #1a1f2e; }
                            .vendor-summary-table tr:nth-child(odd) { background-color: #0e1117; }
                            .vendor-summary-table tr:hover { background-color: #2d3748; }
                        </style>
                        <div style="max-height: 300px; overflow-y: auto;">
                        <table class="vendor-summary-table">
                            <thead>
                                <tr>
                                    <th>#</th>
                                    <th>Mapped To</th>
                                    <th>Trips</th>
                                    <th>Qty</th>
                                </tr>
                            </thead>
                            <tbody>
                        """
                        total_vendor_qty = 0
                        total_vendor_trips = 0
                        for idx, (_, row) in enumerate(vendor_party_summary.iterrows(), 1):
                            qty = int(row['Qty'])
                            trips = int(row['Trips'])
                            total_vendor_qty += qty
                            total_vendor_trips += trips
                            html_vendor_summary += f"""
                                <tr>
                                    <td>{idx}</td>
                                    <td>{row['Mapped To']}</td>
                                    <td style="text-align: center; font-weight: bold; color: #22c55e;">{trips}</td>
                                    <td style="text-align: center; font-weight: bold; color: #a78bfa;">{qty}</td>
                                </tr>
                            """
                        html_vendor_summary += f"""
                                <tr style="background-color: #7c3aed !important; font-weight: bold;">
                                    <td colspan="2" style="text-align: right; color: white;">Grand Total</td>
                                    <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_vendor_trips}</td>
                                    <td style="text-align: center; color: #fbbf24; font-size: 15px;">{total_vendor_qty}</td>
                                </tr>
                            """
                        html_vendor_summary += "</tbody></table></div>"
                        components.html(html_vendor_summary, height=300, scrolling=True)
                    else:
                        st.info("No vendor data for this date.")
                else:
                    st.info("No vendor data available.")

        # Call the fragment
        daily_loading_fragment()

    with tab3:
        st.markdown("### Local/Pilot Loads")

        # Use fragment to prevent tab switching on filter change
        @st.fragment(run_every=REFRESH_15_MIN)
        def local_pilot_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()

            # Pre-load ALL vehicle types once at the start for better performance
            kia_vehicles = load_vehicles_by_type('TR_KIA_LCL')
            kia_ap_vehicles = load_vehicles_by_type('TR_KIA_AP PASSING')
            haridwar_vehicles = load_vehicles_by_type('TR_HRD_LCL')
            gujarat_vehicles = load_vehicles_by_type('TR_Gujarat_LCL')
            nsk_ckn_vehicles = load_vehicles_by_type('NSK/Ckn-north dedicated')
            patna_vehicles = load_vehicles_by_type('TR_Patna_LCL_4018 BS6')
            road_pilot_vehicles = load_vehicles_by_type('Road Pilot')

            # Create filter functions for each category (using pre-loaded vehicle lists)
            def get_toyota_local(data):
                return data[data['NewPartyName'].str.contains('DC Movement', case=False, na=False)]

            def get_patna_local(data):
                if not patna_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in patna_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_haridwar_local(data):
                if not haridwar_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in haridwar_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_road_pilot(data):
                if not road_pilot_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in road_pilot_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_kia_local(data):
                if not kia_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in kia_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_kia_ap_passing(data):
                if not kia_ap_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in kia_ap_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_gujarat_local(data):
                if not gujarat_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in gujarat_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            def get_nsk_ckn_local(data):
                if not nsk_ckn_vehicles:
                    return data.head(0)
                pattern = '|'.join([v.replace(' ', '.*') for v in nsk_ckn_vehicles])
                return data[data['VehicleNo'].str.contains(pattern, case=False, na=False, regex=True)]

            # Filter month_df to only include loaded trips
            loaded_month_df = month_df[
                (month_df['DisplayParty'] != '') &
                (month_df['DisplayParty'].notna()) &
                (month_df['CarQty'] > 0)
            ]

            # Get data for each category (using loaded trips only)
            toyota_local = get_toyota_local(loaded_month_df)
            patna_local = get_patna_local(loaded_month_df)
            haridwar_local = get_haridwar_local(loaded_month_df)
            road_pilot = get_road_pilot(loaded_month_df)
            kia_local = get_kia_local(loaded_month_df)
            kia_ap_passing = get_kia_ap_passing(loaded_month_df)
            gujarat_local = get_gujarat_local(loaded_month_df)
            nsk_ckn_local = get_nsk_ckn_local(loaded_month_df)

            # Summary data for all categories (including unique vehicle count)
            def get_summary(df, category_name):
                vehicles = df['VehicleNo'].nunique() if len(df) > 0 else 0
                freight = df['Freight'].sum()
                avg_freight = freight / vehicles if vehicles > 0 else 0
                return {
                    'Category': category_name,
                    'Trips': len(df),
                    'Cars': int(df['CarQty'].sum()),
                    'Vehicles': vehicles,
                    'Freight': freight,
                    'AvgFreight': avg_freight
                }

            summary_data = [
                get_summary(toyota_local, 'Toyota Local'),
                get_summary(patna_local, 'Patna Local'),
                get_summary(haridwar_local, 'Haridwar Local'),
                get_summary(road_pilot, 'Road Pilot'),
                get_summary(kia_local, 'Kia Local'),
                get_summary(kia_ap_passing, 'Kia AP Passing'),
                get_summary(gujarat_local, 'Gujarat Local'),
                get_summary(nsk_ckn_local, 'NSK/Ckn-north dedicated'),
            ]

            # Summary Section
            st.markdown("#### Summary")
            summary_html = """
            <style>
                .summary-local { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 14px; margin-bottom: 20px; }
                .summary-local th { background-color: #1e3a5f; color: white; padding: 12px; text-align: center; border: 1px solid #64748b; }
                .summary-local td { padding: 10px; border: 1px solid #64748b; color: white; text-align: center; }
                .summary-local tr:nth-child(even) { background-color: #1e293b; }
                .summary-local tr:nth-child(odd) { background-color: #0f172a; }
                .summary-local .total-row { background-color: #1e3a5f !important; font-weight: bold; }
                .summary-local .total-row td { border: 1px solid #64748b; }
            </style>
            <table class="summary-local">
                <thead>
                    <tr>
                        <th>Category</th>
                        <th>Total Trips</th>
                        <th>Cars Lifted</th>
                        <th>No. of Vehicles</th>
                        <th>Freight</th>
                        <th>Avg Freight</th>
                    </tr>
                </thead>
                <tbody>
            """
            total_trips = 0
            total_cars = 0
            total_vehicles = 0
            total_freight = 0
            for item in summary_data:
                total_trips += item['Trips']
                total_cars += item['Cars']
                total_vehicles += item['Vehicles']
                total_freight += item['Freight']
                summary_html += f"""
                    <tr>
                        <td style="text-align: left; font-weight: bold;">{item['Category']}</td>
                        <td>{item['Trips']}</td>
                        <td>{item['Cars']}</td>
                        <td>{item['Vehicles']}</td>
                        <td>₹{item['Freight']/100000:.2f}L</td>
                        <td>₹{item['AvgFreight']/100000:.2f}L</td>
                    </tr>
                """
            # Calculate grand total avg freight
            grand_avg_freight = total_freight / total_vehicles if total_vehicles > 0 else 0
            summary_html += f"""
                    <tr class="total-row">
                        <td style="text-align: left;">Grand Total</td>
                        <td style="color: #fbbf24;">{total_trips}</td>
                        <td style="color: #fbbf24;">{total_cars}</td>
                        <td style="color: #fbbf24;">{total_vehicles}</td>
                        <td style="color: #fbbf24;">₹{total_freight/100000:.2f}L</td>
                        <td style="color: #fbbf24;">₹{grand_avg_freight/100000:.2f}L</td>
                    </tr>
                </tbody>
            </table>
            """
            components.html(summary_html, height=450)

            # Filter dropdown
            st.markdown("#### Details by Category")
            col_filter, col_download, col_empty = st.columns([1, 0.5, 2.5])
            with col_filter:
                category_options = ['Toyota Local', 'Patna Local', 'Haridwar Local', 'Road Pilot', 'Kia Local', 'Kia AP Passing', 'Gujarat Local', 'NSK/Ckn-north dedicated']
                selected_category = st.selectbox("Select Category", category_options, key='local_category')

            # Get filtered data based on selection
            if selected_category == 'Toyota Local':
                filtered_df = toyota_local
            elif selected_category == 'Patna Local':
                filtered_df = patna_local
            elif selected_category == 'Haridwar Local':
                filtered_df = haridwar_local
            elif selected_category == 'Road Pilot':
                filtered_df = road_pilot
            elif selected_category == 'Kia Local':
                filtered_df = kia_local
            elif selected_category == 'Kia AP Passing':
                filtered_df = kia_ap_passing
            elif selected_category == 'Gujarat Local':
                filtered_df = gujarat_local
            else:
                filtered_df = nsk_ckn_local

            if len(filtered_df) > 0:
                # Sort by LoadingDate ascending
                filtered_df = filtered_df.sort_values('LoadingDate', ascending=True)

                # Prepare download data with same formatting as table
                download_data = []
                total_freight_dl = 0
                total_qty_dl = 0
                for idx, (_, row) in enumerate(filtered_df.iterrows(), 1):
                    date_str = row['LoadingDate'].strftime('%d/%m/%Y') if pd.notna(row['LoadingDate']) else ''
                    freight = row['Freight'] if pd.notna(row['Freight']) else 0
                    qty = int(row['CarQty']) if pd.notna(row['CarQty']) else 0
                    total_freight_dl += freight
                    total_qty_dl += qty
                    download_data.append({
                        'S.No.': idx,
                        'Vehicle No': row['VehicleNo'],
                        'Date': date_str,
                        'Route': row['Route'],
                        'Freight': f"₹{freight:,.0f}",
                        'Qty': qty
                    })
                # Add Grand Total row
                download_data.append({
                    'S.No.': '',
                    'Vehicle No': '',
                    'Date': '',
                    'Route': 'Grand Total',
                    'Freight': f"₹{total_freight_dl:,.0f}",
                    'Qty': total_qty_dl
                })
                download_df = pd.DataFrame(download_data)

                # Add download button
                with col_download:
                    st.markdown("<br>", unsafe_allow_html=True)
                    csv_data = download_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download",
                        data=csv_data,
                        file_name=f"{selected_category.replace(' ', '_')}_details.csv",
                        mime="text/csv",
                        key="local_category_download"
                    )

                # Build details table
                details_html = """
                <style>
                    .details-table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 13px; }
                    .details-table th { background-color: #1e3a5f; color: white; padding: 10px; text-align: left; border: 1px solid #64748b; position: sticky; top: 0; z-index: 1; }
                    .details-table td { padding: 8px 10px; border: 1px solid #64748b; color: white; }
                    .details-table tr:nth-child(even) { background-color: #1e293b; }
                    .details-table tr:nth-child(odd) { background-color: #0f172a; }
                    .details-table tr:hover { background-color: #2d3748; }
                    .details-table .total-row { background-color: #1e3a5f !important; font-weight: bold; }
                </style>
                <div style="max-height: 400px; overflow-y: auto;">
                <table class="details-table">
                    <thead>
                        <tr>
                            <th>S.No.</th>
                            <th>Vehicle No</th>
                            <th>Date</th>
                            <th>Route</th>
                            <th>Freight</th>
                            <th>Qty</th>
                        </tr>
                    </thead>
                    <tbody>
                """
                total_freight_detail = 0
                total_qty_detail = 0
                for idx, (_, row) in enumerate(filtered_df.iterrows(), 1):
                    date_str = row['LoadingDate'].strftime('%d/%m/%Y') if pd.notna(row['LoadingDate']) else ''
                    freight = row['Freight'] if pd.notna(row['Freight']) else 0
                    qty = int(row['CarQty']) if pd.notna(row['CarQty']) else 0
                    total_freight_detail += freight
                    total_qty_detail += qty
                    details_html += f"""
                        <tr>
                            <td>{idx}</td>
                            <td>{row['VehicleNo']}</td>
                            <td>{date_str}</td>
                            <td>{row['Route']}</td>
                            <td style="text-align: right;">₹{freight:,.0f}</td>
                            <td style="text-align: center;">{qty}</td>
                        </tr>
                    """
                details_html += f"""
                        <tr class="total-row">
                            <td colspan="4" style="text-align: right; color: white;">Grand Total</td>
                            <td style="text-align: right; color: #fbbf24;">₹{total_freight_detail:,.0f}</td>
                            <td style="text-align: center; color: #fbbf24;">{total_qty_detail}</td>
                        </tr>
                    </tbody>
                </table>
                </div>
                """
                components.html(details_html, height=450, scrolling=True)
            else:
                st.info(f"No data found for {selected_category}.")

        # Call the fragment
        local_pilot_fragment()

    with tab4:
        st.markdown("### Zone View")

        # Zone mapping function
        def get_zone(city):
            if pd.isna(city) or city == '':
                return 'Unknown'
            city_upper = str(city).upper().strip()

            # North Zone - Delhi NCR, Punjab, Haryana, Himachal, J&K, Uttarakhand, Rajasthan, Uttar Pradesh
            north_cities = ['DELHI', 'NOIDA', 'GURGAON', 'GURUGRAM', 'FARIDABAD', 'GHAZIABAD', 'GHAIZABAD', 'GREATER NOIDA',
                           'CHANDIGARH', 'MOHALI', 'PANCHKULA', 'LUDHIANA', 'JALANDHAR', 'JALLANDHAR', 'AMRITSAR', 'PATIALA',
                           'BATHINDA', 'BHATINDA', 'FIROZPUR', 'FEROZEPUR', 'HOSHIARPUR', 'MUKTSAR', 'SANGRUR',
                           'SONIPAT', 'PANIPAT', 'KARNAL', 'KURUKSHETRA', 'AMBALA', 'HISAR', 'HISSAR', 'ROHTAK',
                           'BHIWANI', 'SIRSA', 'JIND', 'KAITHAL', 'REWARI', 'NARNAUL', 'BAHADURGARH', 'DHARUHERA', 'DHARUHEDA',
                           'MANESAR', 'KUNDLI', 'PALWAL', 'NUH', 'FARRUKHNAGAR', 'FARUKHNAGAR', 'PIRTHLA', 'SOHNA',
                           'SHIMLA', 'MANDI', 'KANGRA', 'KULLU', 'SOLAN', 'UNA', 'HAMIRPUR', 'PAONTA SAHIB',
                           'JAMMU', 'SRINAGAR', 'KATHUA', 'PATHANKOT', 'ROPAR', 'RUPNAGAR', 'KAPURTHALA', 'KALANWALI',
                           'DEHRADUN', 'HARIDWAR', 'ROORKEE', 'HALDWANI', 'RUDRAPUR', 'KASHIPUR',
                           'TAPUKERA', 'ICAT MANESAR', 'KHARKHODA', 'BILASPUR (HR)', 'YAMUNANAGAR', 'BHIWADI', 'WAZIRPUR',
                           'JAIPUR', 'JODHPUR', 'UDAIPUR', 'KOTA', 'AJMER', 'BIKANER', 'ALWAR', 'BHILWARA', 'KANKROLI',
                           'SIKAR', 'SRIGANGANAGAR', 'SRI GANGANAGAR', 'BHARATPUR', 'DAUSA', 'JHUNJHUNU', 'CHITTORGARH', 'TONK',
                           'BANSWARA', 'NAGAUR', 'SAWAI MADHOPUR', 'JHALAWAR', 'NEEMUCH',
                           # Uttar Pradesh
                           'LUCKNOW', 'KANPUR', 'AGRA', 'VARANASI', 'ALLAHABAD', 'PRAYAGRAJ', 'GORAKHPUR',
                           'BAREILLY', 'ALIGARH', 'MORADABAD', 'MEERUT', 'SAHARANPUR', 'MATHURA', 'FIROZABAD',
                           'ETAWAH', 'ETAH', 'MAINPURI', 'SHAHJAHANPUR', 'SAHANJANPUR', 'SITAPUR', 'HARDOI',
                           'UNNAO', 'RAE BAREILLY', 'RAEBARELI', 'RAI BARELI', 'SULTANPUR', 'FAIZABAD',
                           'AZAMGARH', 'JAUNPUR', 'MIRZAPUR', 'ROBERTSGANJ', 'BASTI', 'GONDA', 'DEORIA',
                           'BULANDSHAHAR', 'BIJNOR', 'MUZAFFARNAGAR', 'MUZAFFAR NAGAR', 'NAJIBABAD',
                           'LAKHIMPUR', 'LAKHIMPUR KHERI', 'ORAI', 'JHANSI', 'FARRUKHABAD', 'PRATAPGARH',
                           'ABOHAR', 'KUNDA', 'KHURJA', 'KARHAL', 'KARHAL(UP)', 'KOTVA SARAK']

            # East Zone - West Bengal, Bihar, Jharkhand, Odisha, Assam, NE States, Chhattisgarh
            east_cities = ['KOLKATA', 'HOWRAH', 'SILIGURI', 'ASANSOL', 'DURGAPUR', 'KHARAGPUR', 'MALDA',
                          'BARDHAMAN', 'BARDDHAMAN', 'BARDHMAN', 'BEHRAMPORE', 'BERHAMPORE', 'BEHRAMPUR', 'COOCHBEHAR', 'ALIPURDUAR',
                          'BUNIYADPUR', 'MOGRA', 'UPARNAGAR',
                          'PATNA', 'MUZAFFARPUR', 'GAYA', 'BHAGALPUR', 'DARBHANGA', 'BEGUSARAI', 'CHAPRA',
                          'MOTIHARI', 'PURNIA', 'SAMASTIPUR', 'BIHAR SHARIF', 'SAHARSA', 'SHARSHA', 'SIWAN', 'GOPALGANJ',
                          'GOPALGUNJ', 'ARRAH', 'ARAH', 'AURANGABAD (BIHAR)', 'VAISHALI', 'KISHANGANJ', 'JAMALPUR',
                          'ANISABAD', 'ANISHABAD', 'SHERGHATI',
                          'RANCHI', 'JAMSHEDPUR', 'DHANBAD', 'BOKARO', 'HAZARIBAGH', 'HAZARIBAG', 'DEOGHAR', 'DEOGARH',
                          'DALTONGANJ', 'BARHI(JH)', 'GIRIDIH',
                          'BHUBANESHWAR', 'CUTTACK', 'ROURKELA', 'SAMBALPUR', 'BERHAMPUR', 'BRAHMAPUR',
                          'BALASORE', 'PURI', 'ANGUL', 'PANIKOILI', 'KEONJHAR', 'JEYPORE', 'JAYPORE',
                          'GUWAHATI', 'TEZPUR', 'DIBRUGARH', 'JORHAT', 'NAGAON', 'BONGAIGAON', 'NORTH LAKHIMPUR', 'SILCHAR',
                          'SHILLONG', 'GANGTOK', 'DIMAPUR', 'NAHARLAGUN', 'WEST CHAMPARAN', 'AGARTALA', 'IMPHAL', 'PORT BLAIR',
                          # Chhattisgarh
                          'RAIPUR', 'BILASPUR', 'BHILAI', 'KORBA', 'RAJNANDGAON', 'DURG', 'JAGDALPUR', 'AMBIKAPUR', 'KANKER']

            # West Zone - Maharashtra, Gujarat, Goa, Madhya Pradesh
            west_cities = ['MUMBAI', 'PUNE', 'NASHIK', 'NAGPUR', 'AURANGABAD', 'AURNGABAD(MH)', 'AURANGABAD(MAHARASHTRA)',
                          'SOLAPUR', 'KOLHAPUR', 'SANGLI', 'SATARA', 'AHMEDNAGAR', 'THANE', 'NAVI MUMBAI',
                          'BHIWANDI', 'PANVEL', 'PANWEL', 'VASAI', 'KALYAN', 'RATNAGIRI', 'LATUR', 'BEED',
                          'JALGAON', 'DHULE', 'AKOLA', 'AMRAVATI', 'CHANDRAPUR', 'CHANDERPUR', 'MALEGAON',
                          'BARAMATI', 'SANGAMNER', 'RANJANGAON', 'UDGIR', 'NANDED',
                          'AHMEDABAD', 'SURAT', 'VADODARA', 'RAJKOT', 'BHAVNAGAR', 'JAMNAGAR', 'JUNAGADH',
                          'GANDHIDHAM', 'BHUJ', 'ANAND', 'MEHSANA', 'MORBI', 'VAPI', 'NAVSARI', 'BHARUCH',
                          'ANKLESHWAR', 'GODHRA', 'DAHOD', 'HIMATNAGAR', 'HIMMATNAGAR', 'SURENDRANAGAR',
                          'PALANPUR', 'SANAND', 'BECHRAJI', 'HALOL', 'BARDOLI', 'CHHARODI', 'AMBLI', 'GANDHINAGAR',
                          'DHOLERA', 'PIPAVAV PORT', 'KHEDA(GJ)',
                          'GOA', 'NUVEM',
                          # Madhya Pradesh
                          'BHOPAL', 'INDORE', 'JABALPUR', 'GWALIOR', 'UJJAIN', 'RATLAM', 'DEWAS', 'SAGAR',
                          'SATNA', 'REWA', 'KATNI', 'CHHINDWARA', 'CHINDWARA', 'KHANDWA', 'KHARGONE',
                          'HOSHANGABAD', 'SEHORE', 'VIDISHA', 'SHAHDOL', 'SEONI', 'LAKHNADON', 'SHIVPURI',
                          'GUNA', 'BIAORA', 'SHUJALPUR', 'SUJALPUR', 'CHHATARPUR', 'MAHOBA', 'WAIDHAN',
                          'JHABUA', 'JABHUA', 'MAKSI', 'MAKSI(MP)']

            # South Zone - Karnataka, Tamil Nadu, Kerala, Andhra Pradesh, Telangana
            south_cities = ['BANGALORE', 'MYSORE', 'HUBLI', 'BELGAUM', 'BELGAON', 'MANGALORE', 'MANGLORE',
                           'DAVANGERE', 'BELLARY', 'TUMKUR', 'SHIMOGA', 'SIMOGA', 'HASSAN', 'UDUPI',
                           'CHITRADURGA', 'HOSPET', 'GULBARGA', 'KALABURGI', 'BIJAPUR', 'RAICHUR',
                           'CHIKMAGALUR', 'CHIKKAMAGALURU', 'CHIKKABALLAPUR', 'RAMANAGARA', 'BIDADI',
                           'HOSUR', 'KADUR', 'SINDHANUR', 'YELLAPUR(KA)', 'TOYOTA BANGLORE', 'HAROHALLI',
                           'CHENNAI', 'COIMBATORE', 'MADURAI', 'TRICHY', 'TIRUCHIRAPPALLI', 'SALEM', 'TIRUPUR', 'ERODE',
                           'VELLORE', 'TIRUNELVELI', 'NAGERCOIL', 'THANJAVUR', 'CUDDALORE', 'KANCHIPURAM',
                           'PONDICHERRY', 'PUDUCHERRY', 'KARAIKUDI', 'SRI CITY', 'CHENNAI PORT', 'CHENNAI TI',
                           'VILUPPURAM', 'VERA VILPUR', 'HYUNDAI', 'RAMAPURAM',
                           'KOCHI', 'COCHIN', 'KOCHIN', 'THIRUVANANTHAPURAM', 'TRIVANDRUM', 'KOZHIKODE',
                           'CALICUT', 'THRISSUR', 'TRISSUR', 'KOLLAM', 'ALAPPUZHA', 'ALLEPPHY', 'PALAKKAD', 'PALLAKAD',
                           'KANNUR', 'KASARGOD', 'KOTTAYAM', 'KOTTYAM', 'PATHANAMTHITTA', 'KAYAMKULAM', 'MALAPPURAM', 'MALLAPURAM',
                           'ERNAKULAM', 'MUVATTUPUZHA',
                           'HYDERABAD', 'SECUNDERABAD', 'VIJAYAWADA', 'VISAKHAPATNAM', 'VISHAKHAPATNAM',
                           'TIRUPATI', 'GUNTUR', 'NELLORE', 'KURNOOL', 'KADAPA', 'ANANTAPUR', 'ANANTHPUR', 'ONGOLE',
                           'RAJAHMUNDRY', 'KAKINADA', 'BHIMAVARAM', 'SRIKAKULAM', 'ANAKAPALLI', 'KIA',
                           'WARANGAL', 'KARIMNAGAR', 'NIZAMABAD', 'KHAMMAM', 'NALGONDA', 'MAHBUBNAGAR',
                           'NIRMAL', 'ZAHEERABAD', 'ADONI']

            # Check each zone
            for city_check in north_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'North'

            for city_check in east_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'East'

            for city_check in west_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'West'

            for city_check in south_cities:
                if city_check in city_upper or city_upper in city_check:
                    return 'South'

            return 'Other'

        @st.fragment(run_every=REFRESH_15_MIN)
        def zone_view_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()
            frag_df = st.session_state.df
            frag_vendor_df = st.session_state.vendor_df
            # Re-filter for selected month
            frag_month_df = frag_df[(frag_df['LoadingDateOnly'] >= month_start.date()) & (frag_df['LoadingDateOnly'] <= month_end.date())]
            if selected_party != 'All':
                frag_month_df = frag_month_df[frag_month_df['DisplayParty'] == selected_party]

            # Filter loaded trips only (same as summary box logic)
            loaded_df = frag_month_df[
                (frag_month_df['TripStatus'] != 'Empty') &
                (frag_month_df['DisplayParty'] != '') &
                (frag_month_df['DisplayParty'].notna())
            ].copy()

            # Extract Origin and Destination from Route
            loaded_df['Origin'] = loaded_df['Route'].apply(lambda x: str(x).split(' - ')[0].strip() if ' - ' in str(x) else str(x).strip())
            loaded_df['Destination'] = loaded_df['Route'].apply(lambda x: str(x).split(' - ')[1].strip() if ' - ' in str(x) and len(str(x).split(' - ')) > 1 else '')

            # Map to zones
            loaded_df['Origin_Zone'] = loaded_df['Origin'].apply(get_zone)
            loaded_df['Dest_Zone'] = loaded_df['Destination'].apply(get_zone)

            # Create pivot tables (including Other for unmapped cities)
            zones = ['East', 'North', 'South', 'West']

            # Build the zone matrix for Cars Lifted
            cars_matrix = {}
            for origin_zone in zones:
                cars_matrix[origin_zone] = {}
                for dest_zone in zones:
                    count = loaded_df[(loaded_df['Origin_Zone'] == origin_zone) & (loaded_df['Dest_Zone'] == dest_zone)]['CarQty'].sum()
                    cars_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

            # Build the zone matrix for Trips Count
            trips_matrix = {}
            for origin_zone in zones:
                trips_matrix[origin_zone] = {}
                for dest_zone in zones:
                    count = len(loaded_df[(loaded_df['Origin_Zone'] == origin_zone) & (loaded_df['Dest_Zone'] == dest_zone)])
                    trips_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

            # Function to get text color (uniform white color for all values)
            def get_text_color(val, max_val):
                return 'color: white;'

            # Get max values for color scaling
            cars_values = [v for row in cars_matrix.values() for v in row.values() if v > 0]
            trips_values = [v for row in trips_matrix.values() for v in row.values() if v > 0]
            max_cars = max(cars_values) if cars_values else 1
            max_trips = max(trips_values) if trips_values else 1

            # Calculate actual totals from loaded_df (to match summary box)
            actual_total_trips = len(loaded_df)
            actual_total_cars = int(loaded_df['CarQty'].sum())

            # Function to build zone table HTML
            def build_zone_table(matrix, title, max_val, actual_grand_total):
                html = f"""
                <h4 style="color: white; margin-bottom: 10px;">{title}</h4>
                <style>
                    .zone-table {{ width: 100%; border-collapse: collapse; font-size: 14px; border: 2px solid #3b82f6; }}
                    .zone-table th {{ background-color: #1e3a5f; color: white; padding: 12px; text-align: center; border: 1px solid #3b82f6; }}
                    .zone-table td {{ padding: 10px; border: 1px solid #2d3748; color: white; text-align: center; }}
                    .zone-table tr:nth-child(even) {{ background-color: #1a1f2e; }}
                    .zone-table tr:nth-child(odd) {{ background-color: #0e1117; }}
                    .zone-table .total-row {{ background-color: #1e40af !important; font-weight: bold; }}
                    .zone-table .total-row td {{ border: 1px solid #3b82f6; }}
                    .zone-table .row-header {{ text-align: left; font-weight: bold; font-style: italic; }}
                    .zone-table .grand-total {{ color: #fbbf24; font-weight: bold; }}
                </style>
                <table class="zone-table">
                    <thead>
                        <tr>
                            <th style="font-style: italic;">Origin Zone</th>
                """
                for dest_zone in zones:
                    html += f'<th>{dest_zone}</th>'
                html += '<th>Grand Total</th></tr></thead><tbody>'

                col_totals = {z: 0 for z in zones}

                for origin_zone in zones:
                    row_total = 0
                    html += f'<tr><td class="row-header">{origin_zone}</td>'
                    for dest_zone in zones:
                        val = matrix[origin_zone][dest_zone]
                        if val > 0:
                            row_total += val
                            col_totals[dest_zone] += val
                            text_style = get_text_color(val, max_val)
                            html += f'<td style="{text_style}">{val}</td>'
                        else:
                            html += '<td></td>'
                    html += f'<td style="font-weight: bold; color: white;">{row_total}</td></tr>'

                html += '<tr class="total-row"><td class="row-header">Grand Total</td>'
                for dest_zone in zones:
                    html += f'<td class="grand-total">{col_totals[dest_zone]}</td>'
                html += f'<td class="grand-total">{actual_grand_total}</td></tr>'

                html += '</tbody></table>'
                return html

            # Display both tables side by side
            col_table1, col_table2 = st.columns(2)

            with col_table1:
                st.markdown("#### No. of Cars Lifted")
                cars_html = build_zone_table(cars_matrix, "", max_cars, actual_total_cars)
                components.html(cars_html, height=280)

            with col_table2:
                st.markdown("#### No. of Loaded Trips")
                trips_html = build_zone_table(trips_matrix, "", max_trips, actual_total_trips)
                components.html(trips_html, height=280)

            # Zone Legend - States mapped to each zone
            with st.expander("Zone Mapping (States)", expanded=False):
                zone_legend_html = """
                <style>
                    .zone-legend { font-size: 13px; color: white; }
                    .zone-legend table { width: 100%; border-collapse: collapse; }
                    .zone-legend td { padding: 8px 12px; border-bottom: 1px solid #2d3748; vertical-align: top; }
                    .zone-legend .zone-name { font-weight: bold; color: #3b82f6; width: 80px; }
                    .zone-legend .states { color: #d1d5db; }
                </style>
                <div class="zone-legend">
                    <table>
                        <tr>
                            <td class="zone-name">North</td>
                            <td class="states">Delhi NCR, Punjab, Haryana, Himachal Pradesh, J&K, Uttarakhand, Rajasthan, Uttar Pradesh</td>
                        </tr>
                        <tr>
                            <td class="zone-name">East</td>
                            <td class="states">West Bengal, Bihar, Jharkhand, Odisha, Assam, NE States, Chhattisgarh</td>
                        </tr>
                        <tr>
                            <td class="zone-name">West</td>
                            <td class="states">Maharashtra, Gujarat, Goa, Madhya Pradesh</td>
                        </tr>
                        <tr>
                            <td class="zone-name">South</td>
                            <td class="states">Karnataka, Tamil Nadu, Kerala, Andhra Pradesh, Telangana</td>
                        </tr>
                    </table>
                </div>
                """
                components.html(zone_legend_html, height=180)

            # Vendor Zone Tables
            st.markdown("---")
            st.markdown("#### Vendor Zone View")

            if not frag_vendor_df.empty:
                # Filter vendor data for the month
                vendor_zone_df = frag_vendor_df[
                    (frag_vendor_df['CNDate'] >= pd.Timestamp(month_start.date())) &
                    (frag_vendor_df['CNDate'] < pd.Timestamp(month_end.date()) + pd.Timedelta(days=1))
                ].copy()

                if not vendor_zone_df.empty:
                    # Apply vendor mapping
                    vendor_zone_df['MappedParty'] = vendor_zone_df.apply(
                        lambda row: get_vendor_client_mapping(row['BillingParty'], row.get('Origin')), axis=1
                    )
                    vendor_zone_df = vendor_zone_df[vendor_zone_df['MappedParty'].notna()]

                    if not vendor_zone_df.empty:
                        # Extract Origin and Destination from Route
                        vendor_zone_df['OriginCity'] = vendor_zone_df['Route'].apply(lambda x: str(x).split(' - ')[0].strip() if ' - ' in str(x) else str(x).strip())
                        vendor_zone_df['DestCity'] = vendor_zone_df['Route'].apply(lambda x: str(x).split(' - ')[1].strip() if ' - ' in str(x) and len(str(x).split(' - ')) > 1 else '')

                        # Map to zones
                        vendor_zone_df['Origin_Zone'] = vendor_zone_df['OriginCity'].apply(get_zone)
                        vendor_zone_df['Dest_Zone'] = vendor_zone_df['DestCity'].apply(get_zone)

                        # Build vendor zone matrix for Cars Lifted
                        vendor_cars_matrix = {}
                        for origin_zone in zones:
                            vendor_cars_matrix[origin_zone] = {}
                            for dest_zone in zones:
                                count = vendor_zone_df[(vendor_zone_df['Origin_Zone'] == origin_zone) & (vendor_zone_df['Dest_Zone'] == dest_zone)]['CarQty'].sum()
                                vendor_cars_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

                        # Build vendor zone matrix for Trips Count (unique date + vehicle combinations)
                        vendor_trips_matrix = {}
                        for origin_zone in zones:
                            vendor_trips_matrix[origin_zone] = {}
                            for dest_zone in zones:
                                zone_data = vendor_zone_df[(vendor_zone_df['Origin_Zone'] == origin_zone) & (vendor_zone_df['Dest_Zone'] == dest_zone)]
                                # Count unique combinations of CNDate + VehicleNo
                                if not zone_data.empty:
                                    count = zone_data.groupby(['CNDateOnly', 'VehicleNo']).ngroups
                                else:
                                    count = 0
                                vendor_trips_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

                        # Get max values for scaling
                        vendor_cars_values = [v for row in vendor_cars_matrix.values() for v in row.values() if v > 0]
                        vendor_trips_values = [v for row in vendor_trips_matrix.values() for v in row.values() if v > 0]
                        max_vendor_cars = max(vendor_cars_values) if vendor_cars_values else 1
                        max_vendor_trips = max(vendor_trips_values) if vendor_trips_values else 1

                        # Calculate vendor totals
                        vendor_total_cars = int(vendor_zone_df['CarQty'].sum())
                        # Trips = unique combinations of date + vehicle
                        vendor_total_trips = vendor_zone_df.groupby(['CNDateOnly', 'VehicleNo']).ngroups

                        # Function to build vendor zone table HTML (purple theme)
                        def build_vendor_zone_table(matrix, title, max_val, actual_grand_total):
                            html = f"""
                            <style>
                                .vendor-zone-table {{ width: 100%; border-collapse: collapse; font-size: 14px; border: 2px solid #7c3aed; }}
                                .vendor-zone-table th {{ background-color: #5b21b6; color: white; padding: 12px; text-align: center; border: 1px solid #7c3aed; }}
                                .vendor-zone-table td {{ padding: 10px; border: 1px solid #4c1d95; color: white; text-align: center; }}
                                .vendor-zone-table tr:nth-child(even) {{ background-color: #1a1f2e; }}
                                .vendor-zone-table tr:nth-child(odd) {{ background-color: #0e1117; }}
                                .vendor-zone-table .total-row {{ background-color: #7c3aed !important; font-weight: bold; }}
                                .vendor-zone-table .total-row td {{ border: 1px solid #7c3aed; }}
                                .vendor-zone-table .row-header {{ text-align: left; font-weight: bold; font-style: italic; }}
                                .vendor-zone-table .grand-total {{ color: #fbbf24; font-weight: bold; }}
                            </style>
                            <table class="vendor-zone-table">
                                <thead>
                                    <tr>
                                        <th style="font-style: italic;">Origin Zone</th>
                            """
                            for dest_zone in zones:
                                html += f'<th>{dest_zone}</th>'
                            html += '<th>Grand Total</th></tr></thead><tbody>'

                            col_totals = {z: 0 for z in zones}

                            for origin_zone in zones:
                                row_total = 0
                                html += f'<tr><td class="row-header">{origin_zone}</td>'
                                for dest_zone in zones:
                                    val = matrix[origin_zone][dest_zone]
                                    if val > 0:
                                        row_total += val
                                        col_totals[dest_zone] += val
                                        html += f'<td style="color: #a78bfa;">{val}</td>'
                                    else:
                                        html += '<td></td>'
                                html += f'<td style="font-weight: bold; color: white;">{row_total}</td></tr>'

                            html += '<tr class="total-row"><td class="row-header">Grand Total</td>'
                            for dest_zone in zones:
                                html += f'<td class="grand-total">{col_totals[dest_zone]}</td>'
                            html += f'<td class="grand-total">{actual_grand_total}</td></tr>'

                            html += '</tbody></table>'
                            return html

                        # Display vendor tables side by side
                        col_vendor1, col_vendor2 = st.columns(2)

                        with col_vendor1:
                            st.markdown("##### Vendor - Cars Lifted")
                            vendor_cars_html = build_vendor_zone_table(vendor_cars_matrix, "", max_vendor_cars, vendor_total_cars)
                            components.html(vendor_cars_html, height=280)

                        with col_vendor2:
                            st.markdown("##### Vendor - Trips")
                            vendor_trips_html = build_vendor_zone_table(vendor_trips_matrix, "", max_vendor_trips, vendor_total_trips)
                            components.html(vendor_trips_html, height=280)
                    else:
                        st.info("No mapped vendor data for this month.")
                else:
                    st.info("No vendor data for this month.")
            else:
                st.info("No vendor data available.")

        zone_view_fragment()

    with tab5:
        st.markdown("### NSK/Ckn Round Trips - Round Trip Analysis")
        st.caption("*NSK/Ckn-north dedicated vehicles - Loaded from Pune/Nashik with return empty trips*")

        @st.fragment(run_every=REFRESH_15_MIN)
        def cinder_trips_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()

            # TK4 vehicles - special expense rates
            tk4_vehicles = ['2399 NL01N', '2396 NL01N', '2398 NL01N', '2397 NL01N', '3909 NL01N',
                           '3910 NL01N', '3907 NL01N', '3906 NL01N', '3908 NL01N', '7524 NL01N',
                           '7526 NL01N', '7525 NL01N', '7529 NL01N', '7527 NL01N', '7530 NL01N',
                           '7528 NL01N', '7522 NL01N', '7523 NL01N', '7521 NL01N', '2400 NL01N']

            # East Zone cities for expense calculation
            east_zone_cities = ['KOLKATA', 'HOWRAH', 'SILIGURI', 'ASANSOL', 'DURGAPUR', 'KHARAGPUR', 'MALDA',
                               'BARDHAMAN', 'BARDDHAMAN', 'BARDHMAN', 'BEHRAMPORE', 'BERHAMPORE', 'BEHRAMPUR',
                               'COOCHBEHAR', 'ALIPURDUAR', 'BUNIYADPUR', 'MOGRA', 'UPARNAGAR',
                               'PATNA', 'MUZAFFARPUR', 'GAYA', 'BHAGALPUR', 'DARBHANGA', 'BEGUSARAI', 'CHAPRA',
                               'MOTIHARI', 'PURNIA', 'SAMASTIPUR', 'BIHAR SHARIF', 'SAHARSA', 'SHARSHA', 'SIWAN',
                               'GOPALGANJ', 'GOPALGUNJ', 'ARRAH', 'ARAH', 'AURANGABAD (BIHAR)', 'VAISHALI',
                               'KISHANGANJ', 'JAMALPUR', 'ANISABAD', 'ANISHABAD', 'SHERGHATI',
                               'RANCHI', 'JAMSHEDPUR', 'DHANBAD', 'BOKARO', 'HAZARIBAGH', 'HAZARIBAG', 'DEOGHAR',
                               'DEOGARH', 'DALTONGANJ', 'BARHI(JH)', 'GIRIDIH',
                               'BHUBANESHWAR', 'CUTTACK', 'ROURKELA', 'SAMBALPUR', 'BERHAMPUR', 'BRAHMAPUR',
                               'BALASORE', 'PURI', 'ANGUL', 'PANIKOILI', 'KEONJHAR', 'JEYPORE', 'JAYPORE',
                               'GUWAHATI', 'TEZPUR', 'DIBRUGARH', 'JORHAT', 'NAGAON', 'BONGAIGAON',
                               'NORTH LAKHIMPUR', 'SILCHAR', 'SHILLONG', 'GANGTOK', 'DIMAPUR', 'NAHARLAGUN',
                               'WEST CHAMPARAN', 'AGARTALA', 'IMPHAL', 'PORT BLAIR',
                               'RAIPUR', 'BILASPUR', 'BHILAI', 'KORBA', 'RAJNANDGAON', 'DURG', 'JAGDALPUR',
                               'AMBIKAPUR', 'KANKER']

            def get_expense_rates(vehicle_no, destination):
                """Get loaded and empty expense rates based on vehicle and destination"""
                # Check if TK4 vehicle
                if vehicle_no in tk4_vehicles:
                    return 30, 29  # TK4: loaded=30, empty=29

                # Check if destination is in East Zone
                if destination:
                    dest_upper = destination.upper().strip()
                    for city in east_zone_cities:
                        if city in dest_upper or dest_upper in city:
                            return 43.5, 39  # East Zone: loaded=43.5, empty=39

                # Default rates
                return 45.6, 40  # Default: loaded=45.6, empty=40

            # Load NSK/Ckn-north dedicated vehicles (exactly 20 vehicles)
            nsk_ckn_vehicles = load_vehicles_by_type('NSK/Ckn-north dedicated')

            if not nsk_ckn_vehicles:
                st.info("No NSK/Ckn-north dedicated vehicles found.")
                return

            st.caption(f"*Showing {len(nsk_ckn_vehicles)} NSK/Ckn-north dedicated vehicles*")

            # Use month_df which is filtered by sidebar month filter
            frag_df = month_df.copy()

            # Filter for exact NSK/Ckn vehicles only
            vehicle_df = frag_df[frag_df['VehicleNo'].isin(nsk_ckn_vehicles)].copy()

            if len(vehicle_df) == 0:
                st.info("No trips found for NSK/Ckn-north dedicated vehicles.")
                return

            # Extract Origin and Destination from Route
            def extract_origin_dest(route):
                if pd.isna(route) or route == '':
                    return None, None
                parts = str(route).split(' - ', 1)
                if len(parts) == 2:
                    return parts[0].strip(), parts[1].strip()
                return route, None

            vehicle_df[['Origin', 'Destination']] = vehicle_df['Route'].apply(
                lambda x: pd.Series(extract_origin_dest(x))
            )

            # Sort by vehicle and loading date
            vehicle_df = vehicle_df.sort_values(['VehicleNo', 'LoadingDate'], ascending=[True, True])

            # Define Pune/Nashik patterns
            pune_nashik_pattern = r'(?i)(pune|nashik|chakan|pimpri|ranjangaon|talegaon)'

            # Find round trips
            round_trips = []

            for vehicle in vehicle_df['VehicleNo'].unique():
                v_trips = vehicle_df[vehicle_df['VehicleNo'] == vehicle].reset_index(drop=True)

                i = 0
                while i < len(v_trips) - 1:
                    current_trip = v_trips.iloc[i]
                    next_trip = v_trips.iloc[i + 1]

                    # Check if current trip is Loaded and starts from Pune/Nashik
                    if (current_trip['TripStatus'] == 'Loaded' and
                        current_trip['Origin'] and
                        pd.notna(current_trip['Origin']) and
                        bool(pd.Series([current_trip['Origin']]).str.contains(pune_nashik_pattern, regex=True).iloc[0])):

                        # Check if next trip is Empty and returns to Pune/Nashik
                        if (next_trip['TripStatus'] == 'Empty' and
                            next_trip['Destination'] and
                            pd.notna(next_trip['Destination']) and
                            bool(pd.Series([next_trip['Destination']]).str.contains(pune_nashik_pattern, regex=True).iloc[0])):

                            # Check if empty trip origin matches loaded trip destination
                            loaded_dest = str(current_trip['Destination']).upper().strip() if current_trip['Destination'] else ''
                            empty_origin = str(next_trip['Origin']).upper().strip() if next_trip['Origin'] else ''

                            # Fuzzy match - check if they share common words
                            if loaded_dest and empty_origin and (
                                loaded_dest in empty_origin or
                                empty_origin in loaded_dest or
                                loaded_dest.split()[0] == empty_origin.split()[0] if loaded_dest and empty_origin else False
                            ):
                                # Get distances
                                loaded_distance = current_trip['Distance'] if pd.notna(current_trip['Distance']) else 0
                                empty_distance = next_trip['Distance'] if pd.notna(next_trip['Distance']) else 0

                                # Calculate profitability - get rates based on vehicle and destination
                                import math
                                loaded_rate, empty_rate = get_expense_rates(vehicle, loaded_dest)
                                loaded_exp = loaded_distance * loaded_rate
                                empty_exp = empty_distance * empty_rate
                                revenue = current_trip['Freight'] if pd.notna(current_trip['Freight']) else 0
                                total_exp = loaded_exp + empty_exp
                                contribution = revenue - total_exp
                                total_distance = loaded_distance + empty_distance
                                calc_days_raw = total_distance / 350 if total_distance > 0 else 0
                                calc_days = math.ceil(calc_days_raw) if calc_days_raw > 0 else 0  # Round up
                                per_day_contribution = contribution / calc_days if calc_days > 0 else 0

                                round_trips.append({
                                    'VehicleNo': vehicle,
                                    'Loaded_Date': current_trip['LoadingDate'],
                                    'Loaded_Route': current_trip['Route'],
                                    'Loaded_Party': current_trip['NewPartyName'] or current_trip['Party'],
                                    'Loaded_Cars': current_trip['CarQty'],
                                    'Loaded_Freight': current_trip['Freight'],
                                    'Empty_Date': next_trip['LoadingDate'],
                                    'Empty_Route': next_trip['Route'],
                                    'Days_Gap': (next_trip['LoadingDate'].date() - current_trip['LoadingDate'].date()).days if pd.notna(next_trip['LoadingDate']) and pd.notna(current_trip['LoadingDate']) else None,
                                    'Loaded_Distance': loaded_distance,
                                    'Empty_Distance': empty_distance,
                                    'Total_Distance': total_distance,
                                    'Loaded_Exp': loaded_exp,
                                    'Empty_Exp': empty_exp,
                                    'Revenue': revenue,
                                    'Contribution': contribution,
                                    'Calc_Days': calc_days,
                                    'Per_Day_Contribution': per_day_contribution
                                })
                                i += 2  # Skip both trips
                                continue
                    i += 1

            if round_trips:
                round_df = pd.DataFrame(round_trips)

                # Count trips by profitability category
                green_count = len(round_df[round_df['Per_Day_Contribution'] >= 7000])
                amber_count = len(round_df[(round_df['Per_Day_Contribution'] >= 5000) & (round_df['Per_Day_Contribution'] < 7000)])
                red_count = len(round_df[(round_df['Per_Day_Contribution'] >= 3000) & (round_df['Per_Day_Contribution'] < 5000)])
                not_profit_count = len(round_df[round_df['Per_Day_Contribution'] < 3000])

                # Summary metrics
                col1, col2, col3, col4, col5 = st.columns(5)
                with col1:
                    st.metric("Total Round Trips", len(round_df))
                with col2:
                    st.metric("Total Cars Lifted", int(round_df['Loaded_Cars'].sum()))
                with col3:
                    st.metric("Total Revenue", f"₹{round_df['Revenue'].sum()/100000:.2f}L")
                with col4:
                    st.metric("Total Contribution", f"₹{round_df['Contribution'].sum()/100000:.2f}L")
                with col5:
                    avg_per_day = round_df['Per_Day_Contribution'].mean()
                    st.metric("Avg Per Day Contrib.", f"₹{avg_per_day:,.0f}" if pd.notna(avg_per_day) else "N/A")

                # Profitability breakdown
                st.markdown(f"""
                <div style="display: flex; gap: 20px; margin: 10px 0 20px 0;">
                    <span style="background: #166534; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🟢 Green (>₹7K): {green_count}</span>
                    <span style="background: #b45309; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🟡 Amber (₹5-7K): {amber_count}</span>
                    <span style="background: #991b1b; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🔴 Red (₹3-5K): {red_count}</span>
                    <span style="background: #374151; color: #9ca3af; padding: 8px 16px; border-radius: 5px; font-weight: bold;">⚫ Not Profitable (<₹3K): {not_profit_count}</span>
                </div>
                """, unsafe_allow_html=True)

                st.markdown("---")
                st.markdown("#### Round Trip Profitability Details")

                # Create HTML table with color coding
                import streamlit.components.v1 as components

                # Build HTML table
                html_table = """
                <style>
                    body { background-color: #0e1117; }
                    .table-container { overflow-x: auto; background-color: #0e1117; }
                    .profit-table { width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 12px; background-color: #0e1117; table-layout: fixed; }
                    .profit-table th { background: #1e3a5f; color: #ffffff; padding: 8px 6px; text-align: center; border-bottom: 2px solid #2d5a8b; white-space: nowrap; }
                    .profit-table td { padding: 6px; border-bottom: 1px solid #2d3748; white-space: nowrap; color: #ffffff; background-color: #1a1a2e; text-align: center; overflow: hidden; text-overflow: ellipsis; }
                    .profit-table tr:hover td { background: #252540; }
                    .green { background: #166534 !important; color: white; font-weight: bold; }
                    .amber { background: #b45309 !important; color: white; font-weight: bold; }
                    .red { background: #991b1b !important; color: white; font-weight: bold; }
                    .not-profit { background: #374151 !important; color: #9ca3af; font-weight: bold; }
                    .left-align { text-align: left; }
                </style>
                <div class="table-container">
                <table class="profit-table">
                <colgroup>
                    <col style="width: 9%;">
                    <col style="width: 8%;">
                    <col style="width: 16%;">
                    <col style="width: 18%;">
                    <col style="width: 5%;">
                    <col style="width: 8%;">
                    <col style="width: 7%;">
                    <col style="width: 8%;">
                    <col style="width: 8%;">
                    <col style="width: 5%;">
                    <col style="width: 8%;">
                </colgroup>
                <thead>
                    <tr>
                        <th>Vehicle</th>
                        <th>Date</th>
                        <th>Loaded Route</th>
                        <th>Party</th>
                        <th>Cars</th>
                        <th>Revenue</th>
                        <th>Dist</th>
                        <th>Expense</th>
                        <th>Contrib.</th>
                        <th>Days</th>
                        <th>Per Day</th>
                    </tr>
                </thead>
                <tbody>
                """

                for _, row in round_df.iterrows():
                    per_day = row['Per_Day_Contribution']
                    if per_day >= 7000:
                        color_class = 'green'
                    elif per_day >= 5000:
                        color_class = 'amber'
                    elif per_day >= 3000:
                        color_class = 'red'
                    else:
                        color_class = 'not-profit'

                    loaded_date = pd.to_datetime(row['Loaded_Date']).strftime('%d-%b-%Y') if pd.notna(row['Loaded_Date']) else ''
                    revenue_fmt = f"₹{row['Revenue']/100000:.2f}L" if row['Revenue'] > 0 else "₹0"
                    total_exp = row['Loaded_Exp'] + row['Empty_Exp']
                    total_exp_fmt = f"₹{total_exp/100000:.2f}L"
                    contrib_fmt = f"₹{row['Contribution']/100000:.2f}L"
                    per_day_fmt = f"₹{per_day:,.0f}"

                    html_table += f"""
                    <tr>
                        <td>{row['VehicleNo']}</td>
                        <td>{loaded_date}</td>
                        <td class="left-align" title="{row['Loaded_Route']}">{row['Loaded_Route']}</td>
                        <td class="left-align" title="{row['Loaded_Party'] or ''}">{row['Loaded_Party'] or ''}</td>
                        <td>{int(row['Loaded_Cars'])}</td>
                        <td>{revenue_fmt}</td>
                        <td>{int(row['Total_Distance'])} km</td>
                        <td>{total_exp_fmt}</td>
                        <td>{contrib_fmt}</td>
                        <td>{int(row['Calc_Days'])}</td>
                        <td class="{color_class}">{per_day_fmt}</td>
                    </tr>
                    """

                html_table += "</tbody></table></div>"

                components.html(html_table, height=min(len(round_df) * 40 + 100, 500), scrolling=True)
            else:
                st.info("No round trips found matching the criteria (Loaded from Pune/Nashik → Empty return to Pune/Nashik)")

            # Also show all trips for reference
            st.markdown("---")
            with st.expander("📋 All Trips for NSK/Ckn Vehicles"):
                all_trips_display = vehicle_df[['VehicleNo', 'LoadingDate', 'Route', 'TripStatus', 'NewPartyName', 'CarQty', 'Freight']].copy()
                all_trips_display['LoadingDate'] = pd.to_datetime(all_trips_display['LoadingDate']).dt.strftime('%d-%b-%Y')
                all_trips_display.columns = ['Vehicle', 'Date', 'Route', 'Status', 'Party', 'Cars', 'Freight']
                st.dataframe(all_trips_display.sort_values('Date', ascending=False), use_container_width=True, hide_index=True)

        cinder_trips_fragment()

    with tab6:
        st.markdown("### Trip Profitability - All Loaded Trips")
        st.caption("*Profitability analysis for all loaded trips*")

        @st.fragment(run_every=REFRESH_15_MIN)
        def trip_profitability_fragment():
            import math
            # Auto-refresh: reload data from database
            refresh_session_data()

            # TK4 vehicles - special expense rates
            tk4_vehicles = ['2399 NL01N', '2396 NL01N', '2398 NL01N', '2397 NL01N', '3909 NL01N',
                           '3910 NL01N', '3907 NL01N', '3906 NL01N', '3908 NL01N', '7524 NL01N',
                           '7526 NL01N', '7525 NL01N', '7529 NL01N', '7527 NL01N', '7530 NL01N',
                           '7528 NL01N', '7522 NL01N', '7523 NL01N', '7521 NL01N', '2400 NL01N']

            # East Zone cities for expense calculation
            east_zone_cities = ['KOLKATA', 'HOWRAH', 'SILIGURI', 'ASANSOL', 'DURGAPUR', 'KHARAGPUR', 'MALDA',
                               'BARDHAMAN', 'BARDDHAMAN', 'BARDHMAN', 'BEHRAMPORE', 'BERHAMPORE', 'BEHRAMPUR',
                               'COOCHBEHAR', 'ALIPURDUAR', 'BUNIYADPUR', 'MOGRA', 'UPARNAGAR',
                               'PATNA', 'MUZAFFARPUR', 'GAYA', 'BHAGALPUR', 'DARBHANGA', 'BEGUSARAI', 'CHAPRA',
                               'MOTIHARI', 'PURNIA', 'SAMASTIPUR', 'BIHAR SHARIF', 'SAHARSA', 'SHARSHA', 'SIWAN',
                               'GOPALGANJ', 'GOPALGUNJ', 'ARRAH', 'ARAH', 'AURANGABAD (BIHAR)', 'VAISHALI',
                               'KISHANGANJ', 'JAMALPUR', 'ANISABAD', 'ANISHABAD', 'SHERGHATI',
                               'RANCHI', 'JAMSHEDPUR', 'DHANBAD', 'BOKARO', 'HAZARIBAGH', 'HAZARIBAG', 'DEOGHAR',
                               'DEOGARH', 'DALTONGANJ', 'BARHI(JH)', 'GIRIDIH',
                               'BHUBANESHWAR', 'CUTTACK', 'ROURKELA', 'SAMBALPUR', 'BERHAMPUR', 'BRAHMAPUR',
                               'BALASORE', 'PURI', 'ANGUL', 'PANIKOILI', 'KEONJHAR', 'JEYPORE', 'JAYPORE',
                               'GUWAHATI', 'TEZPUR', 'DIBRUGARH', 'JORHAT', 'NAGAON', 'BONGAIGAON',
                               'NORTH LAKHIMPUR', 'SILCHAR', 'SHILLONG', 'GANGTOK', 'DIMAPUR', 'NAHARLAGUN',
                               'WEST CHAMPARAN', 'AGARTALA', 'IMPHAL', 'PORT BLAIR',
                               'RAIPUR', 'BILASPUR', 'BHILAI', 'KORBA', 'RAJNANDGAON', 'DURG', 'JAGDALPUR',
                               'AMBIKAPUR', 'KANKER']

            def get_expense_rates(vehicle_no, destination):
                """Get loaded and empty expense rates based on vehicle and destination"""
                # Check if TK4 vehicle
                if vehicle_no in tk4_vehicles:
                    return 30, 29  # TK4: loaded=30, empty=29

                # Check if destination is in East Zone
                if destination:
                    dest_upper = destination.upper().strip()
                    for city in east_zone_cities:
                        if city in dest_upper or dest_upper in city:
                            return 43.5, 39  # East Zone: loaded=43.5, empty=39

                # Default rates
                return 45.6, 40  # Default: loaded=45.6, empty=40

            # Use month_df which is filtered by sidebar month filter
            frag_df = month_df.copy()

            if len(frag_df) == 0:
                st.info("No trip data available for selected month.")
                return

            # Load historical trip data for distance lookup (both empty and loaded trips)
            @st.cache_data(ttl=3600)
            def get_historical_empty_routes():
                """Get historical trip patterns with distances from all past trips"""
                try:
                    conn = get_db_connection()
                    if conn is None:
                        return {}
                    # Get distances from ALL trips (empty and loaded) for better coverage
                    query = """
                        SELECT
                            UPPER(TRIM(SPLIT_PART(route, ' - ', 1))) as origin,
                            UPPER(TRIM(SPLIT_PART(route, ' - ', 2))) as destination,
                            COUNT(*) as trip_count,
                            AVG(distance) as avg_distance,
                            trip_status
                        FROM swift_trip_log
                        WHERE route IS NOT NULL
                            AND route LIKE '%-%'
                            AND distance > 0
                        GROUP BY UPPER(TRIM(SPLIT_PART(route, ' - ', 1))), UPPER(TRIM(SPLIT_PART(route, ' - ', 2))), trip_status
                        HAVING COUNT(*) >= 1
                    """
                    hist_df = pd.read_sql_query(query, conn)
                    conn.close()

                    # Create lookup: origin -> list of (destination, avg_distance, count)
                    # Prioritize empty trips for suggestions, but use any trip for distance
                    routes = {}
                    for _, row in hist_df.iterrows():
                        origin = row['origin']
                        if origin not in routes:
                            routes[origin] = []
                        # Check if this destination already exists
                        existing = next((r for r in routes[origin] if r['destination'] == row['destination']), None)
                        if existing:
                            # Update with more data
                            existing['count'] += row['trip_count']
                            # Keep the distance (already have it)
                        else:
                            routes[origin].append({
                                'destination': row['destination'],
                                'avg_distance': row['avg_distance'],
                                'count': row['trip_count']
                        })
                    return routes
                except:
                    return {}

            historical_routes = get_historical_empty_routes()

            # Extract Origin and Destination from Route
            def extract_origin_dest(route):
                if pd.isna(route) or route == '':
                    return None, None
                parts = str(route).split(' - ', 1)
                if len(parts) == 2:
                    return parts[0].strip(), parts[1].strip()
                return route, None

            frag_df[['Origin', 'Destination']] = frag_df['Route'].apply(
                lambda x: pd.Series(extract_origin_dest(x))
            )

            # Sort by vehicle and loading date
            frag_df = frag_df.sort_values(['VehicleNo', 'LoadingDate'], ascending=[True, True])

            # Process all loaded trips
            completed_trips = []  # Trips with matched empty
            ongoing_trips = []    # Trips without empty (ongoing or with onward_route)

            for vehicle in frag_df['VehicleNo'].unique():
                v_trips = frag_df[frag_df['VehicleNo'] == vehicle].reset_index(drop=True)

                i = 0
                while i < len(v_trips):
                    current_trip = v_trips.iloc[i]

                    # Only process loaded trips
                    if current_trip['TripStatus'] != 'Loaded':
                        i += 1
                        continue

                    loaded_distance = current_trip['Distance'] if pd.notna(current_trip['Distance']) else 0
                    revenue = current_trip['Freight'] if pd.notna(current_trip['Freight']) else 0
                    loaded_dest = str(current_trip['Destination']).upper().strip() if pd.notna(current_trip['Destination']) else ''
                    party_name = current_trip['NewPartyName'] or current_trip['Party'] or ''

                    # Special handling for DC Movement trips - always green
                    if 'DC Movement' in str(party_name):
                        # DC Movement - shuttle trips, always profitable (green)
                        loaded_exp = revenue * 0.752  # 75.2% of freight
                        completed_trips.append({
                            'VehicleNo': vehicle,
                            'Loaded_Date': current_trip['LoadingDate'],
                            'Loaded_Route': current_trip['Route'],
                            'Loaded_Party': party_name,
                            'Loaded_Cars': current_trip['CarQty'],
                            'Revenue': revenue,
                            'Empty_Route': 'DC Movement (Shuttle)',
                            'Total_Distance': loaded_distance,
                            'Total_Exp': loaded_exp,
                            'Contribution': revenue - loaded_exp,
                            'Calc_Days': 1,
                            'Per_Day_Contribution': 10000,  # Always green (>7000)
                            'Status': 'DC Movement'
                        })
                        i += 1
                        continue

                    # Special handling for "By Road" vehicles - no empty trip needed
                    if 'By Road' in str(vehicle):
                        loaded_exp = revenue * 0.752  # 75.2% of freight
                        contribution = revenue - loaded_exp
                        calc_days = math.ceil(loaded_distance / 350) if loaded_distance > 0 else 1
                        per_day_contribution = contribution / calc_days if calc_days > 0 else 0

                        completed_trips.append({
                            'VehicleNo': vehicle,
                            'Loaded_Date': current_trip['LoadingDate'],
                            'Loaded_Route': current_trip['Route'],
                            'Loaded_Party': party_name,
                            'Loaded_Cars': current_trip['CarQty'],
                            'Revenue': revenue,
                            'Empty_Route': 'By Road (No Return)',
                            'Total_Distance': loaded_distance,
                            'Total_Exp': loaded_exp,
                            'Contribution': contribution,
                            'Calc_Days': calc_days,
                            'Per_Day_Contribution': per_day_contribution,
                            'Status': 'By Road'
                        })
                        i += 1
                        continue

                    # Check if next trip is matching empty
                    has_matching_empty = False
                    if i < len(v_trips) - 1:
                        next_trip = v_trips.iloc[i + 1]
                        if next_trip['TripStatus'] == 'Empty' and pd.notna(next_trip['Origin']):
                            empty_origin = str(next_trip['Origin']).upper().strip()
                            if loaded_dest and empty_origin and (
                                loaded_dest in empty_origin or
                                empty_origin in loaded_dest or
                                (loaded_dest.split()[0] == empty_origin.split()[0] if loaded_dest and empty_origin else False)
                            ):
                                has_matching_empty = True
                                empty_distance = next_trip['Distance'] if pd.notna(next_trip['Distance']) else 0
                                empty_route = next_trip['Route']

                                # Calculate profitability - get rates based on vehicle and destination
                                loaded_rate, empty_rate = get_expense_rates(vehicle, loaded_dest)
                                loaded_exp = loaded_distance * loaded_rate
                                empty_exp = empty_distance * empty_rate
                                total_exp = loaded_exp + empty_exp
                                contribution = revenue - total_exp
                                total_distance = loaded_distance + empty_distance
                                calc_days = math.ceil(total_distance / 350) if total_distance > 0 else 1
                                per_day_contribution = contribution / calc_days if calc_days > 0 else 0

                                completed_trips.append({
                                    'VehicleNo': vehicle,
                                    'Loaded_Date': current_trip['LoadingDate'],
                                    'Loaded_Route': current_trip['Route'],
                                    'Loaded_Party': current_trip['NewPartyName'] or current_trip['Party'],
                                    'Loaded_Cars': current_trip['CarQty'],
                                    'Revenue': revenue,
                                    'Empty_Route': empty_route,
                                    'Total_Distance': total_distance,
                                    'Total_Exp': total_exp,
                                    'Contribution': contribution,
                                    'Calc_Days': calc_days,
                                    'Per_Day_Contribution': per_day_contribution,
                                    'Status': 'Completed'
                                })
                                i += 2
                                continue

                    # No matching empty - check for onward_route or suggest
                    if not has_matching_empty:
                        # Branch locations - vehicles can get loaded trips from here
                        branch_locations = ['PUNE', 'HARIDWAR', 'GURGAON', 'CHENNAI', 'ANANTAPUR',
                                          'TAPUKERA', 'BANGALORE', 'BANGLORE', 'BENGALURU',
                                          'SANAND', 'NASHIK', 'BECHRAJI', 'HALOL', 'CHAKAN',
                                          'PIMPRI', 'RANJANGAON', 'BIDADI', 'KIA', 'JAMALPUR']

                        onward_route = current_trip.get('OnwardRoute', '')
                        if pd.isna(onward_route):
                            onward_route = ''

                        suggested_return = ''
                        empty_distance = loaded_distance  # Default: same as loaded

                        # Check if destination is a branch location
                        is_branch_dest = any(branch.upper() in loaded_dest for branch in branch_locations) if loaded_dest else False

                        if is_branch_dest:
                            # Destination is a branch - assume only 50 km local empty movement
                            suggested_return = f"Branch: {loaded_dest} (local)"
                            empty_distance = 50
                        elif onward_route and str(onward_route).strip():
                            # Has onward route - extract origin/destination and lookup historical distance
                            onward_parts = str(onward_route).split(' - ')
                            if len(onward_parts) >= 2:
                                onward_origin = onward_parts[0].strip().upper()
                                onward_dest = onward_parts[-1].strip().upper()
                                suggested_return = f"Onward: {onward_route}"

                                # Lookup historical distance for this onward route
                                if onward_origin in historical_routes:
                                    for route_opt in historical_routes[onward_origin]:
                                        if route_opt['destination'].upper() == onward_dest:
                                            empty_distance = route_opt['avg_distance'] if route_opt['avg_distance'] else loaded_distance
                                            break
                        else:
                            # No onward route - suggest based on historical data
                            if loaded_dest and loaded_dest in historical_routes:
                                options = historical_routes[loaded_dest]
                                # Sort by count (most frequent routes)
                                options_sorted = sorted(options, key=lambda x: x['count'], reverse=True)
                                if options_sorted:
                                    best = options_sorted[0]
                                    suggested_return = f"Suggest: {loaded_dest} → {best['destination']}"
                                    empty_distance = best['avg_distance'] if best['avg_distance'] else loaded_distance

                        # Calculate estimated profitability - get rates based on vehicle and destination
                        loaded_rate, empty_rate = get_expense_rates(vehicle, loaded_dest)
                        loaded_exp = loaded_distance * loaded_rate
                        empty_exp = empty_distance * empty_rate
                        total_exp = loaded_exp + empty_exp
                        contribution = revenue - total_exp
                        total_distance = loaded_distance + empty_distance
                        calc_days = math.ceil(total_distance / 350) if total_distance > 0 else 1
                        per_day_contribution = contribution / calc_days if calc_days > 0 else 0

                        ongoing_trips.append({
                            'VehicleNo': vehicle,
                            'Loaded_Date': current_trip['LoadingDate'],
                            'Loaded_Route': current_trip['Route'],
                            'Loaded_Party': current_trip['NewPartyName'] or current_trip['Party'],
                            'Loaded_Cars': current_trip['CarQty'],
                            'Revenue': revenue,
                            'Empty_Route': suggested_return or f"Ongoing from {loaded_dest}",
                            'Total_Distance': total_distance,
                            'Total_Exp': total_exp,
                            'Contribution': contribution,
                            'Calc_Days': calc_days,
                            'Per_Day_Contribution': per_day_contribution,
                            'Status': 'Ongoing/Estimated'
                        })

                    i += 1

            # Combine all trips
            all_trips = completed_trips + ongoing_trips

            if all_trips:
                round_df = pd.DataFrame(all_trips)

                # Filters at the top - apply to all summaries and tables
                filter_col1, filter_col2, filter_col3 = st.columns([2, 2, 4])

                with filter_col1:
                    # Date filter based on loading date
                    date_options = ['All Dates'] + sorted(round_df['Loaded_Date'].dropna().apply(lambda x: pd.to_datetime(x).strftime('%d-%b-%Y')).unique().tolist(), reverse=True)
                    selected_date = st.selectbox("📅 Loading Date", date_options, key="trip_date_filter_top")

                with filter_col2:
                    # Freight filter
                    freight_options = ['All', 'Freight = 0', 'Freight > 0']
                    selected_freight = st.selectbox("💰 Freight Filter", freight_options, key="trip_freight_filter_top")

                # Apply filters to round_df
                if selected_date != 'All Dates':
                    round_df = round_df[pd.to_datetime(round_df['Loaded_Date']).dt.strftime('%d-%b-%Y') == selected_date]

                if selected_freight == 'Freight = 0':
                    round_df = round_df[round_df['Revenue'] == 0]
                elif selected_freight == 'Freight > 0':
                    round_df = round_df[round_df['Revenue'] > 0]

                st.caption(f"*Showing {len(round_df)} trips*")
                st.markdown("---")

                # Count DC Movement trips
                dc_movement_count = len(round_df[round_df['Status'] == 'DC Movement'])

                # Count trips by profitability category (excluding DC Movement from regular counts)
                non_dc_df = round_df[round_df['Status'] != 'DC Movement']
                green_count = len(non_dc_df[non_dc_df['Per_Day_Contribution'] >= 7000])
                amber_count = len(non_dc_df[(non_dc_df['Per_Day_Contribution'] >= 5000) & (non_dc_df['Per_Day_Contribution'] < 7000)])
                red_count = len(non_dc_df[(non_dc_df['Per_Day_Contribution'] >= 3000) & (non_dc_df['Per_Day_Contribution'] < 5000)])
                not_profit_count = len(non_dc_df[non_dc_df['Per_Day_Contribution'] < 3000])

                # Count by status
                completed_count = len(round_df[round_df['Status'] == 'Completed'])
                ongoing_count = len(round_df[round_df['Status'] == 'Ongoing/Estimated'])

                # Summary metrics
                col1, col2, col3, col4, col5, col6 = st.columns(6)
                with col1:
                    st.metric("Total Trips", len(round_df))
                with col2:
                    st.metric("Completed", completed_count)
                with col3:
                    st.metric("Ongoing", ongoing_count)
                with col4:
                    st.metric("Total Revenue", f"₹{round_df['Revenue'].sum()/100000:.2f}L")
                with col5:
                    st.metric("Total Contribution", f"₹{round_df['Contribution'].sum()/100000:.2f}L")
                with col6:
                    avg_per_day = round_df['Per_Day_Contribution'].mean()
                    st.metric("Avg Per Day", f"₹{avg_per_day:,.0f}" if pd.notna(avg_per_day) else "N/A")

                # Profitability breakdown
                st.markdown(f"""
                <div style="display: flex; gap: 15px; margin: 10px 0 20px 0; flex-wrap: wrap;">
                    <span style="background: #166534; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🟢 Green (>₹7K): {green_count}</span>
                    <span style="background: #059669; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🚛 DC Movement: {dc_movement_count}</span>
                    <span style="background: #b45309; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🟡 Amber (₹5-7K): {amber_count}</span>
                    <span style="background: #991b1b; color: white; padding: 8px 16px; border-radius: 5px; font-weight: bold;">🔴 Red (₹3-5K): {red_count}</span>
                    <span style="background: #374151; color: #9ca3af; padding: 8px 16px; border-radius: 5px; font-weight: bold;">⚫ Not Profitable (<₹3K): {not_profit_count}</span>
                </div>
                """, unsafe_allow_html=True)

                # Week-wise summary
                st.markdown("#### Week-wise Summary")

                # Add week column based on loading date
                week_df = round_df.copy()
                week_df['LoadDate'] = pd.to_datetime(week_df['Loaded_Date'])

                # Calculate week number within the month (1-5)
                def get_week_of_month(dt):
                    first_day = dt.replace(day=1)
                    day_of_month = dt.day
                    # Week 1: days 1-7, Week 2: days 8-14, etc.
                    return ((day_of_month - 1) // 7) + 1

                week_df['WeekNum'] = week_df['LoadDate'].apply(get_week_of_month)
                week_df['Week_Label'] = week_df['WeekNum'].apply(lambda x: f'Week {x}')

                # Categorize trips by profitability
                def get_profit_category(row):
                    if row['Status'] == 'DC Movement':
                        return 'Green'  # DC Movement always green
                    per_day = row['Per_Day_Contribution']
                    if per_day >= 7000:
                        return 'Green'
                    elif per_day >= 5000:
                        return 'Amber'
                    elif per_day >= 3000:
                        return 'Red'
                    else:
                        return 'NotProfitable'

                week_df['ProfitCategory'] = week_df.apply(get_profit_category, axis=1)

                # Group by week
                week_summary = week_df.groupby(['WeekNum', 'Week_Label']).agg(
                    Trips=('VehicleNo', 'count'),
                    Cars=('Loaded_Cars', 'sum'),
                    Green=('ProfitCategory', lambda x: (x == 'Green').sum()),
                    Amber=('ProfitCategory', lambda x: (x == 'Amber').sum()),
                    Red=('ProfitCategory', lambda x: (x == 'Red').sum()),
                    NotProfitable=('ProfitCategory', lambda x: (x == 'NotProfitable').sum())
                ).reset_index().sort_values('WeekNum')

                # Calculate percentages
                week_summary['Green%'] = (week_summary['Green'] / week_summary['Trips'] * 100).round(1)
                week_summary['Amber%'] = (week_summary['Amber'] / week_summary['Trips'] * 100).round(1)
                week_summary['Red%'] = (week_summary['Red'] / week_summary['Trips'] * 100).round(1)
                week_summary['NotProfit%'] = (week_summary['NotProfitable'] / week_summary['Trips'] * 100).round(1)

                # Create HTML table for week summary
                week_html = """
                <style>
                    body { background-color: #0e1117; margin: 0; padding: 0; }
                    .week-container { background-color: #0e1117; padding: 5px; }
                    .week-table { width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 12px; background-color: #0e1117; }
                    .week-table th { background: #1e3a5f; color: #ffffff; padding: 8px; text-align: center; border-bottom: 2px solid #2d5a8b; }
                    .week-table td { padding: 6px 8px; border-bottom: 1px solid #2d3748; text-align: center; color: #ffffff; background-color: #1a1a2e; }
                    .week-table tr:hover td { background: #252540; }
                    .green-cell { background: #166534 !important; color: #ffffff; font-weight: bold; }
                    .amber-cell { background: #b45309 !important; color: #ffffff; font-weight: bold; }
                    .red-cell { background: #991b1b !important; color: #ffffff; font-weight: bold; }
                    .gray-cell { background: #374151 !important; color: #9ca3af; font-weight: bold; }
                </style>
                <div class="week-container">
                <table class="week-table">
                <thead>
                    <tr>
                        <th>Week</th>
                        <th>Trips</th>
                        <th>Cars</th>
                        <th>🟢 Green</th>
                        <th>Green %</th>
                        <th>🟡 Amber</th>
                        <th>Amber %</th>
                        <th>🔴 Red</th>
                        <th>Red %</th>
                        <th>⚫ Not Profit</th>
                        <th>Not Profit %</th>
                    </tr>
                </thead>
                <tbody>
                """

                for _, row in week_summary.iterrows():
                    week_html += f"""
                    <tr>
                        <td>{row['Week_Label']}</td>
                        <td>{int(row['Trips'])}</td>
                        <td>{int(row['Cars'])}</td>
                        <td class="green-cell">{int(row['Green'])}</td>
                        <td class="green-cell">{row['Green%']}%</td>
                        <td class="amber-cell">{int(row['Amber'])}</td>
                        <td class="amber-cell">{row['Amber%']}%</td>
                        <td class="red-cell">{int(row['Red'])}</td>
                        <td class="red-cell">{row['Red%']}%</td>
                        <td class="gray-cell">{int(row['NotProfitable'])}</td>
                        <td class="gray-cell">{row['NotProfit%']}%</td>
                    </tr>
                    """

                # Add total row
                total_trips = week_summary['Trips'].sum()
                total_cars = week_summary['Cars'].sum()
                total_green = week_summary['Green'].sum()
                total_amber = week_summary['Amber'].sum()
                total_red = week_summary['Red'].sum()
                total_not_profit = week_summary['NotProfitable'].sum()

                week_html += f"""
                <tr style="font-weight: bold; border-top: 3px solid #60a5fa;">
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px;">📊 Total</td>
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px;">{int(total_trips)}</td>
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px;">{int(total_cars)}</td>
                    <td class="green-cell" style="font-size: 13px;">{int(total_green)}</td>
                    <td class="green-cell" style="font-size: 13px;">{(total_green/total_trips*100):.1f}%</td>
                    <td class="amber-cell" style="font-size: 13px;">{int(total_amber)}</td>
                    <td class="amber-cell" style="font-size: 13px;">{(total_amber/total_trips*100):.1f}%</td>
                    <td class="red-cell" style="font-size: 13px;">{int(total_red)}</td>
                    <td class="red-cell" style="font-size: 13px;">{(total_red/total_trips*100):.1f}%</td>
                    <td class="gray-cell" style="font-size: 13px;">{int(total_not_profit)}</td>
                    <td class="gray-cell" style="font-size: 13px;">{(total_not_profit/total_trips*100):.1f}%</td>
                </tr>
                """

                week_html += "</tbody></table></div>"

                import streamlit.components.v1 as components
                components.html(week_html, height=min(len(week_summary) * 45 + 100, 400), scrolling=True)

                # Branch-wise Summary
                st.markdown("#### Branch-wise Summary")

                # Branch mapping function
                def get_branch(origin):
                    if pd.isna(origin) or origin == '':
                        return 'Market Load'
                    origin_upper = str(origin).upper().strip()

                    # Define branch mappings
                    branch_mappings = {
                        'NASHIK': 'Nashik',
                        'PUNE': 'Pune',
                        'TALEGAON': 'Pune',
                        'HARIDWAR': 'Haridwar',
                        'ANANTPUR': 'Anantpur',
                        'ANANTAPUR': 'Anantpur',
                        'KIA': 'Anantpur',
                        'PATNA': 'Patna',
                        'CHENNAI': 'Chennai',
                        'HYUNDAI': 'Chennai',
                        'BANGLORE': 'Banglore',
                        'BANGALORE': 'Banglore',
                        'BENGALURU': 'Banglore',
                        'SANAND': 'Sanand',
                        'ZAHEERABAD': 'Zaheerabad',
                        'FARIDABAD': 'Faridabad',
                        'TAPUKERA': 'Tapukera',
                        'TAPUKARA': 'Tapukera',
                        'BECHRAJI': 'Bechraji',
                        'GURGAON': 'Gurgaon',
                        'GURUGRAM': 'Gurgaon',
                        'DELHI': 'Gurgaon',
                        'JAMALPUR': 'Gurgaon',
                        'NOIDA': 'Gurgaon',
                        'RANJANGAON': 'Ranjangaon',
                        'HALOL': 'Halol',
                        'BIDADI': 'Banglore',
                    }

                    # Check for exact or partial match
                    for key, branch in branch_mappings.items():
                        if key in origin_upper:
                            return branch

                    return 'Market Load'

                # Add branch column
                branch_df = round_df.copy()
                # Extract origin from Loaded_Route (first part before ' - ')
                branch_df['Origin'] = branch_df['Loaded_Route'].apply(lambda x: str(x).split(' - ')[0] if pd.notna(x) else '')
                branch_df['Branch'] = branch_df['Origin'].apply(get_branch)
                branch_df['ProfitCategory'] = branch_df.apply(get_profit_category, axis=1)

                # Group by branch
                branch_summary = branch_df.groupby('Branch').agg(
                    Trips=('VehicleNo', 'count'),
                    Cars=('Loaded_Cars', 'sum'),
                    Green=('ProfitCategory', lambda x: (x == 'Green').sum()),
                    Amber=('ProfitCategory', lambda x: (x == 'Amber').sum()),
                    Red=('ProfitCategory', lambda x: (x == 'Red').sum()),
                    NotProfitable=('ProfitCategory', lambda x: (x == 'NotProfitable').sum())
                ).reset_index()

                # Define branch order
                branch_order = ['Nashik', 'Pune', 'Haridwar', 'Anantpur', 'Patna', 'Chennai',
                               'Banglore', 'Sanand', 'Zaheerabad', 'Faridabad', 'Tapukera',
                               'Bechraji', 'Gurgaon', 'Ranjangaon', 'Halol', 'Market Load']
                branch_summary['BranchOrder'] = branch_summary['Branch'].apply(
                    lambda x: branch_order.index(x) if x in branch_order else 999
                )
                branch_summary = branch_summary.sort_values('BranchOrder')

                # Calculate percentages
                branch_summary['Green%'] = (branch_summary['Green'] / branch_summary['Trips'] * 100).round(1)
                branch_summary['Amber%'] = (branch_summary['Amber'] / branch_summary['Trips'] * 100).round(1)
                branch_summary['Red%'] = (branch_summary['Red'] / branch_summary['Trips'] * 100).round(1)
                branch_summary['NotProfit%'] = (branch_summary['NotProfitable'] / branch_summary['Trips'] * 100).round(1)

                # Create HTML table for branch summary
                branch_html = """
                <style>
                    .branch-container { background-color: #0e1117; padding: 5px; }
                    .branch-table { width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 12px; background-color: #0e1117; }
                    .branch-table th { background: #1e3a5f; color: #ffffff; padding: 8px; text-align: center; border-bottom: 2px solid #2d5a8b; }
                    .branch-table td { padding: 6px 8px; border-bottom: 1px solid #2d3748; text-align: center; color: #ffffff; background-color: #1a1a2e; }
                    .branch-table tr:hover td { background: #252540; }
                    .branch-green { background: #166534 !important; color: #ffffff; font-weight: bold; }
                    .branch-amber { background: #b45309 !important; color: #ffffff; font-weight: bold; }
                    .branch-red { background: #991b1b !important; color: #ffffff; font-weight: bold; }
                    .branch-gray { background: #374151 !important; color: #9ca3af; font-weight: bold; }
                </style>
                <div class="branch-container">
                <table class="branch-table">
                <thead>
                    <tr>
                        <th>Branch</th>
                        <th>Trips</th>
                        <th>Cars</th>
                        <th>🟢 Green</th>
                        <th>Green %</th>
                        <th>🟡 Amber</th>
                        <th>Amber %</th>
                        <th>🔴 Red</th>
                        <th>Red %</th>
                        <th>⚫ Not Profit</th>
                        <th>Not Profit %</th>
                    </tr>
                </thead>
                <tbody>
                """

                for _, row in branch_summary.iterrows():
                    branch_html += f"""
                    <tr>
                        <td style="text-align: left; font-weight: bold;">{row['Branch']}</td>
                        <td>{int(row['Trips'])}</td>
                        <td>{int(row['Cars'])}</td>
                        <td class="branch-green">{int(row['Green'])}</td>
                        <td class="branch-green">{row['Green%']}%</td>
                        <td class="branch-amber">{int(row['Amber'])}</td>
                        <td class="branch-amber">{row['Amber%']}%</td>
                        <td class="branch-red">{int(row['Red'])}</td>
                        <td class="branch-red">{row['Red%']}%</td>
                        <td class="branch-gray">{int(row['NotProfitable'])}</td>
                        <td class="branch-gray">{row['NotProfit%']}%</td>
                    </tr>
                    """

                # Add total row for branch summary
                branch_total_trips = branch_summary['Trips'].sum()
                branch_total_cars = branch_summary['Cars'].sum()
                branch_total_green = branch_summary['Green'].sum()
                branch_total_amber = branch_summary['Amber'].sum()
                branch_total_red = branch_summary['Red'].sum()
                branch_total_not_profit = branch_summary['NotProfitable'].sum()

                branch_html += f"""
                <tr style="font-weight: bold; border-top: 3px solid #60a5fa;">
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px; text-align: left;">📊 Total</td>
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px;">{int(branch_total_trips)}</td>
                    <td style="background: #1e40af !important; color: #ffffff; font-size: 13px;">{int(branch_total_cars)}</td>
                    <td class="branch-green" style="font-size: 13px;">{int(branch_total_green)}</td>
                    <td class="branch-green" style="font-size: 13px;">{(branch_total_green/branch_total_trips*100):.1f}%</td>
                    <td class="branch-amber" style="font-size: 13px;">{int(branch_total_amber)}</td>
                    <td class="branch-amber" style="font-size: 13px;">{(branch_total_amber/branch_total_trips*100):.1f}%</td>
                    <td class="branch-red" style="font-size: 13px;">{int(branch_total_red)}</td>
                    <td class="branch-red" style="font-size: 13px;">{(branch_total_red/branch_total_trips*100):.1f}%</td>
                    <td class="branch-gray" style="font-size: 13px;">{int(branch_total_not_profit)}</td>
                    <td class="branch-gray" style="font-size: 13px;">{(branch_total_not_profit/branch_total_trips*100):.1f}%</td>
                </tr>
                """

                branch_html += "</tbody></table></div>"

                components.html(branch_html, height=min(len(branch_summary) * 40 + 100, 600), scrolling=True)

                st.markdown("---")
                st.markdown("#### Trip Profitability Details")

                # Create export DataFrame for download
                export_df = round_df.copy()
                export_df['Date'] = pd.to_datetime(export_df['Loaded_Date']).dt.strftime('%d-%b-%Y')
                export_df['Revenue_K'] = export_df['Revenue'] / 1000
                export_df['Expense_K'] = export_df['Total_Exp'] / 1000
                export_df['Contribution_K'] = export_df['Contribution'] / 1000
                export_df['Per_Day'] = export_df['Per_Day_Contribution'].round(0)

                # Select and rename columns for export
                export_cols = export_df[['VehicleNo', 'Date', 'Loaded_Route', 'Loaded_Party', 'Empty_Route',
                                        'Loaded_Cars', 'Revenue_K', 'Total_Distance', 'Expense_K',
                                        'Contribution_K', 'Calc_Days', 'Per_Day', 'Status']]
                export_cols.columns = ['Vehicle', 'Date', 'Loaded Route', 'Party', 'Return/Suggest',
                                      'Cars', 'Revenue (K)', 'Distance', 'Expense (K)',
                                      'Contribution (K)', 'Days', 'Per Day', 'Status']

                # Download button
                csv = export_cols.to_csv(index=False)
                st.download_button(
                    label="📥 Download Trip Profitability Data",
                    data=csv,
                    file_name="trip_profitability.csv",
                    mime="text/csv",
                )

                # Create HTML table with color coding
                import streamlit.components.v1 as components

                html_table = """
                <style>
                    body { background-color: #0e1117; }
                    .table-container2 { overflow-x: auto; overflow-y: auto; max-height: 500px; background-color: #0e1117; }
                    .profit-table2 { width: 100%; border-collapse: collapse; font-family: sans-serif; font-size: 11px; background-color: #0e1117; table-layout: fixed; }
                    .profit-table2 th { background: #1e3a5f; color: #ffffff; padding: 6px 4px; text-align: center; border-bottom: 2px solid #2d5a8b; white-space: nowrap; position: sticky; top: 0; z-index: 10; }
                    .profit-table2 td { padding: 5px 4px; border-bottom: 1px solid #2d3748; white-space: nowrap; color: #ffffff; background-color: #1a1a2e; text-align: center; overflow: hidden; text-overflow: ellipsis; }
                    .profit-table2 tr:hover td { background: #252540; }
                    .green3 { background: #166534 !important; color: white; font-weight: bold; }
                    .amber3 { background: #b45309 !important; color: white; font-weight: bold; }
                    .red3 { background: #991b1b !important; color: white; font-weight: bold; }
                    .not-profit3 { background: #374151 !important; color: #9ca3af; font-weight: bold; }
                    .left-align2 { text-align: left; }
                    .status-done { background: #1e40af !important; color: white; font-size: 10px; }
                    .status-ongoing { background: #7c3aed !important; color: white; font-size: 10px; }
                    .status-dc { background: #059669 !important; color: white; font-size: 10px; }
                </style>
                <div class="table-container2">
                <table class="profit-table2">
                <colgroup>
                    <col style="width: 7%;">
                    <col style="width: 6%;">
                    <col style="width: 12%;">
                    <col style="width: 14%;">
                    <col style="width: 14%;">
                    <col style="width: 4%;">
                    <col style="width: 6%;">
                    <col style="width: 5%;">
                    <col style="width: 6%;">
                    <col style="width: 6%;">
                    <col style="width: 4%;">
                    <col style="width: 7%;">
                    <col style="width: 7%;">
                </colgroup>
                <thead>
                    <tr>
                        <th>Vehicle</th>
                        <th>Date</th>
                        <th>Loaded Route</th>
                        <th>Party</th>
                        <th>Return/Suggest</th>
                        <th>Cars</th>
                        <th>Revenue</th>
                        <th>Dist</th>
                        <th>Expense</th>
                        <th>Contrib.</th>
                        <th>Days</th>
                        <th>Per Day</th>
                        <th>Status</th>
                    </tr>
                </thead>
                <tbody>
                """

                for _, row in round_df.iterrows():
                    per_day = row['Per_Day_Contribution']
                    status = row['Status']

                    # DC Movement trips are always green
                    if status == 'DC Movement':
                        color_class = 'green3'
                        per_day_fmt = 'DC'
                        status_class = 'status-dc'
                        status_text = '🚛 DC'
                    elif status == 'By Road':
                        if per_day >= 7000:
                            color_class = 'green3'
                        elif per_day >= 5000:
                            color_class = 'amber3'
                        elif per_day >= 3000:
                            color_class = 'red3'
                        else:
                            color_class = 'not-profit3'
                        per_day_fmt = f"₹{per_day:,.0f}"
                        status_class = 'status-done'
                        status_text = '🚚 ByRoad'
                    else:
                        if per_day >= 7000:
                            color_class = 'green3'
                        elif per_day >= 5000:
                            color_class = 'amber3'
                        elif per_day >= 3000:
                            color_class = 'red3'
                        else:
                            color_class = 'not-profit3'
                        per_day_fmt = f"₹{per_day:,.0f}"
                        if status == 'Completed':
                            status_class = 'status-done'
                            status_text = '✓ Done'
                        else:
                            status_class = 'status-ongoing'
                            status_text = '⏳ Ongoing'

                    loaded_date = pd.to_datetime(row['Loaded_Date']).strftime('%d-%b-%Y') if pd.notna(row['Loaded_Date']) else ''
                    revenue_fmt = f"₹{row['Revenue']/1000:.1f}K" if row['Revenue'] > 0 else "₹0"
                    total_exp_fmt = f"₹{row['Total_Exp']/1000:.1f}K"
                    contrib_fmt = f"₹{row['Contribution']/1000:.1f}K"

                    empty_route = row['Empty_Route'] or ''

                    html_table += f"""
                    <tr>
                        <td>{row['VehicleNo']}</td>
                        <td>{loaded_date}</td>
                        <td class="left-align2" title="{row['Loaded_Route']}">{row['Loaded_Route']}</td>
                        <td class="left-align2" title="{row['Loaded_Party'] or ''}">{row['Loaded_Party'] or ''}</td>
                        <td class="left-align2" title="{empty_route}">{empty_route}</td>
                        <td>{int(row['Loaded_Cars'])}</td>
                        <td>{revenue_fmt}</td>
                        <td>{int(row['Total_Distance'])}</td>
                        <td>{total_exp_fmt}</td>
                        <td>{contrib_fmt}</td>
                        <td>{int(row['Calc_Days'])}</td>
                        <td class="{color_class}">{per_day_fmt}</td>
                        <td class="{status_class}">{status_text}</td>
                    </tr>
                    """

                html_table += "</tbody></table></div>"

                components.html(html_table, height=min(len(round_df) * 40 + 100, 600), scrolling=True)

                # Legend explaining calculations
                st.markdown("""
                <div style="background: #1a1a2e; border: 1px solid #2d3748; border-radius: 8px; padding: 15px; margin-top: 15px; font-size: 12px;">
                    <strong style="color: #60a5fa;">📊 Calculation Legend:</strong>
                    <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 10px; margin-top: 10px;">
                        <div style="color: #d1d5db;">
                            <strong>TK4 Vehicles (20):</strong> Loaded = ₹30/km, Empty = ₹29/km
                        </div>
                        <div style="color: #d1d5db;">
                            <strong>East Zone Dest:</strong> Loaded = ₹43.5/km, Empty = ₹39/km
                        </div>
                        <div style="color: #d1d5db;">
                            <strong>Other Vehicles:</strong> Loaded = ₹45.6/km, Empty = ₹40/km
                        </div>
                        <div style="color: #d1d5db;">
                            <strong>Days:</strong> ⌈Total Distance ÷ 350⌉ (rounded up)
                        </div>
                        <div style="color: #d1d5db;">
                            <strong>Contribution:</strong> Revenue - (Loaded Exp + Empty Exp)
                        </div>
                        <div style="color: #d1d5db;">
                            <strong>Branch Location:</strong> 50 km empty assumed (local movement)
                        </div>
                    </div>
                    <div style="border-top: 1px solid #2d3748; margin-top: 12px; padding-top: 12px; display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 10px;">
                        <div style="color: #10b981;">
                            <strong>🚛 DC Movement:</strong> Shuttle trips, always profitable (green)
                        </div>
                        <div style="color: #f59e0b;">
                            <strong>🚚 By Road:</strong> No empty trip, Loaded Exp = Freight × 75.2%
                        </div>
                        <div style="color: #8b5cf6;">
                            <strong>⏳ Ongoing:</strong> Empty trip estimated from historical data or onward route
                        </div>
                    </div>
                </div>
                """, unsafe_allow_html=True)
            else:
                st.info("No matching trip pairs found (Loaded trip → Empty return trip)")

        trip_profitability_fragment()

    with tab7:
        st.markdown("### Pending CN - Triplogs")

        @st.fragment(run_every=REFRESH_10_MIN)
        def pending_cn_fragment():
            # Auto-refresh: reload data from database
            refresh_session_data()
            frag_df = st.session_state.df

            # D-3 date filter (show trips loaded on or before 3 days ago)
            d_minus_3 = datetime.now().date() - timedelta(days=3)

            # Load excluded trips from database
            excluded_trips = load_excluded_trips()

            # For Pending CN: Include both last month and current month data
            last_month_start = (month_start - timedelta(days=1)).replace(day=1)
            pending_data = frag_df[
                (frag_df['LoadingDateOnly'] >= last_month_start.date()) &
                (frag_df['LoadingDateOnly'] <= month_end.date())
            ].copy()

            # Filter trips with pending CN (LR numbers missing or empty) and loading date <= D-3
            pending_cn_df = pending_data[
                (pending_data['TripStatus'] == 'Loaded') &
                ((pending_data['LRNos'].isna()) | (pending_data['LRNos'] == '') | (pending_data['LRNos'].str.strip() == '')) &
                (pending_data['LoadingDate'].dt.date <= d_minus_3) &
                (~pending_data['TLHSNo'].isin(excluded_trips))
            ].copy()

            # Cross-check with cn_data to exclude trips that have CN records
            if len(pending_cn_df) > 0:
                try:
                    conn = get_db_connection()
                    if conn is not None:
                        # Load cn_data for matching (Method 1: cn_date + vehicle_no)
                        cn_query = """
                            SELECT DISTINCT cn_date, vehicle_no
                            FROM cn_data
                            WHERE cn_date IS NOT NULL AND vehicle_no IS NOT NULL
                              AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
                              AND (is_active = true OR is_active = 'Yes')
                        """
                        cn_records = pd.read_sql_query(cn_query, conn)

                        # Load cn_data for Own Vehicle matching (Method 2: route + vehicle_no where tl_no is blank)
                        own_vehicle_query = """
                            SELECT DISTINCT route, vehicle_no
                            FROM cn_data
                            WHERE (tl_no IS NULL OR tl_no = '')
                              AND vehicle_type = 'Own Vehicle'
                              AND route IS NOT NULL AND route != ''
                              AND vehicle_no IS NOT NULL AND vehicle_no != ''
                              AND NOT (billing_party = 'Ranjeet Singh Logistics' AND basic_freight = 65000)
                              AND (is_active = true OR is_active = 'Yes')
                        """
                        own_vehicle_records = pd.read_sql_query(own_vehicle_query, conn)
                        conn.close()

                        # Method 1: Match by cn_date + vehicle_no
                        if not cn_records.empty:
                            cn_records['cn_date'] = pd.to_datetime(cn_records['cn_date'], errors='coerce').dt.date
                            cn_records['vehicle_no'] = cn_records['vehicle_no'].str.upper().str.strip()

                            # Create lookup key for cn_data
                            cn_records['lookup_key'] = cn_records['cn_date'].astype(str) + '_' + cn_records['vehicle_no']
                            cn_keys = set(cn_records['lookup_key'].tolist())

                            # Create lookup key for pending trips
                            pending_cn_df['LoadingDateOnly'] = pending_cn_df['LoadingDate'].dt.date
                            pending_cn_df['VehicleNoClean'] = pending_cn_df['VehicleNo'].str.upper().str.strip()
                            pending_cn_df['lookup_key'] = pending_cn_df['LoadingDateOnly'].astype(str) + '_' + pending_cn_df['VehicleNoClean']

                            # Exclude trips that have matching CN records
                            pending_cn_df = pending_cn_df[~pending_cn_df['lookup_key'].isin(cn_keys)]

                        # Method 2: Match Own Vehicle by route + vehicle_no (for records where tl_no is blank)
                        if not own_vehicle_records.empty and len(pending_cn_df) > 0:
                            own_vehicle_records['route'] = own_vehicle_records['route'].str.upper().str.strip()
                            own_vehicle_records['vehicle_no'] = own_vehicle_records['vehicle_no'].str.upper().str.strip()

                            # Create lookup key for own vehicle cn_data (route + vehicle_no)
                            own_vehicle_records['route_vehicle_key'] = own_vehicle_records['route'] + '_' + own_vehicle_records['vehicle_no']
                            own_vehicle_keys = set(own_vehicle_records['route_vehicle_key'].tolist())

                            # Create lookup key for pending trips (Route + VehicleNo)
                            pending_cn_df['RouteClean'] = pending_cn_df['Route'].str.upper().str.strip()
                            pending_cn_df['VehicleNoClean'] = pending_cn_df['VehicleNo'].str.upper().str.strip()
                            pending_cn_df['route_vehicle_key'] = pending_cn_df['RouteClean'] + '_' + pending_cn_df['VehicleNoClean']

                            # Exclude trips that match Own Vehicle CN records by route + vehicle_no
                            pending_cn_df = pending_cn_df[~pending_cn_df['route_vehicle_key'].isin(own_vehicle_keys)]
                except Exception as e:
                    st.warning(f"Could not cross-check with cn_data: {e}")

            st.caption(f"*Showing trips loaded on or before {d_minus_3.strftime('%d-%b-%Y')} (D-3)*")

            if len(pending_cn_df) > 0:
                # Sort by loading date
                pending_cn_df = pending_cn_df.sort_values('LoadingDate', ascending=True)

                # Prepare display table data
                display_cols = ['TLHSNo', 'LoadingDate', 'VehicleNo', 'DriverName', 'DriverCode', 'Route', 'Party', 'CarQty']
                available_cols = [col for col in display_cols if col in pending_cn_df.columns]

                display_df = pending_cn_df[available_cols].copy()
                display_df['LoadingDate'] = display_df['LoadingDate'].dt.strftime('%d-%b-%Y')

                # Combine Driver Name with Code
                display_df['Driver'] = display_df.apply(
                    lambda row: f"{row['DriverName']} ({row['DriverCode']})" if pd.notna(row['DriverCode']) and row['DriverCode'] != '' else row['DriverName'],
                    axis=1
                )

                # Select final columns
                final_cols = ['TLHSNo', 'LoadingDate', 'VehicleNo', 'Driver', 'Route', 'Party', 'CarQty']
                display_df = display_df[final_cols]
                display_df.columns = ['Trip No', 'Loading Date', 'Vehicle No', 'Driver', 'Route', 'Party', 'Cars']

                # Layout: Summary on left, Table on right
                col_left, col_right = st.columns([1, 3])

                with col_left:
                    # Total Summary
                    st.markdown(f"""
                    <div style="background: linear-gradient(135deg, #dc2626 0%, #991b1b 100%); padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                        <div style="color: #fecaca; font-size: 12px;">Total Pending Trips</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">{len(pending_cn_df)}</div>
                    </div>
                    <div style="background: linear-gradient(135deg, #1e40af 0%, #1e3a8a 100%); padding: 15px; border-radius: 8px; margin-bottom: 15px;">
                        <div style="color: #bfdbfe; font-size: 12px;">Total Cars</div>
                        <div style="color: white; font-size: 28px; font-weight: bold;">{int(pending_cn_df['CarQty'].sum())}</div>
                    </div>
                    """, unsafe_allow_html=True)

                    # Party-wise summary
                    party_summary = pending_cn_df.groupby('Party').agg({
                        'TLHSNo': 'count',
                        'CarQty': 'sum'
                    }).reset_index()
                    party_summary.columns = ['Party', 'Trips', 'Cars']
                    party_summary = party_summary.sort_values('Trips', ascending=False)
                    party_summary['Cars'] = party_summary['Cars'].astype(int)

                    st.markdown("**Party-wise Summary**")
                    party_html = """
                    <style>
                        .party-summary { width: 100%; border-collapse: collapse; font-size: 11px; }
                        .party-summary th { background-color: #374151; color: white; padding: 6px 8px; text-align: left; }
                        .party-summary td { padding: 5px 8px; border-bottom: 1px solid #4b5563; color: white; }
                        .party-summary tr:nth-child(even) { background-color: #1f2937; }
                    </style>
                    <table class="party-summary">
                        <thead><tr><th>Party</th><th style="text-align:center;">Trips</th><th style="text-align:center;">Cars</th></tr></thead>
                        <tbody>
                    """
                    for _, row in party_summary.iterrows():
                        party_html += f"<tr><td>{row['Party'][:35]}{'...' if len(str(row['Party'])) > 35 else ''}</td><td style='text-align:center; color: #fbbf24;'>{row['Trips']}</td><td style='text-align:center; color: #34d399;'>{row['Cars']}</td></tr>"
                    party_html += "</tbody></table>"
                    components.html(party_html, height=min(len(party_summary) * 30 + 35, 350), scrolling=True)

                with col_right:
                    # Build HTML table
                    pending_html = """
                    <style>
                        .pending-table { width: 100%; border-collapse: separate; border-spacing: 0; font-size: 12px; }
                        .pending-table thead { position: sticky; top: 0; z-index: 10; }
                        .pending-table th { background-color: #dc2626; color: white; padding: 8px; text-align: left; }
                        .pending-table td { padding: 6px 8px; border-bottom: 1px solid #374151; color: white; }
                        .pending-table tr:hover { background-color: #1f2937; }
                    </style>
                    <div style="max-height: 550px; overflow-y: auto;">
                    <table class="pending-table">
                        <thead>
                            <tr>
                    """
                    for col in display_df.columns:
                        pending_html += f"<th>{col}</th>"
                    pending_html += "</tr></thead><tbody>"

                    for _, row in display_df.iterrows():
                        pending_html += "<tr>"
                        for col in display_df.columns:
                            pending_html += f"<td>{row[col]}</td>"
                        pending_html += "</tr>"

                    pending_html += "</tbody></table></div>"
                    components.html(pending_html, height=550, scrolling=True)

                    # Download button for Pending CN data
                    pending_csv = display_df.to_csv(index=False)
                    st.download_button(
                        label="📥 Download Pending CN Data",
                        data=pending_csv,
                        file_name=f"pending_cn_trips_{datetime.now().strftime('%Y%m%d')}.csv",
                        mime="text/csv",
                        key="pending_cn_download"
                    )
            else:
                st.success("No pending CN trips found for this month!")

        pending_cn_fragment()

        # CN Aging Branch-wise Section
        st.markdown("---")
        st.markdown("### CN Aging - Branch Wise")
        st.caption("*Days taken to create CN after Trip Log (Last 7 days - Most frequent)*")

        @st.fragment(run_every=REFRESH_10_MIN)
        def cn_aging_fragment():
            try:
                # Auto-refresh: reload data from database
                refresh_session_data()
                frag_cn_data = st.session_state.cn_data
                frag_df = st.session_state.df

                if not frag_cn_data.empty and not frag_df.empty:
                    last_7_days = (datetime.now() - timedelta(days=7)).date()

                    # Filter cn_data: tl_no not blank, cn_date in last 7 days
                    cn_filtered = frag_cn_data[
                        (frag_cn_data['tl_no'].notna()) &
                        (frag_cn_data['tl_no'] != '') &
                        (frag_cn_data['cn_date'].notna()) &
                        (frag_cn_data['cn_date'].dt.date >= last_7_days)
                    ].copy()

                    # Merge with trip log to get loading_date
                    aging_df = cn_filtered.merge(
                        frag_df[['TLHSNo', 'LoadingDate']],
                        left_on='tl_no',
                        right_on='TLHSNo',
                        how='inner'
                    )

                    # Filter: cn_date >= loading_date and loading_date not null
                    aging_df = aging_df[
                        (aging_df['LoadingDate'].notna()) &
                        (aging_df['cn_date'].dt.date >= aging_df['LoadingDate'].dt.date)
                    ]

                    # Calculate aging days
                    aging_df['aging_days'] = (aging_df['cn_date'].dt.date - aging_df['LoadingDate'].dt.date).apply(lambda x: x.days if x else 0)

                    if len(aging_df) > 0:
                        # Group by branch and calculate most common days (mode)
                        branch_aging = aging_df.groupby('branch').agg({
                            'aging_days': lambda x: x.value_counts().idxmax() if len(x) > 0 else 0
                        }).reset_index()
                        branch_aging.columns = ['Branch', 'Most Common Days']
                        branch_aging = branch_aging.sort_values('Most Common Days', ascending=False)

                        # Build HTML table
                        aging_html = """
                        <style>
                            .aging-table { width: 50%; border-collapse: collapse; font-size: 14px; }
                            .aging-table th { background-color: #1e3a5f; color: white; padding: 12px; text-align: center; border: 1px solid #3b82f6; }
                            .aging-table td { padding: 10px 12px; border: 1px solid #2d3748; color: white; }
                            .aging-table tr:nth-child(even) { background-color: #1a1f2e; }
                            .aging-table tr:nth-child(odd) { background-color: #0e1117; }
                            .aging-table .branch-col { text-align: left; font-weight: bold; }
                            .aging-table .days-col { text-align: center; font-weight: bold; }
                            .aging-table .high-aging { color: #f87171; }
                            .aging-table .medium-aging { color: #fbbf24; }
                            .aging-table .low-aging { color: #34d399; }
                        </style>
                        <table class="aging-table">
                            <thead>
                                <tr>
                                    <th style="text-align: left;">Branch</th>
                                    <th>Days to Create CN (Most Frequent)</th>
                                </tr>
                            </thead>
                            <tbody>
                        """

                        for _, row in branch_aging.iterrows():
                            days_val = int(row['Most Common Days'])
                            if days_val > 3:
                                days_class = 'high-aging'
                            elif days_val > 1:
                                days_class = 'medium-aging'
                            else:
                                days_class = 'low-aging'
                            days_display = 'Same Day' if days_val == 0 else f'{days_val} days'
                            aging_html += f"""
                                <tr>
                                    <td class="branch-col">{row['Branch']}</td>
                                    <td class="days-col {days_class}">{days_display}</td>
                                </tr>
                            """

                        aging_html += "</tbody></table>"

                        components.html(aging_html, height=min(len(branch_aging) * 45 + 60, 500), scrolling=True)
                    else:
                        st.info("No CN records found with Trip Log mapping.")
                else:
                    st.info("No CN records found with Trip Log mapping.")
            except Exception as e:
                st.error(f"Error loading CN aging data: {e}")

        cn_aging_fragment()


if __name__ == "__main__":
    main()
