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

        query = "SELECT * FROM swift_trip_log"
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
            WHERE ((billing_party = 'R.sai Logistics India Pvt. Ltd.' AND tl_no IS NULL)
               OR (billing_party != 'R.sai Logistics India Pvt. Ltd.' AND vehicle_type = 'Hire Vehicle'))
               AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
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


@st.cache_data(ttl=3600)  # Cache for 60 minutes - auto refresh when vehicles change
def load_vehicles_by_type(vehicle_type):
    """Load vehicle numbers from swift_vehicles by vehicle_type"""
    try:
        conn = get_db_connection()
        if conn is None:
            return []

        query = f"""
            SELECT vehicle_no FROM swift_vehicles
            WHERE vehicle_type = '{vehicle_type}' AND is_active = 'Y'
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


def main():
    # Header
    st.markdown("<h1 style='text-align: center;'>🚚 Swift Trip Log Dashboard</h1>", unsafe_allow_html=True)

    # Load data
    with st.spinner("Loading data..."):
        df = load_triplog_data()
        vendor_df = load_vendor_data()
        targets = load_targets()

    if df.empty:
        st.error("No data available. Please check the database connection.")
        return

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

    # Sidebar Filters
    st.sidebar.header("Filters")

    # Select Month
    st.sidebar.subheader("Select Month")
    available_months = df['LoadingDate'].dt.to_period('M').dropna().unique()
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

    # Refresh button
    if st.sidebar.button("🔄 Refresh Data"):
        st.cache_data.clear()
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

    # Convert to date only for accurate comparison
    df['LoadingDateOnly'] = df['LoadingDate'].dt.date

    # Full month data for summary boxes (all available data for the month)
    month_df = df[(df['LoadingDateOnly'] >= month_start.date()) & (df['LoadingDateOnly'] <= month_end.date())]

    if selected_party != 'All':
        month_df = month_df[month_df['DisplayParty'] == selected_party]

    # Month Summary Section (Full Month - Live Data) - Auto refresh every 10 minutes
    @st.fragment(run_every=REFRESH_10_MIN)
    def month_summary_fragment():
        st.markdown(f"### Month Summary ({selected_month.strftime('%B %Y')})")

        # Calculate Own metrics
        loaded_trips = len(month_df[(month_df['TripStatus'] != 'Empty') & (month_df['DisplayParty'] != '') & (month_df['DisplayParty'].notna())])
        empty_trips = len(month_df[(month_df['TripStatus'] == 'Empty') | (month_df['DisplayParty'] == '') | (month_df['DisplayParty'].isna())])
        own_cars = int(month_df['CarQty'].sum())
        own_freight = month_df['Freight'].sum()

        # Calculate Vendor metrics for full month
        vendor_cars = 0
        vendor_freight = 0.0
        vendor_trips = 0
        if not vendor_df.empty:
            vendor_month_df = vendor_df[
                (vendor_df['CNDate'] >= pd.Timestamp(month_start.date())) &
                (vendor_df['CNDate'] < pd.Timestamp(month_end.date()) + pd.Timedelta(days=1))
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
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["📊 Target vs Actual", "📅 Daily Loading Details", "🚚 Local/Pilot Loads", "🗺️ Zone View", "📋 Pending CN - Triplogs", "📄 Unbilled CN"])

    with tab1:
        # Target vs Actual - Client-Wise Summary
        st.markdown("### Target vs Actual - Client-Wise Summary")

        @st.fragment(run_every=REFRESH_10_MIN)
        def target_vs_actual_fragment():
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
            current_df = df[
                (df['LoadingDateOnly'] >= select_month_start) &
                (df['LoadingDateOnly'] <= till_date) &
                (df['DisplayParty'] != '') &
                (df['DisplayParty'].notna())
            ]

            # Filter data for comparison period
            compare_df = df[
                (df['LoadingDateOnly'] >= compare_month) &
                (df['LoadingDateOnly'] <= compare_till_date) &
                (df['DisplayParty'] != '') &
                (df['DisplayParty'].notna())
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

            if not vendor_df.empty:
                # Filter vendor data for current period (convert to Timestamp for consistent comparison)
                vendor_current = vendor_df[
                    (vendor_df['CNDate'] >= pd.Timestamp(select_month_start)) &
                    (vendor_df['CNDate'] < pd.Timestamp(till_date) + pd.Timedelta(days=1))
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
            # Date filter and summary in one row
            col_date, col_cars, col_trips_count, col_empty = st.columns([1, 1, 1, 1])
            with col_date:
                selected_date = st.date_input("Select Date", datetime.now().date(), key='daily_date_fragment')

            # Filter data for selected date - only loaded trips (with Party Name)
            daily_data = df[
                (df['LoadingDateOnly'] == selected_date) &
                (df['DisplayParty'] != '') &
                (df['DisplayParty'].notna()) &
                (df['CarQty'] > 0)
            ]

            # Own totals
            own_cars_lifted = int(daily_data['CarQty'].sum())
            own_trips_count = len(daily_data)

            # Vendor totals for selected date
            vendor_cars_lifted = 0
            vendor_trips_count = 0
            if not vendor_df.empty:
                vendor_daily_data = vendor_df[
                    (vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                    (vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
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
                if not vendor_df.empty:
                    vendor_daily = vendor_df[
                        (vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                        (vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
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

                if not vendor_df.empty:
                    vendor_daily_summary = vendor_df[
                        (vendor_df['CNDate'] >= pd.Timestamp(selected_date)) &
                        (vendor_df['CNDate'] < pd.Timestamp(selected_date) + pd.Timedelta(days=1))
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
            col_filter, col_empty = st.columns([1, 3])
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
            # Filter loaded trips only (same as summary box logic)
            loaded_df = month_df[
                (month_df['TripStatus'] != 'Empty') &
                (month_df['DisplayParty'] != '') &
                (month_df['DisplayParty'].notna())
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

            if not vendor_df.empty:
                # Filter vendor data for the month
                vendor_zone_df = vendor_df[
                    (vendor_df['CNDate'] >= pd.Timestamp(month_start.date())) &
                    (vendor_df['CNDate'] < pd.Timestamp(month_end.date()) + pd.Timedelta(days=1))
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

            # Chart: Zone by Car Lifted (excluding DC Movement)
            st.markdown("---")
            st.markdown("#### Zone by Car Lifted")
            st.caption("*Note: DC Movement not included*")

            import plotly.graph_objects as go

            # Filter out DC Movement for this chart
            chart_df = loaded_df[~loaded_df['NewPartyName'].str.contains('DC Movement', case=False, na=False)]

            # Recalculate zone matrix excluding DC Movement
            chart_cars_matrix = {}
            for origin_zone in zones:
                chart_cars_matrix[origin_zone] = {}
                for dest_zone in zones:
                    count = chart_df[(chart_df['Origin_Zone'] == origin_zone) & (chart_df['Dest_Zone'] == dest_zone)]['CarQty'].sum()
                    chart_cars_matrix[origin_zone][dest_zone] = int(count) if count > 0 else 0

            # Get top routes for chart
            route_data = []
            for oz in zones:
                for dz in zones:
                    if chart_cars_matrix[oz][dz] > 0:
                        route_data.append({
                            'Route': f'{oz} → {dz}',
                            'Cars': chart_cars_matrix[oz][dz]
                        })

            if route_data:
                route_df = pd.DataFrame(route_data).sort_values('Cars', ascending=True).tail(10)

                fig = go.Figure()
                fig.add_trace(go.Bar(
                    y=route_df['Route'],
                    x=route_df['Cars'],
                    orientation='h',
                    marker_color=['#22c55e' if c >= route_df['Cars'].quantile(0.7) else '#3b82f6' if c >= route_df['Cars'].quantile(0.4) else '#eab308' for c in route_df['Cars']],
                    text=route_df['Cars'],
                    textposition='outside'
                ))
                fig.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='white'),
                    height=400,
                    xaxis_title='Cars Lifted',
                    yaxis_title='',
                    margin=dict(l=100, r=50, t=20, b=50)
                )
                fig.update_xaxes(showgrid=True, gridcolor='rgba(255,255,255,0.1)')
                fig.update_yaxes(showgrid=False)
                st.plotly_chart(fig, use_container_width=True)

            # Show unmapped cities if any
            other_origins = loaded_df[loaded_df['Origin_Zone'] == 'Other']['Origin'].unique()
            other_dests = loaded_df[loaded_df['Dest_Zone'] == 'Other']['Destination'].unique()

            if len(other_origins) > 0 or len(other_dests) > 0:
                with st.expander("Unmapped Cities (Other Zone)"):
                    if len(other_origins) > 0:
                        st.write("**Origins:**", ', '.join(sorted(set(other_origins))))
                    if len(other_dests) > 0:
                        st.write("**Destinations:**", ', '.join(sorted(set(other_dests))))

        zone_view_fragment()

    with tab5:
        st.markdown("### Pending CN - Triplogs")

        @st.fragment(run_every=REFRESH_10_MIN)
        def pending_cn_fragment():
            # D-3 date filter (show trips loaded on or before 3 days ago)
            d_minus_3 = datetime.now().date() - timedelta(days=3)

            # Load excluded trips from database
            excluded_trips = load_excluded_trips()

            # Filter trips with pending CN (LR numbers missing or empty) and loading date <= D-3
            pending_cn_df = month_df[
                (month_df['TripStatus'] == 'Loaded') &
                ((month_df['LRNos'].isna()) | (month_df['LRNos'] == '') | (month_df['LRNos'].str.strip() == '')) &
                (month_df['LoadingDate'].dt.date <= d_minus_3) &
                (~month_df['TLHSNo'].isin(excluded_trips))
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
                conn = get_db_connection()
                if conn is not None:
                    # Query cn_data where tl_no is not blank, joined with swift_trip_log for loading_date
                    # Filter by last 7 days based on cn_date
                    last_7_days = (datetime.now() - timedelta(days=7)).date()
                    aging_query = f"""
                        SELECT
                            c.branch,
                            c.cn_no,
                            c.cn_date,
                            c.tl_no,
                            t.loading_date,
                            c.cn_date::date - t.loading_date::date as aging_days
                        FROM cn_data c
                        INNER JOIN swift_trip_log t ON c.tl_no = t.tlhs_no
                        WHERE c.tl_no IS NOT NULL
                          AND c.tl_no != ''
                          AND c.cn_date IS NOT NULL
                          AND t.loading_date IS NOT NULL
                          AND c.cn_date::date >= '{last_7_days}'
                          AND c.cn_date::date >= t.loading_date::date
                    """
                    aging_df = pd.read_sql_query(aging_query, conn)
                    conn.close()

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
                    st.warning("Could not connect to database.")
            except Exception as e:
                st.error(f"Error loading CN aging data: {e}")

        cn_aging_fragment()

    with tab6:
        st.markdown("### Unbilled CN - POD Received")
        st.caption("*CNs where Bill No is blank but POD Receipt No exists - grouped by Category and Month*")

        @st.fragment(run_every=REFRESH_20_MIN)
        def unbilled_cn_fragment():
            # Load unbilled CNs (bill_no blank, pod_receipt_no not blank)
            try:
                conn = get_db_connection()
                if conn is not None:
                    unbilled_query = """
                        SELECT billing_party,
                               TO_CHAR(cn_date, 'YYYY-MM') as month,
                               TO_CHAR(cn_date, 'Mon''YY') as month_display,
                               COUNT(cn_no) as cn_count,
                               SUM(qty) as qty_total,
                               SUM(basic_freight) as unbilled_amount
                        FROM cn_data
                        WHERE (bill_no IS NULL OR bill_no = '')
                          AND pod_receipt_no IS NOT NULL AND pod_receipt_no != ''
                          AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
                        GROUP BY billing_party, TO_CHAR(cn_date, 'YYYY-MM'), TO_CHAR(cn_date, 'Mon''YY')
                        ORDER BY billing_party, month DESC
                    """
                    unbilled_df = pd.read_sql_query(unbilled_query, conn)
                    conn.close()

                    if not unbilled_df.empty:
                        # Add category for grouping
                        unbilled_df['category'] = unbilled_df['billing_party'].apply(get_client_category)

                        # Get unique months for columns
                        months = unbilled_df[['month', 'month_display']].drop_duplicates().sort_values('month', ascending=False)
                        month_order = months['month_display'].tolist()

                        # Pivot tables
                        pivot_cn = unbilled_df.pivot_table(index='billing_party', columns='month_display', values='cn_count', aggfunc='sum', fill_value=0)
                        pivot_qty = unbilled_df.pivot_table(index='billing_party', columns='month_display', values='qty_total', aggfunc='sum', fill_value=0)
                        pivot_amount = unbilled_df.pivot_table(index='billing_party', columns='month_display', values='unbilled_amount', aggfunc='sum', fill_value=0)

                        # Reorder columns
                        pivot_cn = pivot_cn.reindex(columns=[m for m in month_order if m in pivot_cn.columns])
                        pivot_qty = pivot_qty.reindex(columns=[m for m in month_order if m in pivot_qty.columns])
                        pivot_amount = pivot_amount.reindex(columns=[m for m in month_order if m in pivot_amount.columns])

                        # Add category to pivot index
                        party_category = unbilled_df[['billing_party', 'category']].drop_duplicates().set_index('billing_party')['category']

                        # Category order
                        category_order = ['Honda', 'M & M', 'Toyota', 'Skoda', 'Glovis', 'Tata', 'John Deere', 'Spinny', 'JSW MG', 'R.sai', 'Mohan Logistics', 'SAI Auto', 'Kwick', 'Market Load', 'Other']

                        # Calculate totals for summary boxes
                        total_cn_count = int(unbilled_df['cn_count'].sum())
                        total_qty = int(unbilled_df['qty_total'].sum())
                        total_unbilled_amount = unbilled_df['unbilled_amount'].sum()

                        # Layout: Summary on left, Table on right
                        col_left, col_right = st.columns([1, 4])

                        with col_left:
                            # Summary boxes
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #3b82f6 0%, #1d4ed8 100%); padding: 20px; border-radius: 10px; text-align: center; margin-bottom: 15px;">
                                <div style="color: #dbeafe; font-size: 14px;">Total No. of CN</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">{total_cn_count:,}</div>
                            </div>
                            """, unsafe_allow_html=True)

                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #8b5cf6 0%, #7c3aed 100%); padding: 20px; border-radius: 10px; text-align: center; margin-bottom: 15px;">
                                <div style="color: #ede9fe; font-size: 14px;">Total Qty</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">{total_qty:,}</div>
                            </div>
                            """, unsafe_allow_html=True)

                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #10b981 0%, #059669 100%); padding: 20px; border-radius: 10px; text-align: center;">
                                <div style="color: #d1fae5; font-size: 14px;">Total Unbilled Amount</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">₹{total_unbilled_amount/100000:.2f}L</div>
                            </div>
                            """, unsafe_allow_html=True)

                        with col_right:
                            # Build HTML table with sticky header and first column
                            unbilled_html = """
                            <div style="overflow: auto; max-width: 100%; max-height: 550px;">
                            <table style="width: 100%; border-collapse: separate; border-spacing: 0; font-size: 13px; border: 1px solid #64748b;">
                            <thead>
                                <tr style="background: #1e3a5f; color: white;">
                                    <th rowspan="2" style="padding: 12px; text-align: left; border: 1px solid #64748b; min-width: 280px; vertical-align: middle; position: sticky; left: 0; top: 0; z-index: 3; background: #1e3a5f;">Billing Party</th>
                            """

                            for month in pivot_cn.columns:
                                unbilled_html += f'<th colspan="3" style="padding: 12px; text-align: center; border: 1px solid #64748b; position: sticky; top: 0; z-index: 2; background: #1e3a5f;">{month}</th>'

                            unbilled_html += """
                                </tr>
                                <tr style="background: #1e3a5f; color: #e0f2fe;">
                            """

                            for month in pivot_cn.columns:
                                unbilled_html += '<th style="padding: 8px; text-align: center; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #1e3a5f;">No. of CN</th>'
                                unbilled_html += '<th style="padding: 8px; text-align: center; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #1e3a5f;">Qty</th>'
                                unbilled_html += '<th style="padding: 8px; text-align: right; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #1e3a5f;">Unbilled Amt</th>'

                            unbilled_html += "</tr></thead><tbody>"

                            # Grand totals
                            grand_cn = {m: 0 for m in pivot_cn.columns}
                            grand_qty = {m: 0 for m in pivot_cn.columns}
                            grand_amount = {m: 0 for m in pivot_cn.columns}

                            row_idx = 0
                            for category in category_order:
                                # Get parties in this category
                                cat_parties = [p for p in pivot_cn.index if party_category.get(p) == category]

                                if not cat_parties:
                                    continue

                                # Category totals
                                cat_cn = {m: 0 for m in pivot_cn.columns}
                                cat_qty = {m: 0 for m in pivot_cn.columns}
                                cat_amount = {m: 0 for m in pivot_cn.columns}

                                # Add party rows
                                for party in sorted(cat_parties):
                                    bg_color = '#1e293b' if row_idx % 2 == 0 else '#0f172a'
                                    unbilled_html += f'<tr style="background: {bg_color}; color: white;">'
                                    unbilled_html += f'<td style="padding: 8px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: {bg_color};">{party}</td>'

                                    for month in pivot_cn.columns:
                                        cn_count = int(pivot_cn.loc[party, month])
                                        qty_count = int(pivot_qty.loc[party, month])
                                        amount = pivot_amount.loc[party, month]
                                        cat_cn[month] += cn_count
                                        cat_qty[month] += qty_count
                                        cat_amount[month] += amount
                                        unbilled_html += f'<td style="padding: 8px; text-align: center; border: 1px solid #64748b;">{cn_count if cn_count > 0 else "-"}</td>'
                                        unbilled_html += f'<td style="padding: 8px; text-align: center; border: 1px solid #64748b;">{qty_count if qty_count > 0 else "-"}</td>'
                                        unbilled_html += f'<td style="padding: 8px; text-align: right; border: 1px solid #64748b;">{"₹{:,.0f}".format(amount) if amount > 0 else "-"}</td>'

                                    unbilled_html += "</tr>"
                                    row_idx += 1

                                # Add to grand totals
                                for month in pivot_cn.columns:
                                    grand_cn[month] += cat_cn[month]
                                    grand_qty[month] += cat_qty[month]
                                    grand_amount[month] += cat_amount[month]

                                # Category total row (gold) - only show if more than 1 party
                                if len(cat_parties) > 1:
                                    unbilled_html += f'<tr style="background: #b8860b; color: white; font-weight: bold;">'
                                    unbilled_html += f'<td style="padding: 10px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: #b8860b;">{category} - Total</td>'

                                    for month in pivot_cn.columns:
                                        unbilled_html += f'<td style="padding: 10px; text-align: center; border: 1px solid #64748b;">{cat_cn[month] if cat_cn[month] > 0 else "-"}</td>'
                                        unbilled_html += f'<td style="padding: 10px; text-align: center; border: 1px solid #64748b;">{cat_qty[month] if cat_qty[month] > 0 else "-"}</td>'
                                        unbilled_html += f'<td style="padding: 10px; text-align: right; border: 1px solid #64748b;">{"₹{:,.0f}".format(cat_amount[month]) if cat_amount[month] > 0 else "-"}</td>'

                                    unbilled_html += "</tr>"

                            # Grand total row (dark blue)
                            unbilled_html += '<tr style="background: #1e3a5f; color: white; font-weight: bold;">'
                            unbilled_html += '<td style="padding: 12px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: #1e3a5f;">Grand Total</td>'

                            for month in pivot_cn.columns:
                                unbilled_html += f'<td style="padding: 12px; text-align: center; border: 1px solid #64748b;">{grand_cn[month]}</td>'
                                unbilled_html += f'<td style="padding: 12px; text-align: center; border: 1px solid #64748b;">{grand_qty[month]}</td>'
                                unbilled_html += f'<td style="padding: 12px; text-align: right; border: 1px solid #64748b;">₹{grand_amount[month]:,.0f}</td>'

                            unbilled_html += "</tr></tbody></table></div>"

                            components.html(unbilled_html, height=600, scrolling=True)

                            # Download button - Raw CN data
                            conn_download = get_db_connection()
                            if conn_download is not None:
                                raw_unbilled_query = """
                                    SELECT cn_no, cn_date, billing_party, origin, route,
                                           vehicle_no, qty, basic_freight, pod_receipt_no
                                    FROM cn_data
                                    WHERE (bill_no IS NULL OR bill_no = '')
                                      AND pod_receipt_no IS NOT NULL AND pod_receipt_no != ''
                                      AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
                                    ORDER BY cn_date DESC, billing_party
                                """
                                raw_unbilled_df = pd.read_sql_query(raw_unbilled_query, conn_download)
                                conn_download.close()

                                raw_unbilled_df.columns = ['CN No', 'CN Date', 'Billing Party', 'Origin', 'Route',
                                                           'Vehicle No', 'Qty', 'Basic Freight', 'POD Receipt No']
                                unbilled_csv = raw_unbilled_df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Unbilled CN Data",
                                    data=unbilled_csv,
                                    file_name=f"unbilled_cn_{datetime.now().strftime('%Y%m%d')}.csv",
                                    mime="text/csv",
                                    key="unbilled_cn_download"
                                )
                    else:
                        st.success("No unbilled CNs found!")
            except Exception as e:
                st.error(f"Error loading unbilled CN data: {e}")

            # Second table: Pending POD (bill_no blank, pod_receipt_no blank, ETA < D-4)
            st.markdown("---")
            st.markdown("### Unbilled CN - POD not Punch/Received")
            st.caption("*CNs where Bill No is blank, POD Receipt No is blank, and ETA < D-4*")

            try:
                conn = get_db_connection()
                if conn is not None:
                    d_minus_4 = (datetime.now() - timedelta(days=4)).date()

                    pending_pod_query = f"""
                        SELECT billing_party,
                               TO_CHAR(cn_date, 'YYYY-MM') as month,
                               TO_CHAR(cn_date, 'Mon''YY') as month_display,
                               COUNT(cn_no) as cn_count,
                               SUM(qty) as qty_total,
                               SUM(basic_freight) as unbilled_amount
                        FROM cn_data
                        WHERE (bill_no IS NULL OR bill_no = '')
                          AND (pod_receipt_no IS NULL OR pod_receipt_no = '')
                          AND eta < '{d_minus_4}'
                          AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
                        GROUP BY billing_party, TO_CHAR(cn_date, 'YYYY-MM'), TO_CHAR(cn_date, 'Mon''YY')
                        ORDER BY billing_party, month DESC
                    """
                    pending_pod_df = pd.read_sql_query(pending_pod_query, conn)
                    conn.close()

                    if not pending_pod_df.empty:
                        # Add category for grouping
                        pending_pod_df['category'] = pending_pod_df['billing_party'].apply(get_client_category)

                        # Get unique months for columns
                        months = pending_pod_df[['month', 'month_display']].drop_duplicates().sort_values('month', ascending=False)
                        month_order = months['month_display'].tolist()

                        # Pivot tables
                        pivot_cn2 = pending_pod_df.pivot_table(index='billing_party', columns='month_display', values='cn_count', aggfunc='sum', fill_value=0)
                        pivot_qty2 = pending_pod_df.pivot_table(index='billing_party', columns='month_display', values='qty_total', aggfunc='sum', fill_value=0)
                        pivot_amount2 = pending_pod_df.pivot_table(index='billing_party', columns='month_display', values='unbilled_amount', aggfunc='sum', fill_value=0)

                        # Reorder columns
                        pivot_cn2 = pivot_cn2.reindex(columns=[m for m in month_order if m in pivot_cn2.columns])
                        pivot_qty2 = pivot_qty2.reindex(columns=[m for m in month_order if m in pivot_qty2.columns])
                        pivot_amount2 = pivot_amount2.reindex(columns=[m for m in month_order if m in pivot_amount2.columns])

                        # Add category to pivot index
                        party_category2 = pending_pod_df[['billing_party', 'category']].drop_duplicates().set_index('billing_party')['category']

                        # Category order
                        category_order = ['Honda', 'M & M', 'Toyota', 'Skoda', 'Glovis', 'Tata', 'John Deere', 'Spinny', 'JSW MG', 'R.sai', 'Mohan Logistics', 'SAI Auto', 'Kwick', 'Market Load', 'Other']

                        # Calculate totals for summary boxes
                        total_cn_count2 = int(pending_pod_df['cn_count'].sum())
                        total_qty2 = int(pending_pod_df['qty_total'].sum())
                        total_unbilled_amount2 = pending_pod_df['unbilled_amount'].sum()

                        # Layout: Summary on left, Table on right
                        col_left2, col_right2 = st.columns([1, 4])

                        with col_left2:
                            # Summary boxes
                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #ef4444 0%, #dc2626 100%); padding: 20px; border-radius: 10px; text-align: center; margin-bottom: 15px;">
                                <div style="color: #fecaca; font-size: 14px;">Total No. of CN</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">{total_cn_count2:,}</div>
                            </div>
                            """, unsafe_allow_html=True)

                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #a855f7 0%, #9333ea 100%); padding: 20px; border-radius: 10px; text-align: center; margin-bottom: 15px;">
                                <div style="color: #f3e8ff; font-size: 14px;">Total Qty</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">{total_qty2:,}</div>
                            </div>
                            """, unsafe_allow_html=True)

                            st.markdown(f"""
                            <div style="background: linear-gradient(135deg, #f97316 0%, #ea580c 100%); padding: 20px; border-radius: 10px; text-align: center;">
                                <div style="color: #fed7aa; font-size: 14px;">Total Pending Amount</div>
                                <div style="color: white; font-size: 32px; font-weight: bold;">₹{total_unbilled_amount2/100000:.2f}L</div>
                            </div>
                            """, unsafe_allow_html=True)

                        with col_right2:
                            # Build HTML table with sticky header and first column
                            pending_pod_html = """
                            <div style="overflow: auto; max-width: 100%; max-height: 550px;">
                            <table style="width: 100%; border-collapse: separate; border-spacing: 0; font-size: 13px; border: 1px solid #64748b;">
                            <thead>
                                <tr style="background: #7f1d1d; color: white;">
                                    <th rowspan="2" style="padding: 12px; text-align: left; border: 1px solid #64748b; min-width: 280px; vertical-align: middle; position: sticky; left: 0; top: 0; z-index: 3; background: #7f1d1d;">Billing Party</th>
                            """

                            for month in pivot_cn2.columns:
                                pending_pod_html += f'<th colspan="3" style="padding: 12px; text-align: center; border: 1px solid #64748b; position: sticky; top: 0; z-index: 2; background: #7f1d1d;">{month}</th>'

                            pending_pod_html += """
                                </tr>
                            <tr style="background: #7f1d1d; color: #fecaca;">
                        """

                            for month in pivot_cn2.columns:
                                pending_pod_html += '<th style="padding: 8px; text-align: center; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #7f1d1d;">No. of CN</th>'
                                pending_pod_html += '<th style="padding: 8px; text-align: center; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #7f1d1d;">Qty</th>'
                                pending_pod_html += '<th style="padding: 8px; text-align: right; border: 1px solid #64748b; position: sticky; top: 44px; z-index: 2; background: #7f1d1d;">Pending Amt</th>'

                            pending_pod_html += "</tr></thead><tbody>"

                            # Grand totals
                            grand_cn2 = {m: 0 for m in pivot_cn2.columns}
                            grand_qty2 = {m: 0 for m in pivot_cn2.columns}
                            grand_amount2 = {m: 0 for m in pivot_cn2.columns}

                            row_idx = 0
                            for category in category_order:
                                # Get parties in this category
                                cat_parties = [p for p in pivot_cn2.index if party_category2.get(p) == category]

                                if not cat_parties:
                                    continue

                                # Category totals
                                cat_cn = {m: 0 for m in pivot_cn2.columns}
                                cat_qty = {m: 0 for m in pivot_cn2.columns}
                                cat_amount = {m: 0 for m in pivot_cn2.columns}

                                # Add party rows
                                for party in sorted(cat_parties):
                                    bg_color = '#1e293b' if row_idx % 2 == 0 else '#0f172a'
                                    pending_pod_html += f'<tr style="background: {bg_color}; color: white;">'
                                    pending_pod_html += f'<td style="padding: 8px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: {bg_color};">{party}</td>'

                                    for month in pivot_cn2.columns:
                                        cn_count = int(pivot_cn2.loc[party, month])
                                        qty_count = int(pivot_qty2.loc[party, month])
                                        amount = pivot_amount2.loc[party, month]
                                        cat_cn[month] += cn_count
                                        cat_qty[month] += qty_count
                                        cat_amount[month] += amount
                                        pending_pod_html += f'<td style="padding: 8px; text-align: center; border: 1px solid #64748b;">{cn_count if cn_count > 0 else "-"}</td>'
                                        pending_pod_html += f'<td style="padding: 8px; text-align: center; border: 1px solid #64748b;">{qty_count if qty_count > 0 else "-"}</td>'
                                        pending_pod_html += f'<td style="padding: 8px; text-align: right; border: 1px solid #64748b;">{"₹{:,.0f}".format(amount) if amount > 0 else "-"}</td>'

                                    pending_pod_html += "</tr>"
                                    row_idx += 1

                                # Add to grand totals
                                for month in pivot_cn2.columns:
                                    grand_cn2[month] += cat_cn[month]
                                    grand_qty2[month] += cat_qty[month]
                                    grand_amount2[month] += cat_amount[month]

                                # Category total row (gold) - only show if more than 1 party
                                if len(cat_parties) > 1:
                                    pending_pod_html += f'<tr style="background: #b8860b; color: white; font-weight: bold;">'
                                    pending_pod_html += f'<td style="padding: 10px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: #b8860b;">{category} - Total</td>'

                                    for month in pivot_cn2.columns:
                                        pending_pod_html += f'<td style="padding: 10px; text-align: center; border: 1px solid #64748b;">{cat_cn[month] if cat_cn[month] > 0 else "-"}</td>'
                                        pending_pod_html += f'<td style="padding: 10px; text-align: center; border: 1px solid #64748b;">{cat_qty[month] if cat_qty[month] > 0 else "-"}</td>'
                                        pending_pod_html += f'<td style="padding: 10px; text-align: right; border: 1px solid #64748b;">{"₹{:,.0f}".format(cat_amount[month]) if cat_amount[month] > 0 else "-"}</td>'

                                    pending_pod_html += "</tr>"

                            # Grand total row (dark red)
                            pending_pod_html += '<tr style="background: #7f1d1d; color: white; font-weight: bold;">'
                            pending_pod_html += '<td style="padding: 12px; border: 1px solid #64748b; position: sticky; left: 0; z-index: 1; background: #7f1d1d;">Grand Total</td>'

                            for month in pivot_cn2.columns:
                                pending_pod_html += f'<td style="padding: 12px; text-align: center; border: 1px solid #64748b;">{grand_cn2[month]}</td>'
                                pending_pod_html += f'<td style="padding: 12px; text-align: center; border: 1px solid #64748b;">{grand_qty2[month]}</td>'
                                pending_pod_html += f'<td style="padding: 12px; text-align: right; border: 1px solid #64748b;">₹{grand_amount2[month]:,.0f}</td>'

                            pending_pod_html += "</tr></tbody></table></div>"

                            components.html(pending_pod_html, height=600, scrolling=True)

                            # Download button - Raw CN data
                            conn_download2 = get_db_connection()
                            if conn_download2 is not None:
                                raw_pending_pod_query = f"""
                                    SELECT cn_no, cn_date, billing_party, origin, route,
                                           vehicle_no, qty, basic_freight, eta
                                    FROM cn_data
                                    WHERE (bill_no IS NULL OR bill_no = '')
                                      AND (pod_receipt_no IS NULL OR pod_receipt_no = '')
                                      AND eta < '{d_minus_4}'
                                      AND (cn_no IS NULL OR cn_no NOT LIKE 'TEST%')
                                    ORDER BY cn_date DESC, billing_party
                                """
                                raw_pending_pod_df = pd.read_sql_query(raw_pending_pod_query, conn_download2)
                                conn_download2.close()

                                raw_pending_pod_df.columns = ['CN No', 'CN Date', 'Billing Party', 'Origin', 'Route',
                                                              'Vehicle No', 'Qty', 'Basic Freight', 'ETA']
                                pending_pod_csv = raw_pending_pod_df.to_csv(index=False)
                                st.download_button(
                                    label="📥 Download Pending POD Data",
                                    data=pending_pod_csv,
                                    file_name=f"pending_pod_{datetime.now().strftime('%Y%m%d')}.csv",
                                    mime="text/csv",
                                    key="pending_pod_download"
                                )
                    else:
                        st.success("No pending POD CNs found!")
            except Exception as e:
                st.error(f"Error loading pending POD data: {e}")

        unbilled_cn_fragment()


if __name__ == "__main__":
    main()
